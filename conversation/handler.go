package conversation

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aither64/codex-web/codex"
)

const (
	defaultMaxBodyBytes     int64 = 512 * 1024
	defaultMaxMessageBytes        = 48 * 1024
	defaultOperationTimeout       = 30 * time.Second
)

var browserAttemptPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)
var messageDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var basePathPattern = regexp.MustCompile(`^[A-Za-z0-9/._~!$&'()*+,;=:@-]+$`)

//go:embed assets/conversation.js assets/conversation.css assets/uploads.js assets/uploads.css
var assetFiles embed.FS

// Capabilities grants an opaque conversation ID access to individual
// operations. Applications should grant only the operations their own policy
// allows for the current request.
type Capabilities struct {
	Read        bool
	Pending     bool
	QueueRead   bool
	Send        bool
	Queue       bool
	Interrupt   bool
	Settings    bool
	Respond     bool
	EventStream bool
}

// Client is the App Server behavior used by Handler. *codex.Client implements
// this interface.
type Client interface {
	VerifyThread(context.Context, string, string) error
	ReadThread(context.Context, string) (codex.Transcript, error)
	PromptsWithItems(context.Context, string) ([]codex.Prompt, error)
	ListQueue(context.Context, string) ([]codex.QueueEntry, error)
	SendAttempted(context.Context, string, string, string, string) (bool, error)
	Send(context.Context, string, string, string, string) (codex.SendReceipt, error)
	AcknowledgeSends(context.Context, string, []codex.SendAcknowledgement) ([]string, error)
	Queue(context.Context, string, string, string) (codex.QueueEntry, error)
	DeleteQueueEntry(context.Context, string, string) error
	StartQueue(context.Context, string, string) error
	Interrupt(context.Context, string) error
	ListModels(context.Context) ([]codex.Model, error)
	ListCollaborationModes(context.Context) ([]codex.CollaborationMode, error)
	UpdateThreadSettings(context.Context, string, codex.ThreadSettingsUpdate) (codex.ThreadSettings, error)
	RespondDecision(context.Context, string, string, string) error
	RespondAnswers(context.Context, string, string, map[string]map[string][]string) error
	SnoozeUserInput(string, string) error
	Subscribe(context.Context, string) (<-chan struct{}, func(), error)
}

// ActivityProvider is optional, preserving source compatibility for applications
// implementing Client. The resolver selects its trusted authority separately.
type ActivityProvider interface {
	ReadActivity(context.Context, string) (codex.ActivitySnapshot, error)
}

// ResolveRequest describes the operation for which an application must resolve
// authorization and runtime ownership.
type ResolveRequest struct {
	ID        string
	Operation string
	Method    string
	Mutation  bool
}

// Target is the trusted server-side mapping for one opaque conversation ID.
// Socket paths, thread IDs and working directories never come from browser
// input. MutationLock must be shared with every other application operation
// that mutates the same conversation. Release is called after the operation,
// including an event stream.
type Target struct {
	Attachments         AttachmentProvider
	Client              Client
	Activity            ActivityProvider
	ThreadID            string
	Directory           string
	Capabilities        Capabilities
	MutationLock        MutationLocker
	TransformTranscript func(*codex.Transcript)
	Release             func()
}

// Resolver maps an application-defined opaque ID to its trusted App Server
// client, thread and working directory for every request.
type Resolver interface {
	ResolveConversation(context.Context, ResolveRequest) (Target, error)
}

type ResolverFunc func(context.Context, ResolveRequest) (Target, error)

func (fn ResolverFunc) ResolveConversation(ctx context.Context, request ResolveRequest) (Target, error) {
	return fn(ctx, request)
}

type Options struct {
	Resolver         Resolver
	AllowedOrigins   []string
	BasePath         string
	MaxBodyBytes     int64
	MaxMessageBytes  int
	OperationTimeout time.Duration
	Shutdown         <-chan struct{}
	Logger           *log.Logger
}

type Handler struct {
	resolver         Resolver
	allowedOrigins   []string
	basePath         string
	maxBodyBytes     int64
	maxMessageBytes  int
	operationTimeout time.Duration
	shutdown         <-chan struct{}
	logger           *log.Logger
	assets           http.Handler
}

func NewHandler(options Options) (*Handler, error) {
	if options.Resolver == nil {
		return nil, errors.New("conversation resolver is required")
	}
	basePath := options.BasePath
	if basePath == "" {
		basePath = "/codex"
	} else if basePath != "/" {
		basePath = strings.TrimSuffix(basePath, "/")
	}
	if !strings.HasPrefix(basePath, "/") || strings.Contains(basePath, "//") ||
		strings.ContainsAny(basePath, "\\%?#") || path.Clean(basePath) != basePath ||
		!basePathPattern.MatchString(basePath) {
		return nil, errors.New("conversation base path must be an absolute URL path")
	}
	if basePath == "/" {
		basePath = ""
	}
	origins := make([]string, 0, len(options.AllowedOrigins))
	for _, raw := range options.AllowedOrigins {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Path != "" ||
			parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return nil, fmt.Errorf("invalid allowed origin %q", raw)
		}
		origins = append(origins, parsed.Scheme+"://"+parsed.Host)
	}
	if len(origins) == 0 {
		return nil, errors.New("at least one exact allowed origin is required")
	}
	maxBodyBytes := options.MaxBodyBytes
	if maxBodyBytes == 0 {
		maxBodyBytes = defaultMaxBodyBytes
	}
	if maxBodyBytes < 1024 {
		return nil, errors.New("conversation request limit must be at least 1024 bytes")
	}
	maxMessageBytes := options.MaxMessageBytes
	if maxMessageBytes == 0 {
		maxMessageBytes = defaultMaxMessageBytes
	}
	if maxMessageBytes < 1 || int64(maxMessageBytes) > maxBodyBytes {
		return nil, errors.New("conversation message limit must fit within the request limit")
	}
	operationTimeout := options.OperationTimeout
	if operationTimeout == 0 {
		operationTimeout = defaultOperationTimeout
	}
	if operationTimeout < 0 {
		return nil, errors.New("conversation operation timeout cannot be negative")
	}
	assets, err := fs.Sub(assetFiles, "assets")
	if err != nil {
		return nil, err
	}
	logger := options.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &Handler{
		resolver: options.Resolver, allowedOrigins: origins, basePath: basePath,
		maxBodyBytes: maxBodyBytes, maxMessageBytes: maxMessageBytes,
		operationTimeout: operationTimeout, shutdown: options.Shutdown, logger: logger,
		assets: http.FileServer(http.FS(assets)),
	}, nil
}

func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	escapedPath := request.URL.EscapedPath()
	if request.URL.RawPath != "" && request.URL.RawPath != escapedPath {
		http.NotFound(response, request)
		return
	}
	path := escapedPath
	if handler.basePath != "" {
		path = strings.TrimPrefix(escapedPath, handler.basePath)
		if path == escapedPath || !strings.HasPrefix(path, "/") {
			http.NotFound(response, request)
			return
		}
	}
	if strings.HasPrefix(path, "/assets/") {
		assetPath, err := url.PathUnescape(strings.TrimPrefix(path, "/assets"))
		if err != nil || strings.Contains(assetPath, "//") {
			http.NotFound(response, request)
			return
		}
		request.URL.Path = assetPath
		request.URL.RawPath = ""
		response.Header().Set("Cache-Control", "public, max-age=300")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		handler.assets.ServeHTTP(response, request)
		return
	}
	if !strings.HasPrefix(path, "/conversations/") {
		http.NotFound(response, request)
		return
	}
	remainder := strings.TrimPrefix(path, "/conversations/")
	parts := strings.Split(remainder, "/")
	if len(parts) < 2 || len(parts) > 3 || slices.Contains(parts, "") {
		http.NotFound(response, request)
		return
	}
	id, err := url.PathUnescape(parts[0])
	if err != nil || !validOpaqueID(id) || encodeOpaquePathSegment(id) != parts[0] {
		http.NotFound(response, request)
		return
	}
	operation := strings.Join(parts[1:], "/")
	if !handler.originAllowed(request) {
		writeError(response, http.StatusForbidden, "request origin is not allowed")
		return
	}
	if operation != "events" || request.Method != http.MethodGet {
		operationContext, cancel := context.WithTimeout(request.Context(), handler.operationTimeout)
		defer cancel()
		request = request.WithContext(operationContext)
	}
	mutation := request.Method != http.MethodGet && request.Method != http.MethodHead
	resolved, err := handler.resolver.ResolveConversation(request.Context(), ResolveRequest{
		ID: id, Operation: operation, Method: request.Method, Mutation: mutation,
	})
	if err != nil {
		handler.logError(request, fmt.Errorf("resolve conversation: %w", err))
		writeError(response, http.StatusNotFound, "conversation is unavailable")
		return
	}
	if resolved.Release != nil {
		defer resolved.Release()
	}
	if resolved.Client == nil || resolved.ThreadID == "" || !filepath.IsAbs(resolved.Directory) ||
		filepath.Clean(resolved.Directory) != resolved.Directory ||
		(mutation && resolved.MutationLock == nil) {
		handler.logError(request, errors.New("conversation resolver returned an invalid target"))
		writeError(response, http.StatusServiceUnavailable, "Codex service is unavailable")
		return
	}
	verifyContext, cancel := context.WithTimeout(request.Context(), 5*time.Second)
	err = resolved.Client.VerifyThread(verifyContext, resolved.ThreadID, resolved.Directory)
	cancel()
	if err != nil {
		handler.logError(request, fmt.Errorf("verify resolved conversation: %w", err))
		writeError(response, http.StatusNotFound, "conversation is unavailable")
		return
	}
	if mutation {
		if err := resolved.MutationLock.Lock(request.Context()); err != nil {
			handler.serverError(response, request, err)
			return
		}
		defer resolved.MutationLock.Unlock()
	}
	handler.serveOperation(response, request, resolved, operation)
}

func encodeOpaquePathSegment(value string) string {
	const hexadecimal = "0123456789ABCDEF"
	var encoded strings.Builder
	encoded.Grow(len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("-_.!~*'()", rune(character)) {
			encoded.WriteByte(character)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte(hexadecimal[character>>4])
		encoded.WriteByte(hexadecimal[character&0x0f])
	}
	return encoded.String()
}

func validOpaqueID(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) &&
		value != "." && value != ".." && !strings.ContainsAny(value, "/\x00")
}

func (handler *Handler) originAllowed(request *http.Request) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return request.Method == http.MethodGet || request.Method == http.MethodHead
	}
	return slices.Contains(handler.allowedOrigins, origin)
}

func (handler *Handler) serveOperation(
	response http.ResponseWriter, request *http.Request, target Target, operation string,
) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	ctx := request.Context()
	switch operation {
	case "activity":
		if request.Method != http.MethodGet || !target.Capabilities.Read {
			handler.rejectOperation(response, request)
			return
		}
		if target.Activity == nil {
			writeError(response, http.StatusNotFound, "activity is unavailable")
			return
		}
		activity, err := target.Activity.ReadActivity(ctx, target.ThreadID)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if activity.ThreadID != target.ThreadID {
			handler.serverError(response, request, errors.New("activity provider returned a different thread"))
			return
		}
		writeJSON(response, http.StatusOK, activity)
	case "thread":
		if request.Method != http.MethodGet || !target.Capabilities.Read {
			handler.rejectOperation(response, request)
			return
		}
		transcript, err := target.Client.ReadThread(ctx, target.ThreadID)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if target.TransformTranscript != nil {
			target.TransformTranscript(&transcript)
		}
		if target.Attachments != nil {
			if err := target.Attachments.ObserveTranscript(ctx, &transcript); err != nil {
				uploadError(response, err)
				return
			}
		}
		writeJSON(response, http.StatusOK, transcript)
	case "pending":
		if request.Method != http.MethodGet || !target.Capabilities.Pending {
			handler.rejectOperation(response, request)
			return
		}
		prompts, err := target.Client.PromptsWithItems(ctx, target.ThreadID)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		writeJSON(response, http.StatusOK, nonNilPrompts(prompts))
	case "models":
		if request.Method != http.MethodGet || !target.Capabilities.Settings {
			handler.rejectOperation(response, request)
			return
		}
		models, err := target.Client.ListModels(ctx)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if models == nil {
			models = []codex.Model{}
		}
		writeJSON(response, http.StatusOK, models)
	case "collaboration-modes":
		if request.Method != http.MethodGet || !target.Capabilities.Settings {
			handler.rejectOperation(response, request)
			return
		}
		modes, err := target.Client.ListCollaborationModes(ctx)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if modes == nil {
			modes = []codex.CollaborationMode{}
		}
		writeJSON(response, http.StatusOK, modes)
	case "queue":
		handler.queue(response, request, target)
	case "queue/start":
		if request.Method == http.MethodDelete {
			handler.deleteQueue(response, request, target, "start")
		} else {
			handler.startQueue(response, request, target)
		}
	case "message":
		handler.send(response, request, target)
	case "message-ack":
		handler.acknowledge(response, request, target)
	case "interrupt":
		if request.Method != http.MethodPost || !target.Capabilities.Interrupt {
			handler.rejectOperation(response, request)
			return
		}
		if !handler.decodeEmpty(response, request) {
			return
		}
		if err := target.Client.Interrupt(ctx, target.ThreadID); err != nil {
			handler.serverError(response, request, err)
			return
		}
		writeJSON(response, http.StatusAccepted, map[string]bool{"ok": true})
	case "settings":
		handler.settings(response, request, target)
	case "respond":
		handler.respond(response, request, target)
	case "events":
		handler.events(response, request, target)
	default:
		if strings.HasPrefix(operation, "queue/") {
			handler.deleteQueue(response, request, target, strings.TrimPrefix(operation, "queue/"))
			return
		}
		http.NotFound(response, request)
	}
}

func (handler *Handler) send(response http.ResponseWriter, request *http.Request, target Target) {
	if request.Method != http.MethodPost || !target.Capabilities.Send {
		handler.rejectOperation(response, request)
		return
	}
	var body struct {
		Message             string   `json:"message"`
		ClientUserMessageID string   `json:"clientUserMessageId"`
		Retry               bool     `json:"retry"`
		AttachmentIDs       []string `json:"attachmentIds,omitempty"`
	}
	if !handler.decode(response, request, &body) {
		return
	}
	message := strings.TrimSpace(body.Message)
	body.ClientUserMessageID = strings.TrimSpace(body.ClientUserMessageID)
	if (message == "" && len(body.AttachmentIDs) == 0) || len([]byte(message)) > handler.maxMessageBytes ||
		!browserAttemptPattern.MatchString(body.ClientUserMessageID) {
		writeError(response, http.StatusBadRequest, "message and browser attempt ID are invalid")
		return
	}
	var prepareErr error
	message, prepareErr = handler.prepareAttachments(request.Context(), target, "send", body.ClientUserMessageID, message, body.AttachmentIDs)
	if prepareErr != nil {
		uploadError(response, prepareErr)
		return
	}
	if body.Retry {
		if _, err := target.Client.SendAttempted(
			request.Context(), target.ThreadID, message, body.ClientUserMessageID, "",
		); err != nil {
			handler.serverError(response, request, err)
			return
		}
	}
	receipt, err := target.Client.Send(
		request.Context(), target.ThreadID, message, body.ClientUserMessageID, "",
	)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusAccepted, receipt)
}

func (handler *Handler) acknowledge(response http.ResponseWriter, request *http.Request, target Target) {
	if request.Method != http.MethodPost || !target.Capabilities.Send {
		handler.rejectOperation(response, request)
		return
	}
	var body struct {
		Acknowledgements []codex.SendAcknowledgement `json:"acknowledgements"`
	}
	if !handler.decode(response, request, &body) {
		return
	}
	if len(body.Acknowledgements) > 100 {
		writeError(response, http.StatusBadRequest, "too many message acknowledgements")
		return
	}
	seen := make(map[string]struct{}, len(body.Acknowledgements))
	for _, item := range body.Acknowledgements {
		if !browserAttemptPattern.MatchString(item.ClientUserMessageID) ||
			!messageDigestPattern.MatchString(item.Digest) {
			writeError(response, http.StatusBadRequest, "message acknowledgement is invalid")
			return
		}
		if _, duplicate := seen[item.ClientUserMessageID]; duplicate {
			writeError(response, http.StatusBadRequest, "message acknowledgement is repeated")
			return
		}
		seen[item.ClientUserMessageID] = struct{}{}
	}
	acknowledged, err := target.Client.AcknowledgeSends(
		request.Context(), target.ThreadID, body.Acknowledgements,
	)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string][]string{
		"acknowledgedClientUserMessageIds": acknowledged,
	})
}

func (handler *Handler) queue(response http.ResponseWriter, request *http.Request, target Target) {
	if request.Method == http.MethodGet {
		if !target.Capabilities.QueueRead {
			handler.rejectOperation(response, request)
			return
		}
		entries, err := target.Client.ListQueue(request.Context(), target.ThreadID)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if target.Attachments != nil {
			if err := target.Attachments.ObserveQueue(request.Context(), entries); err != nil {
				uploadError(response, err)
				return
			}
		}
		writeJSON(response, http.StatusOK, nonNilQueue(entries))
		return
	}
	if request.Method != http.MethodPost || !target.Capabilities.Queue {
		handler.rejectOperation(response, request)
		return
	}
	var body struct {
		Message             string   `json:"message"`
		ClientUserMessageID string   `json:"clientUserMessageId"`
		AttachmentIDs       []string `json:"attachmentIds,omitempty"`
	}
	if !handler.decode(response, request, &body) {
		return
	}
	message := strings.TrimSpace(body.Message)
	body.ClientUserMessageID = strings.TrimSpace(body.ClientUserMessageID)
	if (message == "" && len(body.AttachmentIDs) == 0) || len([]byte(message)) > handler.maxMessageBytes ||
		!browserAttemptPattern.MatchString(body.ClientUserMessageID) {
		writeError(response, http.StatusBadRequest, "message and browser attempt ID are invalid")
		return
	}
	var prepareErr error
	message, prepareErr = handler.prepareAttachments(request.Context(), target, "queue", body.ClientUserMessageID, message, body.AttachmentIDs)
	if prepareErr != nil {
		uploadError(response, prepareErr)
		return
	}
	entry, err := target.Client.Queue(
		request.Context(), target.ThreadID, message, body.ClientUserMessageID,
	)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	if target.Attachments != nil {
		entries := []codex.QueueEntry{entry}
		if err := target.Attachments.ObserveQueue(request.Context(), entries); err != nil {
			uploadError(response, err)
			return
		}
		entry = entries[0]
	}
	writeJSON(response, http.StatusAccepted, entry)
}

func (handler *Handler) deleteQueue(
	response http.ResponseWriter, request *http.Request, target Target, id string,
) {
	if request.Method != http.MethodDelete || !target.Capabilities.Queue || id == "" {
		handler.rejectOperation(response, request)
		return
	}
	decoded, err := url.PathUnescape(id)
	if err != nil || !validOpaqueID(decoded) || encodeOpaquePathSegment(decoded) != id {
		writeError(response, http.StatusBadRequest, "queued message ID is invalid")
		return
	}
	if err := target.Client.DeleteQueueEntry(request.Context(), target.ThreadID, decoded); err != nil {
		handler.serverError(response, request, err)
		return
	}
	if target.Attachments != nil {
		if err := target.Attachments.QueueDeleted(request.Context(), decoded); err != nil {
			uploadError(response, err)
			return
		}
	}
	writeJSON(response, http.StatusOK, map[string]bool{"ok": true})
}

func (handler *Handler) startQueue(response http.ResponseWriter, request *http.Request, target Target) {
	if request.Method != http.MethodPost || !target.Capabilities.Queue {
		handler.rejectOperation(response, request)
		return
	}
	var body struct {
		QueuedSubmissionID string `json:"queuedSubmissionId"`
	}
	if !handler.decode(response, request, &body) {
		return
	}
	if !validOpaqueID(body.QueuedSubmissionID) {
		writeError(response, http.StatusBadRequest, "queued message ID is invalid")
		return
	}
	if err := target.Client.StartQueue(
		request.Context(), target.ThreadID, body.QueuedSubmissionID,
	); err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusAccepted, map[string]bool{"ok": true})
}

func (handler *Handler) settings(response http.ResponseWriter, request *http.Request, target Target) {
	if (request.Method != http.MethodPost && request.Method != http.MethodPatch) ||
		!target.Capabilities.Settings {
		handler.rejectOperation(response, request)
		return
	}
	var update codex.ThreadSettingsUpdate
	if !handler.decode(response, request, &update) {
		return
	}
	for _, value := range []**string{&update.Model, &update.ReasoningEffort, &update.CollaborationMode} {
		if *value != nil {
			trimmed := strings.TrimSpace(**value)
			*value = &trimmed
		}
	}
	if update.Model == nil && update.ReasoningEffort == nil && update.CollaborationMode == nil {
		writeError(response, http.StatusBadRequest, "no Codex setting was selected")
		return
	}
	if update.Model != nil {
		if *update.Model == "" || update.ReasoningEffort == nil || *update.ReasoningEffort == "" {
			writeError(response, http.StatusBadRequest, "select a Codex model and an explicit reasoning setting")
			return
		}
		models, err := target.Client.ListModels(request.Context())
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		valid := false
		for _, model := range models {
			if model.Model != *update.Model {
				continue
			}
			for _, effort := range model.SupportedReasoningEfforts {
				valid = valid || effort.ReasoningEffort == *update.ReasoningEffort
			}
		}
		if !valid {
			writeError(response, http.StatusBadRequest, "Codex model or reasoning setting is unavailable")
			return
		}
	} else if update.ReasoningEffort != nil {
		writeError(response, http.StatusBadRequest, "select a Codex model before changing its reasoning setting")
		return
	}
	if update.CollaborationMode != nil {
		modes, err := target.Client.ListCollaborationModes(request.Context())
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		valid := false
		for _, mode := range modes {
			valid = valid || mode.Mode == *update.CollaborationMode
		}
		if !valid {
			writeError(response, http.StatusBadRequest, "Codex collaboration mode is unavailable")
			return
		}
	}
	settings, err := target.Client.UpdateThreadSettings(request.Context(), target.ThreadID, update)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, settings)
}

func (handler *Handler) respond(response http.ResponseWriter, request *http.Request, target Target) {
	if request.Method != http.MethodPost || !target.Capabilities.Respond {
		handler.rejectOperation(response, request)
		return
	}
	var body struct {
		ID       string                         `json:"id"`
		Decision string                         `json:"decision"`
		Answers  map[string]map[string][]string `json:"answers"`
		Snooze   bool                           `json:"snooze"`
	}
	if !handler.decode(response, request, &body) {
		return
	}
	body.ID = strings.TrimSpace(body.ID)
	body.Decision = strings.TrimSpace(body.Decision)
	if body.ID == "" || len(body.ID) > 256 {
		writeError(response, http.StatusBadRequest, "prompt ID is invalid")
		return
	}
	actions := 0
	if body.Snooze {
		actions++
	}
	if len(body.Answers) > 0 {
		actions++
	}
	if body.Decision != "" {
		actions++
	}
	if actions != 1 {
		writeError(response, http.StatusBadRequest, "prompt response must select one action")
		return
	}
	var err error
	if body.Snooze {
		err = target.Client.SnoozeUserInput(body.ID, target.ThreadID)
	} else if len(body.Answers) > 0 {
		err = target.Client.RespondAnswers(request.Context(), body.ID, target.ThreadID, body.Answers)
	} else {
		err = target.Client.RespondDecision(request.Context(), body.ID, target.ThreadID, body.Decision)
	}
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]bool{"ok": true})
}

func (handler *Handler) events(response http.ResponseWriter, request *http.Request, target Target) {
	if request.Method != http.MethodGet || !target.Capabilities.EventStream {
		handler.rejectOperation(response, request)
		return
	}
	flusher, ok := response.(http.Flusher)
	if !ok {
		writeError(response, http.StatusInternalServerError, "event streaming is unavailable")
		return
	}
	events, unsubscribe, err := target.Client.Subscribe(request.Context(), target.ThreadID)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	defer unsubscribe()
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("X-Accel-Buffering", "no")
	_, _ = io.WriteString(response, ": connected\n\n")
	flusher.Flush()
	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-handler.shutdown:
			return
		case <-keepalive.C:
			_, _ = io.WriteString(response, ": keepalive\n\n")
			flusher.Flush()
		case _, open := <-events:
			if !open {
				return
			}
			_, _ = io.WriteString(response, "data: update\n\n")
			flusher.Flush()
		}
	}
}

func (handler *Handler) rejectOperation(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead &&
		request.Method != http.MethodPost && request.Method != http.MethodPatch &&
		request.Method != http.MethodDelete {
		response.Header().Set("Allow", "GET, HEAD, POST, PATCH, DELETE")
		writeError(response, http.StatusMethodNotAllowed, "method is not allowed")
		return
	}
	writeError(response, http.StatusForbidden, "conversation operation is not allowed")
}

func (handler *Handler) decode(response http.ResponseWriter, request *http.Request, value any) bool {
	request.Body = http.MaxBytesReader(response, request.Body, handler.maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		writeError(response, http.StatusBadRequest, "request body is invalid")
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(response, http.StatusBadRequest, "request body has trailing data")
		return false
	}
	return true
}

func (handler *Handler) decodeEmpty(response http.ResponseWriter, request *http.Request) bool {
	request.Body = http.MaxBytesReader(response, request.Body, 1024)
	decoder := json.NewDecoder(request.Body)
	var body map[string]json.RawMessage
	err := decoder.Decode(&body)
	if errors.Is(err, io.EOF) {
		return true
	}
	if err != nil || len(body) != 0 {
		writeError(response, http.StatusBadRequest, "request body must be empty")
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(response, http.StatusBadRequest, "request body has trailing data")
		return false
	}
	return true
}

func (handler *Handler) serverError(response http.ResponseWriter, request *http.Request, err error) {
	handler.logError(request, err)
	writeError(response, http.StatusServiceUnavailable, "Codex service is unavailable")
}

func (handler *Handler) logError(request *http.Request, err error) {
	handler.logger.Printf("conversation %s %q: %v", request.Method, request.URL.Path, err)
}

func nonNilPrompts(prompts []codex.Prompt) []codex.Prompt {
	if prompts == nil {
		return []codex.Prompt{}
	}
	return prompts
}

func nonNilQueue(queue []codex.QueueEntry) []codex.QueueEntry {
	if queue == nil {
		return []codex.QueueEntry{}
	}
	return queue
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}
