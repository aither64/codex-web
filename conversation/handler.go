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

	"github.com/aither64/codex-web/conversation/backend"
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
var pageCursorPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var basePathPattern = regexp.MustCompile(`^[A-Za-z0-9/._~!$&'()*+,;=:@-]+$`)

//go:embed assets/conversation.js assets/conversation.css assets/uploads.js assets/uploads.css assets/sync.js assets/refresh.js assets/questions.js assets/citations.js assets/retry.js assets/retry_activity.js assets/delivery_retry.js
var assetFiles embed.FS

// Capabilities grants an opaque conversation ID access to individual
// operations. Applications should grant only the operations their own policy
// allows for the current request.
type Capabilities struct {
	Read          bool
	Pending       bool
	QueueRead     bool
	Send          bool
	Queue         bool
	Interrupt     bool
	Settings      bool
	Respond       bool
	EventStream   bool
	Retry         bool
	RetryActivity bool
}

type Handler struct {
	resolver           BackendResolver
	allowedOrigins     []string
	basePath           string
	maxBodyBytes       int64
	maxMessageBytes    int
	operationTimeout   time.Duration
	shutdown           <-chan struct{}
	logger             *log.Logger
	assets             http.Handler
	pageCursorMinBytes int
	pageCursorMaxBytes int
	pageQueryMaxBytes  int
}

func NewBackendHandler(options BackendOptions) (*Handler, error) {
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
		assets:             http.FileServer(http.FS(assets)),
		pageCursorMinBytes: 1, pageCursorMaxBytes: 5462, pageQueryMaxBytes: 16393,
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
	resolved, err := handler.resolver.ResolveBackend(request.Context(), ResolveRequest{
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
	if resolved.Operations.Verify == nil || resolved.Identity.BackendID == "" || resolved.Identity.ScopeKey == "" || resolved.Identity.ConversationID == "" || !filepath.IsAbs(resolved.Identity.Directory) ||
		filepath.Clean(resolved.Identity.Directory) != resolved.Identity.Directory ||
		(mutation && resolved.MutationLock == nil) {
		handler.logError(request, errors.New("conversation resolver returned an invalid target"))
		writeError(response, http.StatusServiceUnavailable, "Codex service is unavailable")
		return
	}
	verifyContext, cancel := context.WithTimeout(request.Context(), 5*time.Second)
	err = resolved.Operations.Verify.Verify(verifyContext, resolved.Identity)
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
	response http.ResponseWriter, request *http.Request, target BackendTarget, operation string,
) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	ctx := request.Context()
	switch operation {
	case "retry/activity", "retry/now", "retry/stop":
		handler.retryActivity(response, request, target, operation)
	case "retry", "retry/enabled", "retry/cancel":
		handler.retry(response, request, target, operation)
	case "thread/page":
		if request.Method != http.MethodGet || !target.Capabilities.Read {
			handler.rejectOperation(response, request)
			return
		}
		if len(request.URL.RawQuery) > handler.pageQueryMaxBytes {
			writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid page cursor", "code": "transcript_cursor_invalid"})
			return
		}
		query, err := url.ParseQuery(request.URL.RawQuery)
		if err != nil || len(query) > 1 || len(query["cursor"]) > 1 ||
			(len(query) == 1 && len(query["cursor"]) != 1) ||
			(len(query["cursor"]) == 1 && (len(query["cursor"][0]) < handler.pageCursorMinBytes ||
				len(query["cursor"][0]) > handler.pageCursorMaxBytes || !pageCursorPattern.MatchString(query["cursor"][0]))) {
			writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid page cursor", "code": "transcript_cursor_invalid"})
			return
		}
		reader := target.Operations.Page
		if reader == nil {
			writeJSON(response, http.StatusNotImplemented, map[string]string{"error": "conversation paging is unavailable", "code": "transcript_paging_unavailable"})
			return
		}
		cursor := ""
		if len(query["cursor"]) == 1 {
			cursor = query["cursor"][0]
		}
		page, err := reader.ReadPage(ctx, target.Identity, backend.HistoryCursor(cursor))
		if err != nil {
			var cursorErr *backend.CursorError
			if errors.As(err, &cursorErr) {
				status := http.StatusConflict
				message := "conversation history changed; reload recent messages"
				switch cursorErr.Code {
				case "transcript_cursor_invalid":
					status = http.StatusBadRequest
					message = "invalid page cursor"
				case "transcript_cursor_expired":
					message = "conversation history page expired; reload recent messages"
				case "transcript_reset_required":
				default:
					handler.serverError(response, request, errors.New("page reader returned invalid cursor error"))
					return
				}
				writeJSON(response, status, map[string]string{"error": message, "code": cursorErr.Code})
				return
			}
			var upload *UploadError
			if errors.As(err, &upload) {
				uploadError(response, err)
			} else {
				handler.serverError(response, request, errors.New("read transcript page failed"))
			}
			return
		}
		if page.ThreadID != target.Identity.ConversationID || len(page.Entries) > 100 {
			handler.serverError(response, request, errors.New("page reader returned invalid transcript"))
			return
		}
		if target.TransformTranscript != nil {
			target.TransformTranscript(&page.Transcript)
		}
		if target.PresentTranscript != nil {
			target.PresentTranscript(&page.Transcript)
		}
		writeJSON(response, http.StatusOK, page)
	case "activity":
		if request.Method != http.MethodGet || !target.Capabilities.Read {
			handler.rejectOperation(response, request)
			return
		}
		if target.Operations.Activity == nil {
			writeError(response, http.StatusNotFound, "activity is unavailable")
			return
		}
		activity, err := target.Operations.Activity.ReadActivity(ctx, target.Identity)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if activity.ThreadID != target.Identity.ConversationID {
			handler.serverError(response, request, errors.New("activity provider returned a different thread"))
			return
		}
		writeJSON(response, http.StatusOK, activity)
	case "thread":
		if request.Method != http.MethodGet || !target.Capabilities.Read {
			handler.rejectOperation(response, request)
			return
		}
		if !handler.requireSupport(response, request, target.Operations.Read != nil) {
			return
		}

		transcript, err := target.Operations.Read.Read(ctx, target.Identity)
		if err != nil {
			handler.readError(response, request, err)
			return
		}
		if target.TransformTranscript != nil {
			target.TransformTranscript(&transcript)
		}
		if target.PresentTranscript != nil {
			target.PresentTranscript(&transcript)
		}
		writeJSON(response, http.StatusOK, transcript)
	case "pending":
		if request.Method != http.MethodGet || !target.Capabilities.Pending {
			handler.rejectOperation(response, request)
			return
		}
		if !handler.requireSupport(response, request, target.Operations.Pending != nil) {
			return
		}

		prompts, err := target.Operations.Pending.Pending(ctx, target.Identity)
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
		if !handler.requireSupport(response, request, target.Operations.Settings != nil) {
			return
		}

		models, err := target.models(ctx)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if models == nil {
			models = []backend.Model{}
		}
		writeJSON(response, http.StatusOK, models)
	case "collaboration-modes":
		if request.Method != http.MethodGet || !target.Capabilities.Settings {
			handler.rejectOperation(response, request)
			return
		}
		if !handler.requireSupport(response, request, target.Operations.Settings != nil) {
			return
		}

		modes, err := target.Operations.Settings.Modes(ctx, target.Identity)
		if err != nil {
			handler.serverError(response, request, err)
			return
		}
		if modes == nil {
			modes = []backend.CollaborationMode{}
		}
		writeJSON(response, http.StatusOK, modes)
	case "queue":
		handler.queue(response, request, target)
	case "queue/reconcile":
		if request.Method == http.MethodDelete {
			handler.deleteQueue(response, request, target, "reconcile")
		} else {
			handler.reconcileQueue(response, request, target)
		}
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
		if !handler.requireSupport(response, request, target.Operations.Interrupt != nil) {
			return
		}

		if !handler.decodeEmpty(response, request) {
			return
		}
		if err := target.Operations.Interrupt.Interrupt(ctx, target.Identity); err != nil {
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

func (handler *Handler) send(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method != http.MethodPost || !target.Capabilities.Send {
		handler.rejectOperation(response, request)
		return
	}
	if !handler.requireSupport(response, request, target.Operations.Send != nil) {
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
	if err := request.Context().Err(); err != nil {
		handler.serverError(response, request, err)
		return
	}
	// Validation and mutation locking used the browser context. An admitted
	// send now owns the remaining operation budget, including receipt recording.
	deadline, ok := request.Context().Deadline()
	if !ok {
		deadline = time.Now().Add(handler.operationTimeout)
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(request.Context()), deadline)
	defer cancel()
	if handler.shutdown != nil {
		go func() {
			select {
			case <-handler.shutdown:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	result, err := target.Operations.Send.Send(ctx, target.Identity, backend.SendRequest{
		Message: message, ClientUserMessageID: body.ClientUserMessageID, Retry: body.Retry,
	})
	if result.Outcome == backend.Accepted && err == nil {
		writeJSON(response, http.StatusAccepted, result.Receipt)
		return
	}
	handler.sendError(response, request, result, err)
}

func (handler *Handler) sendError(response http.ResponseWriter, request *http.Request, result backend.SendResult, err error) {
	if err != nil {
		handler.logError(request, err)
	}
	switch result.Outcome {
	case backend.NotSubmitted:
		writeJSON(response, http.StatusServiceUnavailable, map[string]any{
			"error": "The message was not sent.", "code": "message_not_sent", "notSent": true, "retryable": result.Retryable,
		})
	case backend.Unknown:
		writeJSON(response, http.StatusConflict, map[string]any{
			"error": "Message delivery could not be confirmed.", "code": "message_delivery_unknown", "notSent": false,
		})
	default:
		// Empty/unknown outcomes never prove either success or non-submission.
		handler.serverError(response, request, errors.New("message delivery failed without an evidenced outcome"))
	}
}

func (handler *Handler) acknowledge(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method != http.MethodPost || !target.Capabilities.Send {
		handler.rejectOperation(response, request)
		return
	}
	if !handler.requireSupport(response, request, target.Operations.Acknowledge != nil) {
		return
	}

	var body struct {
		Acknowledgements []backend.SendAcknowledgement `json:"acknowledgements"`
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
	acknowledged, err := target.Operations.Acknowledge.Acknowledge(
		request.Context(), target.Identity, body.Acknowledgements,
	)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string][]string{
		"acknowledgedClientUserMessageIds": acknowledged,
	})
}

func (handler *Handler) queue(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method == http.MethodGet {
		if !target.Capabilities.QueueRead {
			handler.rejectOperation(response, request)
			return
		}
		if !handler.requireSupport(response, request, target.Operations.QueueRead != nil) {
			return
		}

		entries, err := target.Operations.QueueRead.ListQueue(request.Context(), target.Identity)
		if err != nil {
			handler.readError(response, request, err)
			return
		}
		writeJSON(response, http.StatusOK, nonNilQueue(entries))
		return
	}
	if request.Method != http.MethodPost || !target.Capabilities.Queue {
		handler.rejectOperation(response, request)
		return
	}
	if !handler.requireSupport(response, request, target.Operations.QueueWrite != nil) {
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
	entry, err := target.Operations.QueueWrite.Queue(request.Context(), target.Identity, backend.QueueRequest{
		Message: message, ClientUserMessageID: body.ClientUserMessageID,
	})
	if err != nil {
		handler.queueError(response, request, err)
		return
	}
	writeJSON(response, http.StatusAccepted, entry)
}

func (handler *Handler) reconcileQueue(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method != http.MethodPost || !target.Capabilities.Queue {
		handler.rejectOperation(response, request)
		return
	}
	if client := target.Operations.QueueCompletion; client != nil {
		complete := func(id string) error {
			if target.Attachments != nil {
				return target.Attachments.QueueDeleted(request.Context(), id)
			}
			return nil
		}
		if err := client.ReconcileDeletions(request.Context(), target.Identity, complete); err != nil {
			handler.serverError(response, request, err)
			return
		}
	} else if target.Attachments != nil {
		writeError(response, http.StatusServiceUnavailable, "Attachment deletion recovery is unavailable")
		return
	} else if !target.legacyQueueReconcile {
		handler.requireSupport(response, request, false)
		return
	}
	writeJSON(response, http.StatusOK, map[string]bool{"ok": true})
}

func (handler *Handler) deleteQueue(
	response http.ResponseWriter, request *http.Request, target BackendTarget, id string,
) {
	if request.Method != http.MethodDelete || !target.Capabilities.Queue || id == "" {
		handler.rejectOperation(response, request)
		return
	}
	if target.Attachments == nil && !handler.requireSupport(response, request, target.Operations.QueueWrite != nil) {
		return
	}

	decoded, err := url.PathUnescape(id)
	if err != nil || !validOpaqueID(decoded) || encodeOpaquePathSegment(decoded) != id {
		writeError(response, http.StatusBadRequest, "queued message ID is invalid")
		return
	}
	if target.Attachments != nil {
		client := target.Operations.QueueCompletion
		if client == nil {
			writeError(response, http.StatusServiceUnavailable, "Attachment deletion recovery is unavailable")
			return
		}
		err = client.DeleteWithCompletion(request.Context(), target.Identity, decoded, func() error {
			return target.Attachments.QueueDeleted(request.Context(), decoded)
		})
	} else {
		err = target.Operations.QueueWrite.Delete(request.Context(), target.Identity, decoded)
	}
	if err != nil {
		handler.serverError(response, request, err)
		return
	}

	writeJSON(response, http.StatusOK, map[string]bool{"ok": true})
}

func (handler *Handler) startQueue(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method != http.MethodPost || !target.Capabilities.Queue {
		handler.rejectOperation(response, request)
		return
	}
	if !handler.requireSupport(response, request, target.Operations.QueueWrite != nil) {
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
	if err := target.Operations.QueueWrite.Start(request.Context(), target.Identity, body.QueuedSubmissionID); err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusAccepted, map[string]bool{"ok": true})
}

func (handler *Handler) settings(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if (request.Method != http.MethodPost && request.Method != http.MethodPatch) ||
		!target.Capabilities.Settings {
		handler.rejectOperation(response, request)
		return
	}
	if !handler.requireSupport(response, request, target.Operations.Settings != nil) {
		return
	}

	var update backend.SettingsUpdate
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
		models, err := target.models(request.Context())
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
		modes, err := target.Operations.Settings.Modes(request.Context(), target.Identity)
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
	settings, err := target.Operations.Settings.Update(request.Context(), target.Identity, update)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, settings)
}

func (handler *Handler) respond(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method != http.MethodPost || !target.Capabilities.Respond {
		handler.rejectOperation(response, request)
		return
	}
	var body backend.PromptResponse
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
	if target.Operations.Respond == nil && body.Token == "" && !handler.requireSupport(response, request, target.Operations.LegacyRespond != nil) {
		return
	}

	var err error
	responder := target.Operations.Respond
	bound := responder != nil
	if bound && body.Token == "" {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "Reload this page to update the question controls.", "code": "reload_required", "notSent": true})
		return
	}
	if body.Token != "" {
		ok := bound
		if !ok {
			writeJSON(response, http.StatusConflict, map[string]any{
				"error": "Reload this page to update the question controls.", "code": "reload_required", "notSent": true,
			})
			return
		}
		err = responder.Respond(request.Context(), target.Identity, body)
	} else if body.Snooze {
		err = target.Operations.LegacyRespond.Snooze(request.Context(), target.Identity, body.ID)
	} else if len(body.Answers) > 0 {
		err = target.Operations.LegacyRespond.Answers(request.Context(), target.Identity, body.ID, body.Answers)
	} else {
		err = target.Operations.LegacyRespond.Decision(request.Context(), target.Identity, body.ID, body.Decision)
	}
	if err != nil {
		var prompt *backend.PromptError
		if errors.As(err, &prompt) {
			status := http.StatusServiceUnavailable
			message := "The connection changed before your response was sent."
			if prompt.Code == "prompt_changed" {
				status = http.StatusConflict
				message = prompt.Error()
			} else if prompt.Code == "invalid_response" {
				status = http.StatusBadRequest
				message = prompt.Error()
			} else if !prompt.NotSent {
				message = "Delivery could not be confirmed."
			}
			handler.logError(request, err)
			writeJSON(response, status, map[string]any{"error": message, "code": prompt.Code, "notSent": prompt.NotSent})
			return
		}
		handler.serverError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]bool{"ok": true})
}

const heartbeatInterval = 20 * time.Second

func (handler *Handler) events(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	if request.Method != http.MethodGet || !target.Capabilities.EventStream {
		handler.rejectOperation(response, request)
		return
	}
	if !handler.requireSupport(response, request, target.Operations.Watch != nil) {
		return
	}

	flusher, ok := response.(http.Flusher)
	if !ok {
		writeError(response, http.StatusInternalServerError, "event streaming is unavailable")
		return
	}
	events, unsubscribe, err := target.Operations.Watch.Subscribe(request.Context(), target.Identity)
	if err != nil {
		handler.serverError(response, request, err)
		return
	}
	defer unsubscribe()
	keepalive := time.NewTicker(heartbeatInterval)
	defer keepalive.Stop()
	var revision func() uint64
	if provider := target.Operations.PendingRevision; provider != nil && target.Capabilities.Pending {
		revision = func() uint64 { return provider.PendingRevision(target.Identity) }
	}
	handler.streamEvents(response, request, flusher, events, keepalive.C, revision)
}

func (handler *Handler) streamEvents(response http.ResponseWriter, request *http.Request, flusher http.Flusher, events <-chan struct{}, keepalive <-chan time.Time, revisions ...func() uint64) {
	var revision func() uint64
	if len(revisions) != 0 {
		revision = revisions[0]
	}
	var previous uint64
	ready := map[string]any{"heartbeatIntervalMs": heartbeatInterval.Milliseconds()}
	if revision != nil {
		previous = revision()
		ready["pendingInvalidation"] = true
	}
	data, _ := json.Marshal(ready)
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("X-Accel-Buffering", "no")
	if _, err := fmt.Fprintf(response, "event: ready\ndata: %s\n\n", data); err != nil {
		return
	}
	flusher.Flush()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-handler.shutdown:
			return
		case <-keepalive:
			if _, err := io.WriteString(response, "event: heartbeat\ndata: {}\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case _, open := <-events:
			if !open {
				return
			}
			if revision != nil {
				current := revision()
				if current != previous {
					if _, err := io.WriteString(response, "event: pending\ndata: update\n\n"); err != nil {
						return
					}
					previous = current
				}
			}
			if _, err := io.WriteString(response, "data: update\n\n"); err != nil {
				return
			}
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

func (handler *Handler) requireSupport(response http.ResponseWriter, request *http.Request, supported bool) bool {
	if !supported {
		writeError(response, http.StatusNotImplemented, "conversation operation is unavailable")
	}
	return supported
}

func (handler *Handler) readError(response http.ResponseWriter, request *http.Request, err error) {
	var upload *UploadError
	if errors.As(err, &upload) {
		uploadError(response, err)
		return
	}
	handler.serverError(response, request, err)
}

func (handler *Handler) queueError(response http.ResponseWriter, request *http.Request, err error) {
	var legacy *legacyQueueError
	if errors.As(err, &legacy) {
		if legacy.observation {
			uploadError(response, legacy.err)
		} else {
			handler.serverError(response, request, err)
		}
		return
	}
	handler.readError(response, request, err)
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

func nonNilPrompts(prompts []backend.Prompt) []backend.Prompt {
	if prompts == nil {
		return []backend.Prompt{}
	}
	return prompts
}

func nonNilQueue(queue []backend.QueueEntry) []backend.QueueEntry {
	if queue == nil {
		return []backend.QueueEntry{}
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
