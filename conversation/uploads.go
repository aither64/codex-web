package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aither64/codex-web/codex"
)

// AttachmentProvider is optional. The application resolves uploaded IDs within
// the trusted conversation and durably freezes the generated prompt before any
// App Server submission. Its observations must preserve Text and receipt digests.
type AttachmentProvider interface {
	Prepare(context.Context, string, string, string, []string) (string, error)
	ObserveTranscript(context.Context, *codex.Transcript) error
	ObserveQueue(context.Context, []codex.QueueEntry) error
	QueueDeleted(context.Context, string) error
}

// QueueDeletionCompleter is required only for attachment-aware queue deletion.
// Completion runs before the durable deletion identity is forgotten. Refresh
// reconciliation must run under mutation authority and never delete an entry
// that is still in the queue.
type QueueDeletionCompleter interface {
	DeleteQueueEntryWithCompletion(context.Context, string, string, func() error) error
	ReconcileQueueDeletionsWithCompletion(context.Context, string, func(string) error) error
}

type UploadLimits struct {
	FileBytes      int64 `json:"fileBytes"`
	PromptBytes    int64 `json:"promptBytes"`
	SessionBytes   int64 `json:"sessionBytes"`
	WorkspaceBytes int64 `json:"workspaceBytes"`
	ChunkBytes     int64 `json:"chunkBytes"`
	Files          int   `json:"files"`
}

type Upload struct {
	codex.Attachment
	Offset    int64    `json:"offset"`
	Checksums []string `json:"checksums,omitempty"`
}

type UploadRequest struct {
	ClientID string `json:"clientId"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
}

type UploadContent struct {
	File interface {
		io.Reader
		io.Seeker
		io.Closer
	}
	Name     string
	Modified time.Time
}

// UploadStore owns storage and lifetime policy. Append must commit only complete,
// checksum-verified chunks and reject offsets that do not match durable progress.
// Open returns only completed files authorized for the resolved scope.
type UploadStore interface {
	Limits() UploadLimits
	List(context.Context) ([]Upload, error)
	Create(context.Context, UploadRequest) (Upload, error)
	Status(context.Context, string) (Upload, error)
	Append(context.Context, string, int64, string, io.Reader) (Upload, error)
	Complete(context.Context, string) (Upload, error)
	Delete(context.Context, string, bool) error
	Open(context.Context, string) (UploadContent, error)
}

// UploadError exposes an actionable application error without disclosing
// filesystem paths or internal failures through the HTTP handler.
type UploadError struct {
	Status  int
	Message string
}

func (err *UploadError) Error() string { return err.Message }

type UploadTarget struct {
	Store    UploadStore
	Writable bool
	Release  func()
}

type UploadOptions struct {
	BasePath       string
	AllowedOrigins []string
	Resolve        func(context.Context, string, bool) (UploadTarget, error)
}

// NewUploadHandler serves /{scope}, /{scope}/{id}, and /{scope}/{id}/content
// below BasePath. Scopes are application-owned opaque identities; draft scopes
// need not have a Codex thread. Every request is resolved independently.
func NewUploadHandler(options UploadOptions) (http.Handler, error) {
	if options.Resolve == nil || len(options.AllowedOrigins) == 0 ||
		!strings.HasPrefix(options.BasePath, "/") || strings.HasSuffix(options.BasePath, "/") ||
		!basePathPattern.MatchString(options.BasePath) || strings.Contains(options.BasePath, "..") {
		return nil, errors.New("upload handler requires a resolver, origins and an absolute base path")
	}
	for _, origin := range options.AllowedOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "" ||
			parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, errors.New("upload origin must be an exact HTTPS origin")
		}
	}
	// Bound simultaneous chunk buffers even when clients ignore browser limits.
	transfers := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mutation := r.Method != http.MethodGet && r.Method != http.MethodHead
		origin := r.Header.Get("Origin")
		if (origin != "" && !slices.Contains(options.AllowedOrigins, origin)) || (mutation && origin == "") {
			writeError(w, http.StatusForbidden, "request origin is not allowed")
			return
		}
		path, found := strings.CutPrefix(r.URL.EscapedPath(), options.BasePath+"/")
		parts := strings.Split(path, "/")
		if !found || len(parts) > 3 || slices.Contains(parts, "") {
			http.NotFound(w, r)
			return
		}
		for _, part := range parts {
			if !validOpaqueID(part) || encodeOpaquePathSegment(part) != part {
				http.NotFound(w, r)
				return
			}
		}
		if len(parts) == 3 && (parts[2] != "content" || mutation) {
			http.NotFound(w, r)
			return
		}
		// Downloads are streamed for their request lifetime. Each upload request
		// has a separate deadline; a large file never holds one long request.
		if len(parts) != 3 {
			deadline := time.Now().Add(2 * time.Minute)
			controller := http.NewResponseController(w)
			_ = controller.SetReadDeadline(deadline)
			defer controller.SetReadDeadline(time.Time{})
			ctx, cancel := context.WithDeadline(r.Context(), deadline)
			defer cancel()
			r = r.WithContext(ctx)
		}
		if r.Method == http.MethodPatch {
			select {
			case transfers <- struct{}{}:
				defer func() { <-transfers }()
			default:
				writeError(w, http.StatusTooManyRequests, "Too many uploads are transferring; retry shortly")
				return
			}
		}
		target, err := options.Resolve(r.Context(), parts[0], mutation)
		if err != nil {
			uploadError(w, err)
			return
		}
		if target.Release != nil {
			defer target.Release()
		}
		if target.Store == nil {
			http.NotFound(w, r)
			return
		}
		if mutation && !target.Writable {
			writeError(w, http.StatusForbidden, "uploads are read-only")
			return
		}
		store := target.Store
		ctx := r.Context()
		var result any
		status := http.StatusOK
		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				var files []Upload
				files, err = store.List(ctx)
				if files == nil {
					files = []Upload{}
				}
				result = struct {
					Limits UploadLimits `json:"limits"`
					Files  []Upload     `json:"files"`
				}{store.Limits(), files}
			case http.MethodPost:
				var request UploadRequest
				decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
				decoder.DisallowUnknownFields()
				if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
					writeError(w, http.StatusBadRequest, "invalid upload request")
					return
				}
				result, err = store.Create(ctx, request)
				status = http.StatusCreated
			default:
				writeError(w, http.StatusMethodNotAllowed, "upload operation is unavailable")
				return
			}
		} else if len(parts) == 3 {
			var content UploadContent
			content, err = store.Open(ctx, parts[1])
			if err == nil {
				defer content.File.Close()
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": content.Name}))
				http.ServeContent(w, r, content.Name, content.Modified, content.File)
				return
			}
		} else {
			switch r.Method {
			case http.MethodGet:
				result, err = store.Status(ctx, parts[1])
			case http.MethodPatch:
				offset, parseErr := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
				checksum := r.Header.Get("Upload-Checksum")
				if parseErr != nil || offset < 0 || !messageDigestPattern.MatchString(checksum) {
					writeError(w, http.StatusBadRequest, "upload offset or checksum is invalid")
					return
				}
				result, err = store.Append(ctx, parts[1], offset, checksum, http.MaxBytesReader(w, r.Body, store.Limits().ChunkBytes))
			case http.MethodPost:
				result, err = store.Complete(ctx, parts[1])
			case http.MethodDelete:
				err = store.Delete(ctx, parts[1], r.URL.Query().Get("confirmed") == "true")
				result = map[string]bool{"ok": err == nil}
			default:
				writeError(w, http.StatusMethodNotAllowed, "upload operation is unavailable")
				return
			}
		}
		if err != nil {
			uploadError(w, err)
			return
		}
		writeJSON(w, status, result)
	}), nil
}

func uploadError(w http.ResponseWriter, err error) {
	var exposed *UploadError
	if errors.As(err, &exposed) && exposed.Status >= 400 && exposed.Status <= 599 {
		writeError(w, exposed.Status, exposed.Message)
		return
	}
	writeError(w, http.StatusInternalServerError, "file operation failed; retry shortly")
}

func (handler *Handler) prepareAttachments(ctx context.Context, target Target, kind, id, message string, ids []string) (string, error) {
	if len(ids) > 100 {
		return "", &UploadError{http.StatusBadRequest, "too many attachments"}
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !browserAttemptPattern.MatchString(id) || seen[id] {
			return "", &UploadError{http.StatusBadRequest, "attachment IDs are invalid or repeated"}
		}
		seen[id] = true
	}
	if target.Attachments == nil {
		if len(ids) != 0 {
			return "", &UploadError{http.StatusBadRequest, "attachments are unavailable"}
		}
		return message, nil
	}
	prepared, err := target.Attachments.Prepare(ctx, kind, id, message, ids)
	if err != nil {
		return "", err
	}
	if len(prepared) > handler.maxMessageBytes || strings.TrimSpace(prepared) == "" {
		return "", &UploadError{http.StatusBadRequest, "message and attachment references exceed the prompt limit"}
	}
	return prepared, nil
}
