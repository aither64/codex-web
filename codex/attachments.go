package codex

// Attachment is application-owned presentation metadata, not an App Server
// input type. Files are resolved to prompt text by the embedding application.
type Attachment struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Size        int64    `json:"size"`
	State       string   `json:"state"`
	DownloadURL string   `json:"downloadUrl,omitempty"`
	DeleteURL   string   `json:"deleteUrl,omitempty"`
	References  []string `json:"references,omitempty"`
}
