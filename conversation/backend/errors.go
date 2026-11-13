package backend

// CursorError is a bounded history-cursor disposition, not a native path.
type CursorError struct{ Code string }

func (e *CursorError) Error() string { return "conversation cursor: " + e.Code }

// PromptError distinguishes rejection before dispatch from uncertain delivery.
// Message is public only for the handler's validated response categories.
type PromptError struct {
	Code    string
	NotSent bool
	Message string
	Err     error
}

func (e *PromptError) Error() string { return e.Message }
func (e *PromptError) Unwrap() error { return e.Err }

// ServiceError retains an adapter cause without making it public HTTP content
// or asserting a delivery disposition.
type ServiceError struct{ Err error }

func (e *ServiceError) Error() string {
	if e.Err == nil {
		return "conversation service operation failed"
	}
	return "conversation service operation failed: " + e.Err.Error()
}
func (e *ServiceError) Unwrap() error { return e.Err }
