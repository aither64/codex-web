package codex

// RequestPhase identifies the allocation or retirement of one observed RPC.
type RequestPhase uint8

const (
	RequestStarted RequestPhase = iota + 1
	RequestFinished
)

// RequestObservation identifies one allocated RPC without parameters or results.
// WebSocketAccept is the validated downstream handshake value, not an authority
// token or proof of native receipt. Empty metadata is unusable for correlation.
// Each start has one finish, after pending removal and delivery observation.
// Different requests may call the observer concurrently. The effective RPC
// context is borrowed for the call; observers must release references at finish.
// Initialization and failures before envelope encoding emit no pair.
type RequestObservation struct {
	Phase           RequestPhase
	WebSocketAccept string
	Generation      uint64
	RequestID       uint64
	Method          string
	Failed          bool // Only meaningful on RequestFinished; describes the RPC result.
}
