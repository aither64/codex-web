package codex

// IsCapacityFailure recognizes the native overload discriminator and one exact
// wrapped capacity error emitted as "other". It does not change the raw error
// or establish stopped execution, retry authority or a new attempt's budget.
func IsCapacityFailure(code, message string) bool {
	return code == "serverOverloaded" || code == "other" &&
		message == "stream disconnected before completion: Codex is at capacity, please try again later"
}
