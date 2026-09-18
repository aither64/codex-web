package codex

type requestPolicy struct {
	category string
	blocking bool
}

// requestPolicyFor is shared by prompt normalization and activity recording.
func requestPolicyFor(method string) requestPolicy {
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval":
		return requestPolicy{category: "approval", blocking: true}
	case "mcpServer/elicitation/request":
		// The client rejects unsupported MCP elicitation automatically, so it
		// must not open a user-visible waiting state.
		return requestPolicy{category: "unsupported"}
	case "item/tool/requestUserInput":
		return requestPolicy{category: "userInput", blocking: true}
	default:
		return requestPolicy{category: "unknown"}
	}
}
