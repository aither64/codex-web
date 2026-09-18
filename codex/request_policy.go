package codex

type requestPolicy struct {
	category string
	blocking bool
}

// requestPolicyFor is shared by prompt normalization and activity recording.
// MCP elicitation remains unsupported by the browser prompt handler, but is a
// blocking approval for activity accounting until the server resolves it.
func requestPolicyFor(method string) requestPolicy {
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval", "mcpServer/elicitation/request":
		return requestPolicy{category: "approval", blocking: true}
	case "item/tool/requestUserInput":
		return requestPolicy{category: "userInput", blocking: true}
	default:
		return requestPolicy{category: "unknown"}
	}
}
