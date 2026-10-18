package codex

import (
	"context"
	"errors"
)

// LifecycleRequest describes the application-supplied lifecycle destination.
// ThreadID is empty for start and identifies the source thread for fork. Cwd
// can be empty on reconnect; applications can then resolve it from ThreadID.
type LifecycleRequest struct {
	Method   string
	ThreadID string
	Cwd      string
}

// lifecycleParams copies caller-owned maps before adding routing overrides.
// Keeping this at the RPC boundary also covers implicit watch restoration.
func (c *Client) lifecycleParams(ctx context.Context, method string, value any) (any, string, error) {
	switch method {
	case "thread/start", "thread/resume", "thread/fork":
	default:
		return value, "", nil
	}
	original, ok := value.(map[string]any)
	if !ok {
		if c.options.LifecycleSettings == nil && c.options.LifecycleRequestSettings == nil {
			return value, "", nil
		}
		return nil, "", errors.New("Codex lifecycle parameters must be an object")
	}
	params := make(map[string]any, len(original)+3)
	for key, value := range original {
		params[key] = value
	}
	if c.options.LifecycleSettings != nil || c.options.LifecycleRequestSettings != nil {
		var settings ThreadSettings
		var err error
		if resolve := c.options.LifecycleRequestSettings; resolve != nil {
			settings, err = resolve(ctx, LifecycleRequest{Method: method, ThreadID: stringValue(params["threadId"]), Cwd: stringValue(params["cwd"])})
		} else {
			settings, err = c.options.LifecycleSettings(ctx, stringValue(params["threadId"]))
		}
		if err != nil {
			return nil, "", err
		}
		if settings.ModelProvider != "" {
			params["modelProvider"] = settings.ModelProvider
		}
		if settings.Model != "" && stringValue(params["model"]) == "" {
			params["model"] = settings.Model
		}
		if settings.ReasoningEffort != "" {
			config := make(map[string]any)
			if existing, ok := params["config"].(map[string]any); ok {
				for key, value := range existing {
					config[key] = value
				}
			}
			if _, explicit := config["model_reasoning_effort"]; !explicit {
				config["model_reasoning_effort"] = settings.ReasoningEffort
			}
			params["config"] = config
		}
	}
	return params, stringValue(params["modelProvider"]), nil
}
