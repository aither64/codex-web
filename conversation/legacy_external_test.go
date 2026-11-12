package conversation_test

import (
	"context"
	"testing"

	"github.com/aither64/codex-web/codex"
	"github.com/aither64/codex-web/conversation"
)

// Compile the original external embedding surface, including optional contracts.
var (
	_ conversation.Client = (*codex.Client)(nil)
	_ conversation.TranscriptPageReader = (*codex.Client)(nil)
	_ conversation.PromptResponder = (*codex.Client)(nil)
	_ conversation.PendingRevisionProvider = (*codex.Client)(nil)
	_ conversation.QueueDeletionCompleter = (*codex.Client)(nil)
	_ conversation.ActivityProvider = (*codex.Client)(nil)
)

func TestLegacyExternalEmbeddingRemainsSourceCompatible(t *testing.T) {
	client := codex.New("/unused")
	defer client.Close()
	_, err := conversation.NewHandler(conversation.Options{
		AllowedOrigins: []string{"https://portal.test"},
		Resolver: conversation.ResolverFunc(func(context.Context, conversation.ResolveRequest) (conversation.Target, error) {
			return conversation.Target{
				Client: client, ThreadID: "trusted", Directory: "/trusted",
				Capabilities: conversation.Capabilities{Read:true},
				MutationLock: conversation.NewMutationLock(), Activity: client,
				ModelCatalog: func(context.Context)([]codex.Model,error){return nil,nil},
				TransformTranscript: func(*codex.Transcript){},
				PresentTranscript: func(*codex.Transcript){},
			},nil
		}),
	})
	if err != nil { t.Fatal(err) }
	// Construction does not connect to or create a native server.
}
