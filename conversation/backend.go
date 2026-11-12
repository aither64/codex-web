package conversation

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/aither64/codex-web/conversation/backend"
)

// NewHandler retains the Codex embedding API and adapts it to common dispatch.
func NewHandler(options Options) (*Handler, error) {
	if options.Resolver == nil {
		return nil, errors.New("conversation resolver is required")
	}
	handler, err := NewBackendHandler(BackendOptions{
		Resolver: BackendResolverFunc(func(ctx context.Context, request ResolveRequest) (BackendTarget, error) {
			target, err := options.Resolver.ResolveConversation(ctx, request)
			return adaptLegacyTarget(target), err
		}),
		AllowedOrigins: options.AllowedOrigins, BasePath: options.BasePath,
		MaxBodyBytes: options.MaxBodyBytes, MaxMessageBytes: options.MaxMessageBytes,
		OperationTimeout: options.OperationTimeout, Shutdown: options.Shutdown, Logger: options.Logger,
	})
	if err != nil {
		return nil, err
	}
	handler.pageCursorMinBytes = 32
	handler.pageCursorMaxBytes = 32
	handler.pageQueryMaxBytes = 256
	return handler, nil
}

// BackendResolver resolves application authorization and trusted adapter routing
// independently for every request. Browser input never selects native authority.
type BackendResolver interface {
	ResolveBackend(context.Context, ResolveRequest) (BackendTarget, error)
}

type BackendResolverFunc func(context.Context, ResolveRequest) (BackendTarget, error)

func (fn BackendResolverFunc) ResolveBackend(ctx context.Context, request ResolveRequest) (BackendTarget, error) {
	return fn(ctx, request)
}

// BackendTarget separates supported operations from authorization grants.
// Identity and every provider are trusted application values. Release runs after
// the request, including the lifetime of an event stream.
type BackendTarget struct {
	// Set only by the legacy constructor for its historical attachment-free
	// queue reconciliation no-op. It never advertises native completion support.
	legacyQueueReconcile bool
	Identity             backend.Identity
	Operations           backend.Operations
	Capabilities         Capabilities
	MutationLock         MutationLocker
	Release              func()
	ModelCatalog         func(context.Context) ([]backend.Model, error)
	Attachments          backend.AttachmentPreparer
	TransformTranscript  func(*backend.Transcript)
	PresentTranscript    func(*backend.Transcript)
	Retry                RetryProvider
	RetryActivity        RetryActivityProvider
}

// BackendOptions has the same HTTP policy as Options, with a neutral resolver.
type BackendOptions struct {
	Resolver         BackendResolver
	AllowedOrigins   []string
	BasePath         string
	MaxBodyBytes     int64
	MaxMessageBytes  int
	OperationTimeout time.Duration
	Shutdown         <-chan struct{}
	Logger           *log.Logger
}

func (target BackendTarget) models(ctx context.Context) ([]backend.Model, error) {
	if target.ModelCatalog != nil {
		return target.ModelCatalog(ctx)
	}
	return target.Operations.Settings.Models(ctx, target.Identity)
}
