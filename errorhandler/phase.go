package errorhandler

import (
	"context"
)

// ErrorPhase indicates where in the processing pipeline an error occurred
type ErrorPhase int

const (
	PhaseUnknown    ErrorPhase = iota // zero value - uninitialized phase
	PhaseSerde                        // error during key/value serialization or deserialization
	PhaseProcessing                   // error during processor execution
	PhaseProduction                   // error during sink production
)

func (p ErrorPhase) String() string {
	switch p {
	case PhaseUnknown:
		return "unknown"
	case PhaseSerde:
		return "serde"
	case PhaseProcessing:
		return "processing"
	case PhaseProduction:
		return "production"
	default:
		return "unknown"
	}
}

var _ Handler = (*PhaseRouter)(nil)

type PhaseRouter struct {
	handler           Handler
	serdeHandler      Handler
	processingHandler Handler
	productionHandler Handler
}

// NewPhaseRouter creates a new PhaseRouter with the provided handlers for each phase.
// If a handler for a specific phase is nil, the router will fall back to the default handler.
// If the default handler is unset, defaults to SilentFail, which fails without logging at the error handler level.
func NewPhaseRouter(
	handler Handler, serdeHandler Handler, processingHandler Handler, productionHandler Handler,
) *PhaseRouter {
	if handler == nil {
		handler = SilentFail()
	}

	return &PhaseRouter{
		handler:           handler,
		serdeHandler:      serdeHandler,
		processingHandler: processingHandler,
		productionHandler: productionHandler,
	}
}

func (r *PhaseRouter) Handle(ctx context.Context, ec ErrorContext) Action {
	switch ec.Phase {
	case PhaseSerde:
		if r.serdeHandler != nil {
			return r.serdeHandler.Handle(ctx, ec)
		}
	case PhaseProcessing:
		if r.processingHandler != nil {
			return r.processingHandler.Handle(ctx, ec)
		}
	case PhaseProduction:
		if r.productionHandler != nil {
			return r.productionHandler.Handle(ctx, ec)
		}
	case PhaseUnknown:
	default:
	}

	return r.handler.Handle(ctx, ec)
}
