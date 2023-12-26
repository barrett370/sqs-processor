package sqsprocessor

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/propagation"
	"time"
)

type workItemMetadata struct {
	ReceiptHandle string
	Deadline      time.Time
}

type workItem struct {
	workItemMetadata
	cleanup processorCleanupFunc

	msg        types.Message
	Body       string
	Attributes map[string]types.MessageAttributeValue
}

type workItemResult struct {
	ProcessResult
	workItemMetadata
}

type worker struct {
	work       <-chan workItem
	f          ProcessFunc
	tracing    bool
	propagator propagation.TextMapPropagator
}

func (w *worker) Start(ctx context.Context) {
	for {
		select {
		case msg := <-w.work:
			fctx, cancel := context.WithDeadline(ctx, msg.Deadline)
			res := w.f(fctx, msg.msg)
			msg.cleanup(fctx, workItemResult{
				ProcessResult:    res,
				workItemMetadata: msg.workItemMetadata,
			})
			cancel()
		case <-ctx.Done():
			return
		}
	}
}
