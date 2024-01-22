package sqsprocessor

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type workItemMetadata struct {
	ReceiptHandle string
	Deadline      time.Time
}

type workItem struct {
	workItemMetadata
	cleanup processorCleanupFunc

	msg types.Message
}

type workItemResult struct {
	ProcessResult
	workItemMetadata
}

type worker struct {
	work <-chan workItem
	f    ProcessFunc
}

func (w *worker) start(ctx context.Context) {
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
