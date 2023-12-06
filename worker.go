package sqsprocessor

import (
	"context"
	"time"
)

type workItemMetadata struct {
	ReceiptHandle string
	Deadline      time.Time
}

type workItem struct {
	workItemMetadata
	cleanup processorCleanupFunc

	Body string
}

type workItemResult struct {
	ProcessResult
	workItemMetadata
}

type worker struct {
	work <-chan workItem
	f    ProcessFunc
}

func (w *worker) Start(ctx context.Context) {
	for {
		select {
		case msg := <-w.work:
			fctx, cancel := context.WithDeadline(ctx, msg.Deadline)
			res := w.f(fctx, msg.Body)
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
