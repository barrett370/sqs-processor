package sqsprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type processorCleanupFunc func(context.Context, workItemResult)

// SQSClienter encapsulates all sqs methods a Processor will use
type SQSClienter interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

type ProcessorConfig struct {
	// NumWorkers is the number of worker
	// goroutines spawned, managed and
	// used by the Processor to process any
	// received messages
	NumWorkers int
	// Backoff is the amount of time the
	// Processor will block before polling
	// for new messages if none were received
	// in the previous call
	Backoff time.Duration
	// TODO abstract these?
	Receive        sqs.ReceiveMessageInput
	ReceiveOptions []func(*sqs.Options)
}

// Processor is the struct which orchestrates polling
// for messages as well as starting and feeding a configured
// number of workers in a pool
type Processor struct {
	client SQSClienter
	config ProcessorConfig
	work   chan workItem
	// TODO a better way to handle errors?
	errs chan error
}

// ProcessResult is an enum used to signal success
// or failure when processing a message in a ProcessFunc
type ProcessResult uint8

const (
	/*
		ProcessResultNack indicates that the
		ProcessFunc either does not want to
		process a message or has failed to,
		upon receiving this, the Processor
		expedites the re-processing of the
		message by making it visable in the queue
	*/
	ProcessResultNack ProcessResult = iota
	/*
				ProcessResultAck indicates that the
				ProcessFunc was successful in processing
		 		the message. Upon receiving this,
				the Processor deletes the message from
				the queue to prevent re-delivery
	*/
	ProcessResultAck
)

// ProcessFunc is the signature of functions the user provides
// to process each message received off the queue
type ProcessFunc func(ctx context.Context, msg types.Message) ProcessResult

// New returns a pointer to a new Processor given a config and sqs client
func New(c SQSClienter, config ProcessorConfig) *Processor {
	work := make(chan workItem, config.NumWorkers)

	return &Processor{
		client: c,
		config: config,
		work:   work,
	}
}

// Errors returns a channel to which any errors
// encountered during processing are sent to.
// If it has not been previously called, a new,
// blocking channel is created. If you call this method,
// make sure there is a goroutine cosuming from the
// returned channel to prevent deadlock
func (p *Processor) Errors() <-chan error {
	if p.errs == nil {
		p.errs = make(chan error)
	}
	return p.errs
}

func deadline(visibilityTimeout int32, receiveTime time.Time) time.Time {
	dur := time.Duration(visibilityTimeout) * time.Second
	return receiveTime.Add(dur)
}

/*
Process starts the processor and workers in the pool.
It passes each message received to a worker in the pool
which executes the given ProcessFunc.
To stop processing, cancel the provided context
*/
func (p *Processor) Process(ctx context.Context, pf ProcessFunc) {
	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(p.config.NumWorkers)
	for i := 0; i < p.config.NumWorkers; i++ {
		w := &worker{
			f:    pf,
			work: p.work,
		}
		go func() {
			defer wg.Done()
			w.start(ctx)
		}()
	}

	var backoff time.Duration = 0

	for {
		select {
		case <-ctx.Done():
			if p.errs != nil {
				close(p.errs)
			}
			return
		case <-time.After(backoff):
			// reset backoff
			backoff = 0

			res, err := p.client.ReceiveMessage(ctx, &p.config.Receive, p.config.ReceiveOptions...)
			if err != nil {
				p.reportError(err)
				continue
			}
			receiveTime := time.Now()

			if len(res.Messages) == 0 {
				backoff = p.config.Backoff
				continue
			}

			for _, msg := range res.Messages {
				if msg.Body == nil || msg.ReceiptHandle == nil {
					continue
				}
				select {
				case p.work <- workItem{
					cleanup: p.cleanup,
					workItemMetadata: workItemMetadata{
						ReceiptHandle: *msg.ReceiptHandle,
						Deadline:      deadline(p.config.Receive.VisibilityTimeout, receiveTime),
					},
					msg: msg,
				}:
				case <-ctx.Done():
					return
				}
			}
		}

	}
}

// ErrMessageExpired is returned when a message
// with an expired deadline is encountered and
// processing is abandoned
type ErrMessageExpired struct {
	receiptHandle string
	expiredAt     time.Time
}

func (e ErrMessageExpired) Error() string {
	return fmt.Sprintf("message has expired before cleanup finished, receiptHandle: %s, expired at: %s", e.receiptHandle, e.expiredAt)
}

// cleanup listens for WorkItemResults and tidies them according to their ProcessResult
// Ack - deleted from the queue
// Nack - published back to the queue for re-attempt (or sending to DLQ, depending on queue configuration)
func (p *Processor) cleanup(ctx context.Context, res workItemResult) {
	if time.Now().After(res.Deadline) {
		p.reportError(ErrMessageExpired{
			receiptHandle: res.ReceiptHandle,
			expiredAt:     res.Deadline,
		})
		return
	}
	switch res.ProcessResult {
	case ProcessResultAck:
		_, err := p.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      p.config.Receive.QueueUrl,
			ReceiptHandle: &res.ReceiptHandle,
		})
		if err != nil {
			p.reportError(err)
		}
	case ProcessResultNack:
		// make message instantly visible again
		_, err := p.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          p.config.Receive.QueueUrl,
			ReceiptHandle:     &res.ReceiptHandle,
			VisibilityTimeout: 0,
		})
		if err != nil {
			p.reportError(err)
		}
	}
}

func (p *Processor) reportError(err error) {
	if p.errs == nil {
		return
	}
	p.errs <- err
}
