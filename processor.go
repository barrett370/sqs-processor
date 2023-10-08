package sqsprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type WorkItemMetadata struct {
	ReceiptHandle string
	Deadline      time.Time
}

type WorkItem[T any] struct {
	WorkItemMetadata
	Body T
}

type WorkItemResult struct {
	ProcessResult
	WorkItemMetadata
}

type ProcessorConfig struct {
	NumWorkers     int
	Receive        sqs.ReceiveMessageInput
	ReceiveOptions []func(*sqs.Options)
}

type worker[T any] struct {
	work    <-chan WorkItem[T]
	results chan<- WorkItemResult
	f       ProcessFunc[T]
}

func (w *worker[T]) Start(ctx context.Context) {
	for {
		select {
		case msg := <-w.work:
			println("got work")
			fctx, cancel := context.WithDeadline(ctx, msg.Deadline)
			res := w.f(fctx, msg.Body)
			cancel()
			w.results <- WorkItemResult{
				ProcessResult:    res,
				WorkItemMetadata: msg.WorkItemMetadata,
			}
		case <-ctx.Done():
			fmt.Println("stopping worker")
			return
		}
	}
}

type sqsClienter interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

type Processor[T any] struct {
	c       sqsClienter
	config  ProcessorConfig
	work    chan WorkItem[T]
	results chan WorkItemResult
	errs    chan error
}

type ProcessResult uint8

const (
	Nack ProcessResult = iota
	Ack
)

type ProcessFunc[T any] func(ctx context.Context, msg T) ProcessResult

func NewProcessor[T any](c sqsClienter, config ProcessorConfig) *Processor[T] {
	work := make(chan WorkItem[T], config.NumWorkers)
	results := make(chan WorkItemResult)

	return &Processor[T]{
		c:       c,
		config:  config,
		work:    work,
		results: results,
	}
}

func (p *Processor[T]) Errors() <-chan error {
	return p.errs
}

func deadline(visibilityTimeout int32, receiveTime time.Time) time.Time {
	dur := time.Duration(visibilityTimeout) * time.Second
	return receiveTime.Add(dur)
}

func (p *Processor[T]) Process(ctx context.Context, pf ProcessFunc[T]) {
	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.cleanup(ctx)
	}()

	wg.Add(p.config.NumWorkers)
	for i := 0; i < p.config.NumWorkers; i++ {
		w := &worker[T]{
			f:       pf,
			work:    p.work,
			results: p.results,
		}
		go func() {
			defer wg.Done()
			w.Start(ctx)
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// TODO ticker?
			res, err := p.c.ReceiveMessage(ctx, &p.config.Receive, p.config.ReceiveOptions...)
			if err != nil {
				p.errs <- err
				continue
			}
			receiveTime := time.Now()

			for _, msg := range res.Messages {
				if msg.Body == nil || msg.ReceiptHandle == nil {
					continue
				}
				var b T
				err := json.Unmarshal([]byte(*msg.Body), &b)
				if err != nil {
					//TODO handle
					p.errs <- err
					continue
				}
				fmt.Printf("got message %v\n", b)

				select {
				case p.work <- WorkItem[T]{
					WorkItemMetadata: WorkItemMetadata{
						ReceiptHandle: *msg.ReceiptHandle,
						Deadline:      deadline(p.config.Receive.VisibilityTimeout, receiveTime),
					},
					Body: b,
				}:
					fmt.Println("sent work")
				case <-ctx.Done():
					fmt.Println("context is done, returning", ctx.Err())
					return
				}
			}
		}

	}
}

// cleanup listens for WorkItemResults and tidies them according to their ProcessResult
// Ack - deleted from the queue
// Nack - published back to the queue for re-attempt (or sending to DLQ, depending on queue configuration)
func (p *Processor[T]) cleanup(ctx context.Context) {
	for {
		select {
		case res := <-p.results:
			if time.Now().After(res.Deadline) {
				// TODO what do?
				fmt.Println("after deadline, skipping cleanup")
				continue
			}
			switch res.ProcessResult {
			case Ack:
				fmt.Printf("got result %v\n", res)
				_, err := p.c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      p.config.Receive.QueueUrl,
					ReceiptHandle: &res.ReceiptHandle,
				})
				if err != nil {
					p.errs <- err
				}
			case Nack:
				// logging?

				// make message instantly visible again
				_, err := p.c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          p.config.Receive.QueueUrl,
					ReceiptHandle:     &res.ReceiptHandle,
					VisibilityTimeout: 0,
				})
				if err != nil {
					p.errs <- err
				}
			}
		case <-ctx.Done():
			fmt.Printf("context done %v", ctx.Err())
			return
		}

	}
}
