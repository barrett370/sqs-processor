package sqsprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type workItemMetadata struct {
	ReceiptHandle string
	Deadline      time.Time
}

type workItem struct {
	workItemMetadata
	Body string
}

type workItemResult struct {
	ProcessResult
	workItemMetadata
}

type worker struct {
	work    <-chan workItem
	results chan<- workItemResult
	f       ProcessFunc
}

func (w *worker) Start(ctx context.Context) {
	for {
		select {
		case msg := <-w.work:
			println("got work")
			fctx, cancel := context.WithDeadline(ctx, msg.Deadline)
			res := w.f(fctx, msg.Body)
			cancel()
			w.results <- workItemResult{
				ProcessResult:    res,
				workItemMetadata: msg.workItemMetadata,
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

type ProcessorConfig struct {
	NumWorkers int
	// TODO abstract these?
	Receive        sqs.ReceiveMessageInput
	ReceiveOptions []func(*sqs.Options)
	Backoff        time.Duration
}

type Processor struct {
	client  sqsClienter
	config  ProcessorConfig
	work    chan workItem
	results chan workItemResult
	// TODO a better way to handle errors?
	errs chan error
}

type ProcessResult uint8

const (
	ProcessResultNack ProcessResult = iota
	ProcessResultAck
)

type ProcessFunc func(ctx context.Context, msgBody string) ProcessResult

func NewProcessor(c sqsClienter, config ProcessorConfig) *Processor {
	work := make(chan workItem, config.NumWorkers)
	results := make(chan workItemResult)

	return &Processor{
		client:  c,
		config:  config,
		work:    work,
		results: results,
	}
}

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

func (p *Processor) Process(ctx context.Context, pf ProcessFunc) {
	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.cleanup(ctx)
	}()

	wg.Add(p.config.NumWorkers)
	for i := 0; i < p.config.NumWorkers; i++ {
		w := &worker{
			f:       pf,
			work:    p.work,
			results: p.results,
		}
		go func() {
			defer wg.Done()
			w.Start(ctx)
		}()
	}

	var backoff time.Duration = 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			// reset backoff
			backoff = 0

			println("waiting for messages")
			res, err := p.client.ReceiveMessage(ctx, &p.config.Receive, p.config.ReceiveOptions...)
			if err != nil {
				if p.errs != nil {
					p.errs <- err
				}
				continue
			}
			receiveTime := time.Now()

			if len(res.Messages) == 0 {
				fmt.Printf("no messages received, backing off for %s\n", p.config.Backoff)
				backoff = p.config.Backoff
				continue
			}

			for _, msg := range res.Messages {
				if msg.Body == nil || msg.ReceiptHandle == nil {
					continue
				}
				select {
				case p.work <- workItem{
					workItemMetadata: workItemMetadata{
						ReceiptHandle: *msg.ReceiptHandle,
						Deadline:      deadline(p.config.Receive.VisibilityTimeout, receiveTime),
					},
					Body: *msg.Body,
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
func (p *Processor) cleanup(ctx context.Context) {
	for {
		select {
		case res := <-p.results:
			if time.Now().After(res.Deadline) {
				// TODO what do?
				fmt.Println("after deadline, skipping cleanup")
				continue
			}
			switch res.ProcessResult {
			case ProcessResultAck:
				fmt.Printf("got result %v\n", res)
				_, err := p.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      p.config.Receive.QueueUrl,
					ReceiptHandle: &res.ReceiptHandle,
				})
				if err != nil {
					if p.errs != nil {
						p.errs <- err
					}
				}
			case ProcessResultNack:
				// logging?

				// make message instantly visible again
				_, err := p.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          p.config.Receive.QueueUrl,
					ReceiptHandle:     &res.ReceiptHandle,
					VisibilityTimeout: 0,
				})
				if err != nil {
					if p.errs != nil {
						p.errs <- err
					}
				}
			}
		case <-ctx.Done():
			fmt.Printf("context done %v", ctx.Err())
			return
		}

	}
}
