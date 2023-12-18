# SQS Processor


Attempt to wrap AWS SQS client with functionality similar to that of the gcloud pub/sub client.

## Usage 

The processor will simply run a given `ProcessFunc` over any messages found on a given SQS queue. `ProcessFunc` takes the sqs message body in as a string and decides how to decode and action on the message.

The `ProcessFunc` must return either `ProcessResultAck` or `ProcessResultNack`. `Ack` implies a success and leads to the message being deleted from the queue, whereas `Nack` will re-publish the message to the queue. 

### Example 

```go
import (
    "context"
    "time"

    "github.com/barrett370/sqs-processor/middleware"
    sqsprocessor "github.com/barrett370/sqs-processor"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type messageBody struct {
    ID string
    Action EnumType
}

func (s *service) process(ctx context.Context, message messageBody) (ret sqsprocessor.ProcessResult) {
    err := s.DoAction(message.ID, message.Action)
    if err == nil {
        ret = sqsprocessor.ProcessResultAck
    }
    return
}


func main() {
    // initialise v2 sqs client
    c := newClient() 

	config := sqsprocessor.ProcessorConfig{
		Receive: sqs.ReceiveMessageInput{
			WaitTimeSeconds:     10,
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   2,
		},
		NumWorkers: 10,
		Backoff:    time.Second,
	}

	p := sqsprocessor.NewProcessor(c, config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})


    cleanup := func() {
        cancel()
        <- done
    }

	go func() {
		p.Process(ctx, middleware.JSONDecode(svc.process))
		close(done)
	}()

    // some other code

    cleanup()
}
```