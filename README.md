# SQS Processor (WIP)


Attempt to wrap AWS SQS client with functionality similar to that of the gcloud pub/sub client.

## Usage 

The processor will simply run a given `ProcessFunc[T]` over any messages found on a given SQS queue. It will unmarshal json from a the sqs message body into `T` before passing it to the `ProcessFunc`. 

The `ProcessFunc` must return either `ProcessResultAck` or `ProcessResultNack`. `Ack` implies a success and leads to the message being deleted from the queue, whereas `Nack` will re-publish the message to the queue. 

### Example 

```go
type messageBody struct {
    ID string
    Action EnumType
}

func (s *service) process(ctx context.Context, message messageBody) (ret sqsprocessor.ProcessResult) {
    err := s.DoAction(message.Action)
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
	p := sqsprocessor.NewProcessor[messageBody](c, config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})


    cleanup := func() {
        cancel()
        <- done
    }

	go func() {
		p.Process(ctx, process)
		close(done)
	}()

...

    cleanup()
}
```