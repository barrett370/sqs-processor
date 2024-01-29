/*
Package sqsprocessor contains an implementation of an sqs processor, similar in design to the provided pubsub client in the gcloud go sdk

The main structure is the Processor, which handles spawning, managing and feeding a pool of workers which execute a provided ProcessFunc over each message they receive.

# Basic Usage

	c := sqs.NewFromConfig(cfg)

	p := NewProcessor(c, ProcessorConfig{
		NumWorkers: 10,
		Backoff: time.Second,
		Receive: sqs.ReceiveMessageInput{
			QueueUrl: sqsQueueURL,
			MaxNumberOfMessages: 10,
			VisibilityTimeout: 2,
			WaitTimeSeconds: 1,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Process(ctx, func(ctx context.Context, message types.Message) ProcessResult {
			if message.Body == nil {
				// Delete bad messages from queue
				return ProcessResultAck
			}
			if *msg.Body == "good" {
				// Happy path
				return ProcessResultAck
			}
			// Sad path
			return ProcessResultNack
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range p.Errors() {
			log.Printf("received error from processor, %v\n", err)
		}
	}()


	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)

	<-sigC

	log.Print("Receieved signal to quit, stopping processor")
	cancel()
	wg.Wait()
	log.Print("Processor stopped, exiting")
*/
package sqsprocessor
