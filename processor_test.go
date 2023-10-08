package sqsprocessor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqsprocessor "github.com/barrett370/sqs-processor"
	"github.com/stretchr/testify/require"
)

type mockMessage struct {
	ID string
}

type mockSQSClient struct {
	sync.Mutex
	incoming chan types.Message
	inflight []types.Message
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	var messages []types.Message
out:
	for {
		select {
		case <-time.After(time.Duration(params.WaitTimeSeconds)):
			break out
		case msg := <-m.incoming:
			println("got message")
			messages = append(messages, msg)
			m.Lock()
			m.inflight = append(m.inflight, msg)
			m.Unlock()
		}
	}
	ret := &sqs.ReceiveMessageOutput{
		Messages: messages,
	}
	return ret, nil
}
func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	fmt.Printf("got message to delete %v\n", params)
	return nil, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return nil, nil
}

func mockWorkFunc(ctx context.Context, wi mockMessage) sqsprocessor.ProcessResult {
	fmt.Printf("got work to process %+v\n", wi)
	return sqsprocessor.Ack
}

func TestProcessor(t *testing.T) {
	messages := make(chan types.Message, 100)
	c := &mockSQSClient{
		incoming: messages,
	}
	config := sqsprocessor.ProcessorConfig{
		Receive: sqs.ReceiveMessageInput{
			WaitTimeSeconds:     1,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   1,
		},
		NumWorkers: 1,
	}
	p := sqsprocessor.NewProcessor[mockMessage](c, config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Process(ctx, mockWorkFunc)
		close(done)
	}()
	msgBodyBytes, err := json.Marshal(mockMessage{ID: "333"})
	msgBody := string(msgBodyBytes)
	require.NoError(t, err)
	messages <- types.Message{
		ReceiptHandle: aws.String("1234"),
		Body:          &msgBody,
	}

	time.Sleep(time.Second * 1)

	cancel()
	<-done
}
