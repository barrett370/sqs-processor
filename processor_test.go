package sqsprocessor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqsprocessor "github.com/barrett370/sqs-processor"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	methodReceive          = "receive"
	methodDelete           = "delete"
	methodChangeVisibility = "change_visibility"
)

type mockMessage struct {
	ID string
}

type mockSQSClient struct {
	sync.Mutex
	called   map[string]int
	incoming chan types.Message
	inflight []types.Message
	cfg      sqs.ReceiveMessageInput
}

func (m *mockSQSClient) incr(method string) {
	m.Lock()
	m.called[method]++
	m.Unlock()
}

func (m *mockSQSClient) deleteInflight(handle string) (found bool) {
	m.Lock()
	defer m.Unlock()
	var n int
	for _, msg := range m.inflight {
		if handle != *msg.ReceiptHandle {
			m.inflight[n] = msg
			n++
		} else {
			found = true
		}
	}
	m.inflight = m.inflight[:n]
	return
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.incr(methodReceive)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var messages []types.Message
out:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(params.WaitTimeSeconds) * time.Second):
			break out
		case msg := <-m.incoming:
			println("got message")
			messages = append(messages, msg)
			m.Lock()
			m.inflight = append(m.inflight, msg)
			go func() {
				<-time.After(time.Duration(m.cfg.VisibilityTimeout) * time.Second)
				found := m.deleteInflight(*msg.ReceiptHandle)
				if found {
					println("republishing message")
					m.incoming <- msg
				}
			}()
			m.Unlock()
		}
	}
	ret := &sqs.ReceiveMessageOutput{
		Messages: messages,
	}
	return ret, nil
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.incr(methodDelete)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	fmt.Printf("got message to delete %v, %v\n", params, m.inflight)
	found := m.deleteInflight(*params.ReceiptHandle)
	fmt.Printf("deleted message %v\n", m.inflight)
	var err error
	if !found {
		err = errors.New("message not found")
	}
	return nil, err
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	fmt.Printf("changing visibility of %s to %d", *params.ReceiptHandle, params.VisibilityTimeout)
	m.incr(methodChangeVisibility)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, nil
}

type results struct {
	sync.Mutex
	messages []mockMessage
}

var res *results

type mockService struct {
	mock.Mock
}

func (m *mockService) mockWorkFunc(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
	var wi mockMessage
	err := json.Unmarshal([]byte(*msg.Body), &wi)
	if err != nil {
		return sqsprocessor.ProcessResultNack
	}
	args := m.Called(wi)

	fmt.Printf("got work to process %+v\n", wi)
	res.Lock()
	defer res.Unlock()
	res.messages = append(res.messages, wi)
	return args.Get(0).(sqsprocessor.ProcessResult)
}

func TestProcessor(t *testing.T) {
	msvc := &mockService{}
	res = &results{}
	messages := make(chan types.Message, 100)
	c := &mockSQSClient{
		called:   map[string]int{},
		incoming: messages,
		cfg:      sqs.ReceiveMessageInput{VisibilityTimeout: 2},
	}
	config := sqsprocessor.ProcessorConfig{
		Receive: sqs.ReceiveMessageInput{
			WaitTimeSeconds:     1,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   2,
		},
		NumWorkers: 2,
		Backoff:    time.Millisecond * 100,
	}
	p := sqsprocessor.New(c, config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cleanup := func() {
		cancel()
		<-done
	}
	go func() {
		p.Process(ctx, msvc.mockWorkFunc)
		close(done)
	}()
	msg1 := mockMessage{ID: "333"}
	msgBodyBytes, err := json.Marshal(msg1)
	require.NoError(t, err)
	msgBody := string(msgBodyBytes)
	msvc.On("mockWorkFunc", msg1).Return(sqsprocessor.ProcessResultAck)
	messages <- types.Message{
		ReceiptHandle: aws.String("1234"),
		Body:          &msgBody,
	}

	msg2 := mockMessage{ID: "444"}
	msg2BodyBytes, err := json.Marshal(msg2)
	require.NoError(t, err)
	msg2Body := string(msg2BodyBytes)
	msvc.On("mockWorkFunc", msg2).Return(sqsprocessor.ProcessResultNack)
	messages <- types.Message{
		ReceiptHandle: aws.String("1235"),
		Body:          &msg2Body,
	}
	time.Sleep(time.Millisecond * 1500)
	cleanup()

	// res.Lock()
	// require.Len(t, res.messages, 2)
	// res.Unlock()

	require.Equal(t, 2, c.called[methodReceive])
	require.Equal(t, 1, c.called[methodDelete])
	require.Equal(t, 1, c.called[methodChangeVisibility])

}

type benchSQSClient struct {
	consumed *atomic.Int64
}

var (
	benchBody   = "bench"
	benchHandle = "123"
)

func (m *benchSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	var messages []types.Message
	for i := 0; i < int(params.MaxNumberOfMessages); i++ {
		messages = append(messages, types.Message{Body: &benchBody, ReceiptHandle: &benchHandle})
	}
	ret := &sqs.ReceiveMessageOutput{
		Messages: messages[:params.MaxNumberOfMessages],
	}
	return ret, nil
}

func (m *benchSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	_ = m.consumed.Add(1)
	return nil, nil
}

func (m *benchSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return nil, nil
}

func benchWorkFunc(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
	select {
	case <-ctx.Done():
		return sqsprocessor.ProcessResultNack
	case <-time.After(time.Duration(rand.Intn(100)) * time.Millisecond):
	}
	return sqsprocessor.ProcessResultAck
}

func TestProcessorThroughput(t *testing.T) {

	c := &benchSQSClient{
		consumed: &atomic.Int64{},
	}

	testcases := []struct {
		name   string
		config sqsprocessor.ProcessorConfig
	}{
		{
			name: "foo",
			config: sqsprocessor.ProcessorConfig{
				Receive: sqs.ReceiveMessageInput{
					MaxNumberOfMessages: 10,
					WaitTimeSeconds:     1,
					VisibilityTimeout:   2,
				},
				NumWorkers: 100,
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			p := sqsprocessor.New(c, tt.config)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				p.Process(ctx, benchWorkFunc)
				close(done)
			}()
			time.Sleep(time.Second * 1)
			cancel()
			<-done
			fmt.Printf("processed %d messages\n", c.consumed.Load())
			c.consumed.Store(0)
		})
	}

}
