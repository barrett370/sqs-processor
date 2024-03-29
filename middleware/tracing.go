package middleware

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqsprocessor "github.com/barrett370/sqs-processor"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// MessageAttributeValueCarrier implements opentelemetry's
// propagation.TextMapCarrier interface for injecting and
// extracting traces into sqs messages
type MessageAttributeValueCarrier struct {
	values map[string]types.MessageAttributeValue
}

// NewMessageAttributeValueCarrier returns a pointer to a
// new MessageAttributeCarrier and can be initalised with
// a map of string to types.MessageAttributeValue,
// provide nil if using for injection
func NewMessageAttributeValueCarrier(init map[string]types.MessageAttributeValue) *MessageAttributeValueCarrier {
	return &MessageAttributeValueCarrier{
		values: init,
	}
}

func (m *MessageAttributeValueCarrier) Get(key string) string {
	if ret := m.values[key].StringValue; ret != nil {
		return *ret
	}
	return ""
}

func (m *MessageAttributeValueCarrier) Set(key string, value string) {
	if m.values == nil {
		m.values = make(map[string]types.MessageAttributeValue)
	}
	m.values[key] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: &value,
	}
}

func (m *MessageAttributeValueCarrier) Keys() (keys []string) {
	for key := range m.values {
		keys = append(keys, key)
	}
	return
}

// ContextFromMessageAttributes assumes the MessageAttributeValueCarrier was used alongside a
// propagation.TraceContext to inject a trace from the sender
func ContextFromMessageAttributes(tracer trace.Tracer, spanName string) func(next sqsprocessor.ProcessFunc) sqsprocessor.ProcessFunc {
	return func(next sqsprocessor.ProcessFunc) sqsprocessor.ProcessFunc {
		return func(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
			carrier := NewMessageAttributeValueCarrier(msg.MessageAttributes)
			ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
			ctx, span := tracer.Start(ctx, spanName)
			defer span.End()
			span.AddEvent("received message")
			res := next(ctx, msg)

			switch res {
			case sqsprocessor.ProcessResultNack:
				span.SetStatus(codes.Error, "error processing message")
			case sqsprocessor.ProcessResultAck:
				span.SetStatus(codes.Ok, "successfully processed message")
			}

			span.AddEvent("finished processing")
			return res
		}
	}
}

/*
ContextFromMessageBody requires that the concrete message type implements the
open telemetry propagation.TextMapCarrier interface, such as embedding the
propagation.MapCarrier type: e.g.

	type Message struct {
		AField string `json:"a_field"`
		propagation.MapCarrier `json:"trace"`
	}
*/
func ContextFromMessageBody[T propagation.TextMapCarrier](tracer trace.Tracer, spanName string) func(next CustomProcessFunc[T]) CustomProcessFunc[T] {
	return func(next CustomProcessFunc[T]) CustomProcessFunc[T] {
		return func(ctx context.Context, i T) sqsprocessor.ProcessResult {
			ctx = otel.GetTextMapPropagator().Extract(ctx, i)
			ctx, span := tracer.Start(ctx, spanName)
			defer span.End()
			span.AddEvent("received message")
			res := next(ctx, i)

			switch res {
			case sqsprocessor.ProcessResultNack:
				span.SetStatus(codes.Error, "error processing message")
			case sqsprocessor.ProcessResultAck:
				span.SetStatus(codes.Ok, "successfully processed message")
			}

			span.AddEvent("finished processing")
			return res
		}
	}
}
