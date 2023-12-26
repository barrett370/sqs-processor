package middleware

import (
	"bithub.brightcove.com/alive/alive-sdk-go/aws/v2/sqs"
	"bithub.brightcove.com/alive/alive-sdk-go/util"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	sqsprocessor "github.com/barrett370/sqs-processor"
	"go.opentelemetry.io/otel/propagation"
)

type MetadataCarrier struct {
	values map[string]types.MessageAttributeValue
}

func NewMetadataCarrier(init map[string]types.MessageAttributeValue) MetadataCarrier {
	return MetadataCarrier{
		values: init,
	}
}

func (m MetadataCarrier) Get(key string) string {
	if ret := m.values[key].StringValue; ret != nil {
		return *ret
	}
	return ""
}

func (m MetadataCarrier) Set(key string, value string) {
	if m.values == nil {
		m.values = make(map[string]types.MessageAttributeValue)
	}
	m.values[key] = types.MessageAttributeValue{
		DataType:    util.Ptr("String"),
		StringValue: &value,
	}
}

func (m MetadataCarrier) Keys() (keys []string) {
	for key := range m.values {
		keys = append(keys, key)
	}
	return
}

func (m MetadataCarrier) ToMetadata() (ret middleware.Metadata) {
	for k, v := range m.values {
		ret.Set(k, v)
	}
	return
}

var defaultPropagator propagation.TraceContext

func ContextFromMessageAttributes(next sqsprocessor.ProcessFunc) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
		carrier := sqs.NewMessageAttributeValueCarrier(msg.MessageAttributes)
		ctx = defaultPropagator.Extract(ctx, carrier)
		return next(ctx, msg)
	}
}
