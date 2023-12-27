package middleware

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	sqsprocessor "github.com/barrett370/sqs-processor"
)

type CustomProcessFunc[T any] func(context.Context, T) sqsprocessor.ProcessResult

type Decoder[T any] func(processFunc CustomProcessFunc[T]) sqsprocessor.ProcessFunc

func RawMessage(processFunc CustomProcessFunc[string]) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
		if msg.Body == nil {
			return sqsprocessor.ProcessResultNack
		}
		return processFunc(ctx, *msg.Body)
	}
}

func JSONDecode[T any](processFunc CustomProcessFunc[T]) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
		if msg.Body == nil {
			return sqsprocessor.ProcessResultNack
		}
		var t T
		err := json.Unmarshal([]byte(*msg.Body), &t)
		if err != nil {
			return sqsprocessor.ProcessResultNack
		}
		return processFunc(ctx, t)
	}
}

func XMLDecode[T any](processFunc CustomProcessFunc[T]) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
		if msg.Body == nil {
			return sqsprocessor.ProcessResultNack
		}
		var t T
		err := xml.Unmarshal([]byte(*msg.Body), &t)
		if err != nil {
			return sqsprocessor.ProcessResultNack
		}
		return processFunc(ctx, t)
	}
}
