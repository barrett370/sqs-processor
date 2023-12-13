package middleware

import (
	"context"
	"encoding/json"
	"encoding/xml"

	sqsprocessor "github.com/barrett370/sqs-processor"
)

type CustomProcessFunc[T any] func(context.Context, T) sqsprocessor.ProcessResult

func JSONDecode[T any](processFunc CustomProcessFunc[T]) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msgBody string) sqsprocessor.ProcessResult {
		var t T
		err := json.Unmarshal([]byte(msgBody), &t)
		if err != nil {
			return sqsprocessor.ProcessResultNack
		}
		return processFunc(ctx, t)
	}
}

func XMLDecode[T any](processFunc CustomProcessFunc[T]) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msgBody string) sqsprocessor.ProcessResult {
		var t T
		err := xml.Unmarshal([]byte(msgBody), &t)
		if err != nil {
			return sqsprocessor.ProcessResultNack
		}
		return processFunc(ctx, t)
	}
}
