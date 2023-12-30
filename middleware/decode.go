package middleware

import (
	"context"
	"encoding/json"
	"encoding/xml"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	sqsprocessor "github.com/barrett370/sqs-processor"
)

// CustomProcessFunc describes a sqsprocessor.ProcessFunc
// which operates on a concrete, generic type.
type CustomProcessFunc[T any] func(context.Context, T) sqsprocessor.ProcessResult

// Decoder describes any function which transforms a sqsprocessor.ProcessFunc
// into a CustomProcessFunc operating on a given generic type, T
type Decoder[T any] func(processFunc CustomProcessFunc[T]) sqsprocessor.ProcessFunc

// RawMessage simply extracts the string value of an sqs message and
// passes it to a given CustomProcessFunc
func RawMessage(processFunc CustomProcessFunc[string]) sqsprocessor.ProcessFunc {
	return func(ctx context.Context, msg types.Message) sqsprocessor.ProcessResult {
		if msg.Body == nil {
			return sqsprocessor.ProcessResultNack
		}
		return processFunc(ctx, *msg.Body)
	}
}

// JSONDecode decodes the sqs message body into a given struct with json encoding
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

// XMLDecode decodes the sqs message into a given struct with xml encoding
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
