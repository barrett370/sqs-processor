package sqsprocessor

type Middleware func(ProcessFunc) ProcessFunc
