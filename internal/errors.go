package internal

import "errors"

var (
	ErrConnectionFailed = errors.New("connection to Kafka broker failed")
	ErrRequestTimeout   = errors.New("request timed out")
	ErrInvalidResponse  = errors.New("invalid response from Kafka broker")
)
