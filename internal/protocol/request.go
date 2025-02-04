package protocol

type Request interface {
	ApiKey() int16
	ApiVersion() int16
	Encode() []byte
	Size() int32
}
