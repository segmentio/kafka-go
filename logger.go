package kafka

// Logger interface API for log.Logger.
type Logger interface {
	Printf(string, ...any)
}

// LoggerFunc is a bridge between Logger and any third party logger with the
// same signature.
//
// Usage:
//
//	l := NewLogger() // some logger
//	r := kafka.NewReader(kafka.ReaderConfig{
//	  Logger:      kafka.LoggerFunc(l.Infof),
//	  ErrorLogger: kafka.LoggerFunc(l.Errorf),
//	})
type LoggerFunc func(string, ...any)

func (f LoggerFunc) Printf(msg string, args ...any) { f(msg, args...) }
