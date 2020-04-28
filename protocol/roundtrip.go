package protocol

import (
	"bufio"
	"fmt"
)

// RoundTrip sends a request to a kafka broker and returns the response.
//
// The function expects that there were no other concurrent requests served by
// the connection wrapped by rw, and therefore uses a correlation ID of 42.
func RoundTrip(rw *bufio.ReadWriter, apiVersion int16, clientID string, msg Message) (Message, error) {
	const correlationID = 42
	if err := WriteRequest(rw.Writer, apiVersion, correlationID, clientID, msg); err != nil {
		return nil, err
	}
	id, res, err := ReadResponse(rw.Reader, int16(msg.ApiKey()), apiVersion)
	if err != nil {
		return nil, err
	}
	if id != correlationID {
		return nil, fmt.Errorf("correlation id mismatch (expected=%d, found=%d)", correlationID, id)
	}
	return res, nil
}
