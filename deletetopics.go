package kafka

import (
	"time"
)

// See http://kafka.apache.org/protocol.html#The_Messages_DeleteTopics
type deleteTopicsRequestV0 struct {
	// Topics holds the topic names
	Topics []string

	// Timeout holds the time in ms to wait for a topic to be completely deleted
	// on the controller node. Values <= 0 will trigger topic deletion and return
	// immediately.
	Timeout int32
}

func (t deleteTopicsRequestV0) size() int32 {
	return sizeofStringArray(t.Topics) + sizeofInt32(t.Timeout)
}

func (t deleteTopicsRequestV0) writeTo(wb *writeBuffer) {
	wb.writeStringArray(t.Topics)
	wb.writeInt32(t.Timeout)
}

type deleteTopicsResponseV0 struct {
	// TopicErrorCodes holds per topic error codes
	TopicErrorCodes []deleteTopicsResponseV0TopicErrorCode
}

func (t deleteTopicsResponseV0) size() int32 {
	return sizeofArray(len(t.TopicErrorCodes), func(i int) int32 { return t.TopicErrorCodes[i].size() })
}

func (t *deleteTopicsResponseV0) readFrom(rb *readBuffer) {
	rb.readArray(func() {
		topicError := deleteTopicsResponseV0TopicErrorCode{}
		topicError.readFrom(rb)
		t.TopicErrorCodes = append(t.TopicErrorCodes, topicError)
	})
}

func (t deleteTopicsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.TopicErrorCodes), func(i int) { t.TopicErrorCodes[i].writeTo(wb) })
}

type deleteTopicsResponseV0TopicErrorCode struct {
	// Topic holds the topic name
	Topic string

	// ErrorCode holds the error code
	ErrorCode int16
}

func (t deleteTopicsResponseV0TopicErrorCode) size() int32 {
	return sizeofString(t.Topic) +
		sizeofInt16(t.ErrorCode)
}

func (t *deleteTopicsResponseV0TopicErrorCode) readFrom(rb *readBuffer) {
	t.Topic = rb.readString()
	t.ErrorCode = rb.readInt16()
}

func (t deleteTopicsResponseV0TopicErrorCode) writeTo(wb *writeBuffer) {
	wb.writeString(t.Topic)
	wb.writeInt16(t.ErrorCode)
}

// deleteTopics deletes the specified topics.
//
// See http://kafka.apache.org/protocol.html#The_Messages_DeleteTopics
func (c *Conn) deleteTopics(request deleteTopicsRequestV0) (deleteTopicsResponseV0, error) {
	var res deleteTopicsResponseV0

	err := c.writeOperation(
		func(deadline time.Time, id int32) {
			if request.Timeout == 0 {
				now := time.Now()
				deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
				request.Timeout = milliseconds(deadlineToTimeout(deadline, now))
			}
			c.writeRequest(deleteTopicsRequest, v0, id, request)
		},
		func(deadline time.Time) {
			res.readFrom(&c.rb)
		},
	)

	if err != nil {
		return deleteTopicsResponseV0{}, err
	}

	for _, c := range res.TopicErrorCodes {
		if c.ErrorCode != 0 {
			return res, Error(c.ErrorCode)
		}
	}

	return res, nil
}
