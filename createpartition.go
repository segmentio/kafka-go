package kafka

import (
	"bufio"
	"time"
)

//createPartitionsRequestV0
//
//see https://kafka.apache.org/protocol#The_Messages_CreatePartitions
type createPartitionsRequestV0 struct {
	Topics       []createPartitionsRequestV0Topic
	TimeoutMs    int32
	ValidateOnly bool
}

func (t createPartitionsRequestV0) size() int32 {
	return sizeofArray(len(t.Topics), func(i int) int32 { return t.Topics[i].size() }) +
		sizeofInt32(t.TimeoutMs) +
		sizeofBool(t.ValidateOnly)
}

func (t createPartitionsRequestV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.Topics), func(i int) { t.Topics[i].writeTo(wb) })
	wb.writeInt32(t.TimeoutMs)
	wb.writeBool(t.ValidateOnly)
}

type createPartitionsRequestV0Topic struct {
	Name        string
	Count       int32
	Assignments []brokerIds
}

func (t createPartitionsRequestV0Topic) size() int32 {
	return sizeofString(t.Name) +
		sizeofInt32(t.Count) +
		sizeofArray(len(t.Assignments), func(i int) int32 { return t.Assignments[i].size() })
}

func (t createPartitionsRequestV0Topic) writeTo(wb *writeBuffer) {
	wb.writeString(t.Name)
	wb.writeInt32(t.Count)
	if t.Assignments == nil {
		wb.writeArrayLen(-1)
	} else {
		wb.writeArray(len(t.Assignments), func(i int) { t.Assignments[i].writeTo(wb) })
	}
}

type brokerIds []int32

func (t brokerIds) size() int32 {
	return sizeofInt32Array([]int32(t))
}

func (t brokerIds) writeTo(wb *writeBuffer) {
	if t == nil {
		wb.writeArrayLen(-1)
	} else {
		wb.writeInt32Array([]int32(t))
	}
}

type createPartitionsResponseV0Result struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string
}

func (t createPartitionsResponseV0Result) size() int32 {
	return sizeofString(t.Name) +
		sizeofInt16(t.ErrorCode) +
		sizeofString(t.ErrorMessage)
}

func (t createPartitionsResponseV0Result) writeTo(wb *writeBuffer) {
	wb.writeString(t.Name)
	wb.writeInt16(t.ErrorCode)
	wb.writeString(t.ErrorMessage)
}

func (t *createPartitionsResponseV0Result) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.Name); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ErrorMessage); err != nil {
		return
	}
	return
}

//createPartitionsResponseV0
type createPartitionsResponseV0 struct {
	ThrottleTimeMs int32
	Results        []createPartitionsResponseV0Result
}

func (t createPartitionsResponseV0) size() int32 {
	return sizeofInt32(t.ThrottleTimeMs) +
		sizeofArray(len(t.Results), func(i int) int32 { return t.Results[i].size() })
}

func (t createPartitionsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt32(t.ThrottleTimeMs)
	wb.writeArray(len(t.Results), func(i int) { t.Results[i].writeTo(wb) })
}

func (t *createPartitionsResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMs); err != nil {
		return
	}
	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var result createPartitionsResponseV0Result
		if fnRemain, fnErr = (&result).readFrom(r, size); err != nil {
			return
		}
		t.Results = append(t.Results, result)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

func (c *Conn) createPartitions(request createPartitionsRequestV0) (createPartitionsResponseV0, error) {
	var response createPartitionsResponseV0
	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			if request.TimeoutMs == 0 {
				now := time.Now()
				deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
				request.TimeoutMs = milliseconds(deadlineToTimeout(deadline, now))
			}
			return c.writeRequest(createPartitions, v0, id, request)
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(func() (remain int, err error) {
				return (&response).readFrom(&c.rbuf, size)
			}())
		},
	)
	if err != nil {
		return response, err
	}
	for _, tr := range response.Results {
		if tr.ErrorCode != 0 {
			return response, Error(tr.ErrorCode)
		}
	}
	return response, nil
}

//Topic is a topic config for CreatePartitionsConfig
type Topic struct {
	Name        string
	Count       int32
	Assignments [][]int32
}

//CreatePartitionsConfig is a config that passing parameters to CreatePartitions Operation
type CreatePartitionsConfig struct {
	Topics       []Topic
	TimeoutMs    int32
	ValidateOnly bool
}

//CreatePartitions update topic partitions.
//
//Detailed info please look at https://cwiki.apache.org/confluence/display/KAFKA/KIP-195%3A+AdminClient.createPartitions
func (c *Conn) CreatePartitions(configs CreatePartitionsConfig) error {
	var requestV0Topic []createPartitionsRequestV0Topic
	for _, t := range configs.Topics {
		var assignments []brokerIds
		for _, b := range t.Assignments {
			assignments = append(assignments, b)
		}
		requestV0Topic = append(
			requestV0Topic,
			createPartitionsRequestV0Topic{
				Name:        t.Name,
				Count:       t.Count,
				Assignments: assignments,
			})
	}

	_, err := c.createPartitions(createPartitionsRequestV0{
		Topics:       requestV0Topic,
		TimeoutMs:    configs.TimeoutMs,
		ValidateOnly: configs.ValidateOnly,
	})

	if err != nil {
		return err
	}
	return nil
}
