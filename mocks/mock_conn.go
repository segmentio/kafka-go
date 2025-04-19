// Code generated by MockGen. DO NOT EDIT.
// Source: conn.go
//
// Generated by this command:
//
//	mockgen -source=conn.go -destination=mocks/mock_conn.go -package=mocks ConnIface
//

// Package mocks is a generated GoMock package.
package mocks

import (
	net "net"
	reflect "reflect"
	time "time"

	kafka_go "github.com/segmentio/kafka-go"
	gomock "go.uber.org/mock/gomock"
)

// MockConnIface is a mock of ConnIface interface.
type MockConnIface struct {
	ctrl     *gomock.Controller
	recorder *MockConnIfaceMockRecorder
	isgomock struct{}
}

// MockConnIfaceMockRecorder is the mock recorder for MockConnIface.
type MockConnIfaceMockRecorder struct {
	mock *MockConnIface
}

// NewMockConnIface creates a new mock instance.
func NewMockConnIface(ctrl *gomock.Controller) *MockConnIface {
	mock := &MockConnIface{ctrl: ctrl}
	mock.recorder = &MockConnIfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnIface) EXPECT() *MockConnIfaceMockRecorder {
	return m.recorder
}

// ApiVersions mocks base method.
func (m *MockConnIface) ApiVersions() ([]kafka_go.ApiVersion, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ApiVersions")
	ret0, _ := ret[0].([]kafka_go.ApiVersion)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ApiVersions indicates an expected call of ApiVersions.
func (mr *MockConnIfaceMockRecorder) ApiVersions() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApiVersions", reflect.TypeOf((*MockConnIface)(nil).ApiVersions))
}

// Broker mocks base method.
func (m *MockConnIface) Broker() kafka_go.Broker {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Broker")
	ret0, _ := ret[0].(kafka_go.Broker)
	return ret0
}

// Broker indicates an expected call of Broker.
func (mr *MockConnIfaceMockRecorder) Broker() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Broker", reflect.TypeOf((*MockConnIface)(nil).Broker))
}

// Brokers mocks base method.
func (m *MockConnIface) Brokers() ([]kafka_go.Broker, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Brokers")
	ret0, _ := ret[0].([]kafka_go.Broker)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Brokers indicates an expected call of Brokers.
func (mr *MockConnIfaceMockRecorder) Brokers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Brokers", reflect.TypeOf((*MockConnIface)(nil).Brokers))
}

// Close mocks base method.
func (m *MockConnIface) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockConnIfaceMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockConnIface)(nil).Close))
}

// Controller mocks base method.
func (m *MockConnIface) Controller() (kafka_go.Broker, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Controller")
	ret0, _ := ret[0].(kafka_go.Broker)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Controller indicates an expected call of Controller.
func (mr *MockConnIfaceMockRecorder) Controller() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Controller", reflect.TypeOf((*MockConnIface)(nil).Controller))
}

// DeleteTopics mocks base method.
func (m *MockConnIface) DeleteTopics(topics ...string) error {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range topics {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteTopics", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteTopics indicates an expected call of DeleteTopics.
func (mr *MockConnIfaceMockRecorder) DeleteTopics(topics ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTopics", reflect.TypeOf((*MockConnIface)(nil).DeleteTopics), topics...)
}

// LocalAddr mocks base method.
func (m *MockConnIface) LocalAddr() net.Addr {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LocalAddr")
	ret0, _ := ret[0].(net.Addr)
	return ret0
}

// LocalAddr indicates an expected call of LocalAddr.
func (mr *MockConnIfaceMockRecorder) LocalAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LocalAddr", reflect.TypeOf((*MockConnIface)(nil).LocalAddr))
}

// Offset mocks base method.
func (m *MockConnIface) Offset() (int64, int) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Offset")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(int)
	return ret0, ret1
}

// Offset indicates an expected call of Offset.
func (mr *MockConnIfaceMockRecorder) Offset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Offset", reflect.TypeOf((*MockConnIface)(nil).Offset))
}

// Read mocks base method.
func (m *MockConnIface) Read(b []byte) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", b)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockConnIfaceMockRecorder) Read(b any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockConnIface)(nil).Read), b)
}

// ReadBatch mocks base method.
func (m *MockConnIface) ReadBatch(minBytes, maxBytes int) *kafka_go.Batch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadBatch", minBytes, maxBytes)
	ret0, _ := ret[0].(*kafka_go.Batch)
	return ret0
}

// ReadBatch indicates an expected call of ReadBatch.
func (mr *MockConnIfaceMockRecorder) ReadBatch(minBytes, maxBytes any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadBatch", reflect.TypeOf((*MockConnIface)(nil).ReadBatch), minBytes, maxBytes)
}

// ReadBatchWith mocks base method.
func (m *MockConnIface) ReadBatchWith(cfg kafka_go.ReadBatchConfig) *kafka_go.Batch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadBatchWith", cfg)
	ret0, _ := ret[0].(*kafka_go.Batch)
	return ret0
}

// ReadBatchWith indicates an expected call of ReadBatchWith.
func (mr *MockConnIfaceMockRecorder) ReadBatchWith(cfg any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadBatchWith", reflect.TypeOf((*MockConnIface)(nil).ReadBatchWith), cfg)
}

// ReadFirstOffset mocks base method.
func (m *MockConnIface) ReadFirstOffset() (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadFirstOffset")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadFirstOffset indicates an expected call of ReadFirstOffset.
func (mr *MockConnIfaceMockRecorder) ReadFirstOffset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadFirstOffset", reflect.TypeOf((*MockConnIface)(nil).ReadFirstOffset))
}

// ReadLastOffset mocks base method.
func (m *MockConnIface) ReadLastOffset() (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadLastOffset")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadLastOffset indicates an expected call of ReadLastOffset.
func (mr *MockConnIfaceMockRecorder) ReadLastOffset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadLastOffset", reflect.TypeOf((*MockConnIface)(nil).ReadLastOffset))
}

// ReadMessage mocks base method.
func (m *MockConnIface) ReadMessage(maxBytes int) (kafka_go.Message, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadMessage", maxBytes)
	ret0, _ := ret[0].(kafka_go.Message)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadMessage indicates an expected call of ReadMessage.
func (mr *MockConnIfaceMockRecorder) ReadMessage(maxBytes any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadMessage", reflect.TypeOf((*MockConnIface)(nil).ReadMessage), maxBytes)
}

// ReadOffset mocks base method.
func (m *MockConnIface) ReadOffset(t time.Time) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadOffset", t)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadOffset indicates an expected call of ReadOffset.
func (mr *MockConnIfaceMockRecorder) ReadOffset(t any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadOffset", reflect.TypeOf((*MockConnIface)(nil).ReadOffset), t)
}

// ReadOffsets mocks base method.
func (m *MockConnIface) ReadOffsets() (int64, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadOffsets")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ReadOffsets indicates an expected call of ReadOffsets.
func (mr *MockConnIfaceMockRecorder) ReadOffsets() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadOffsets", reflect.TypeOf((*MockConnIface)(nil).ReadOffsets))
}

// ReadPartitions mocks base method.
func (m *MockConnIface) ReadPartitions(topics ...string) ([]kafka_go.Partition, error) {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range topics {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ReadPartitions", varargs...)
	ret0, _ := ret[0].([]kafka_go.Partition)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadPartitions indicates an expected call of ReadPartitions.
func (mr *MockConnIfaceMockRecorder) ReadPartitions(topics ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadPartitions", reflect.TypeOf((*MockConnIface)(nil).ReadPartitions), topics...)
}

// RemoteAddr mocks base method.
func (m *MockConnIface) RemoteAddr() net.Addr {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoteAddr")
	ret0, _ := ret[0].(net.Addr)
	return ret0
}

// RemoteAddr indicates an expected call of RemoteAddr.
func (mr *MockConnIfaceMockRecorder) RemoteAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoteAddr", reflect.TypeOf((*MockConnIface)(nil).RemoteAddr))
}

// Seek mocks base method.
func (m *MockConnIface) Seek(offset int64, whence int) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Seek", offset, whence)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Seek indicates an expected call of Seek.
func (mr *MockConnIfaceMockRecorder) Seek(offset, whence any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Seek", reflect.TypeOf((*MockConnIface)(nil).Seek), offset, whence)
}

// SetDeadline mocks base method.
func (m *MockConnIface) SetDeadline(t time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetDeadline", t)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetDeadline indicates an expected call of SetDeadline.
func (mr *MockConnIfaceMockRecorder) SetDeadline(t any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetDeadline", reflect.TypeOf((*MockConnIface)(nil).SetDeadline), t)
}

// SetReadDeadline mocks base method.
func (m *MockConnIface) SetReadDeadline(t time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetReadDeadline", t)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetReadDeadline indicates an expected call of SetReadDeadline.
func (mr *MockConnIfaceMockRecorder) SetReadDeadline(t any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetReadDeadline", reflect.TypeOf((*MockConnIface)(nil).SetReadDeadline), t)
}

// SetRequiredAcks mocks base method.
func (m *MockConnIface) SetRequiredAcks(n int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetRequiredAcks", n)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetRequiredAcks indicates an expected call of SetRequiredAcks.
func (mr *MockConnIfaceMockRecorder) SetRequiredAcks(n any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetRequiredAcks", reflect.TypeOf((*MockConnIface)(nil).SetRequiredAcks), n)
}

// SetWriteDeadline mocks base method.
func (m *MockConnIface) SetWriteDeadline(t time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetWriteDeadline", t)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetWriteDeadline indicates an expected call of SetWriteDeadline.
func (mr *MockConnIfaceMockRecorder) SetWriteDeadline(t any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetWriteDeadline", reflect.TypeOf((*MockConnIface)(nil).SetWriteDeadline), t)
}

// Write mocks base method.
func (m *MockConnIface) Write(b []byte) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", b)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Write indicates an expected call of Write.
func (mr *MockConnIfaceMockRecorder) Write(b any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockConnIface)(nil).Write), b)
}

// WriteCompressedMessages mocks base method.
func (m *MockConnIface) WriteCompressedMessages(codec kafka_go.CompressionCodec, msgs ...kafka_go.Message) (int, error) {
	m.ctrl.T.Helper()
	varargs := []any{codec}
	for _, a := range msgs {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WriteCompressedMessages", varargs...)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WriteCompressedMessages indicates an expected call of WriteCompressedMessages.
func (mr *MockConnIfaceMockRecorder) WriteCompressedMessages(codec any, msgs ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{codec}, msgs...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteCompressedMessages", reflect.TypeOf((*MockConnIface)(nil).WriteCompressedMessages), varargs...)
}

// WriteCompressedMessagesAt mocks base method.
func (m *MockConnIface) WriteCompressedMessagesAt(codec kafka_go.CompressionCodec, msgs ...kafka_go.Message) (int, int32, int64, time.Time, error) {
	m.ctrl.T.Helper()
	varargs := []any{codec}
	for _, a := range msgs {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WriteCompressedMessagesAt", varargs...)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(int32)
	ret2, _ := ret[2].(int64)
	ret3, _ := ret[3].(time.Time)
	ret4, _ := ret[4].(error)
	return ret0, ret1, ret2, ret3, ret4
}

// WriteCompressedMessagesAt indicates an expected call of WriteCompressedMessagesAt.
func (mr *MockConnIfaceMockRecorder) WriteCompressedMessagesAt(codec any, msgs ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{codec}, msgs...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteCompressedMessagesAt", reflect.TypeOf((*MockConnIface)(nil).WriteCompressedMessagesAt), varargs...)
}

// WriteMessages mocks base method.
func (m *MockConnIface) WriteMessages(msgs ...kafka_go.Message) (int, error) {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range msgs {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WriteMessages", varargs...)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WriteMessages indicates an expected call of WriteMessages.
func (mr *MockConnIfaceMockRecorder) WriteMessages(msgs ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteMessages", reflect.TypeOf((*MockConnIface)(nil).WriteMessages), msgs...)
}
