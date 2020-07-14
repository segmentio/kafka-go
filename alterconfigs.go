package kafka

import (
	"bufio"
	"time"
)

// See https://kafka.apache.org/protocol#The_Messages_AlterConfigs
type alterConfigsRequestV0 struct {
	Resources    []alterConfigsRequestV0Resource
	ValidateOnly bool
}

func (t alterConfigsRequestV0) size() int32 {
	return sizeofArray(len(t.Resources), func(i int) int32 { return t.Resources[i].size() }) +
		sizeofBool(t.ValidateOnly)
}

func (t alterConfigsRequestV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.Resources), func(i int) { t.Resources[i].writeTo(wb) })
	wb.writeBool(t.ValidateOnly)
}

type alterConfigsRequestV0Resource struct {
	ResourceType  int8
	ResourceName  string
	ConfigEntries []alterConfigsResponseV0ConfigEntry
}

func (t alterConfigsRequestV0Resource) size() int32 {
	return sizeofInt8(t.ResourceType) +
		sizeofString(t.ResourceName) +
		sizeofArray(len(t.ConfigEntries), func(i int) int32 { return t.ConfigEntries[i].size() })
}

func (t alterConfigsRequestV0Resource) writeTo(wb *writeBuffer) {
	wb.writeInt8(t.ResourceType)
	wb.writeString(t.ResourceName)
	wb.writeArray(len(t.ConfigEntries), func(i int) { t.ConfigEntries[i].writeTo(wb) })
}

type alterConfigsResponseV0ConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func (t alterConfigsResponseV0ConfigEntry) size() int32 {
	return sizeofString(t.ConfigName) +
		sizeofString(t.ConfigValue)
}

func (t alterConfigsResponseV0ConfigEntry) writeTo(wb *writeBuffer) {
	wb.writeString(t.ConfigName)
	wb.writeString(t.ConfigValue)
}

func (t *alterConfigsResponseV0ConfigEntry) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.ConfigName); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ConfigValue); err != nil {
		return
	}
	return
}

type alterConfigsResponseV0Resource struct {
	ErrorCode    int16
	ErrorMessage string
	ResourceType int8
	ResourceName string
}

func (t alterConfigsResponseV0Resource) size() int32 {
	return sizeofInt16(t.ErrorCode) +
		sizeofString(t.ErrorMessage) +
		sizeofInt8(t.ResourceType) +
		sizeofString(t.ResourceName)
}

func (t alterConfigsResponseV0Resource) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeString(t.ErrorMessage)
	wb.writeInt8(t.ResourceType)
	wb.writeString(t.ResourceName)
}

func (t *alterConfigsResponseV0Resource) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ErrorMessage); err != nil {
		return
	}
	if remain, err = readInt8(r, remain, &t.ResourceType); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ResourceName); err != nil {
		return
	}

	return
}

//alterConfigsResponseV0 descirbe configs response V0 version
type alterConfigsResponseV0 struct {
	ThrottleTimeMs int32
	Resources      []alterConfigsResponseV0Resource
}

func (t alterConfigsResponseV0) size() int32 {
	return sizeofInt32(t.ThrottleTimeMs) +
		sizeofArray(len(t.Resources), func(i int) int32 { return t.Resources[i].size() })
}

func (t alterConfigsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt32(t.ThrottleTimeMs)
	wb.writeArray(len(t.Resources), func(i int) { t.Resources[i].writeTo(wb) })
}

func (t *alterConfigsResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMs); err != nil {
		return
	}
	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var resource alterConfigsResponseV0Resource
		if fnRemain, fnErr = (&resource).readFrom(r, size); err != nil {
			return
		}
		t.Resources = append(t.Resources, resource)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

type Resource struct {
	ResourceType  int8
	ResourceName  string
	ConfigEntries []ConfigEntry
}

//AlterConfig
type AlterConfig struct {
	Resources    []Resource
	ValidateOnly bool
}

func (c *Conn) alterConfigs(request alterConfigsRequestV0) (alterConfigsResponseV0, error) {
	var response alterConfigsResponseV0
	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(alterConfigs, v0, id, request)
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
	return response, nil
}

//AlterConfigs alters configuration data.
//
//Detailed info please look at https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs
func (c *Conn) AlterConfigs(config AlterConfig) error {
	var requestV0Resource []alterConfigsRequestV0Resource
	for _, t := range config.Resources {
		configEntries := []alterConfigsResponseV0ConfigEntry{}
		for _, ce := range t.ConfigEntries {
			configEntries = append(configEntries, alterConfigsResponseV0ConfigEntry{
				ConfigName:  ce.ConfigName,
				ConfigValue: ce.ConfigValue,
			})
		}

		requestV0Resource = append(
			requestV0Resource,
			alterConfigsRequestV0Resource{
				ResourceType:  t.ResourceType,
				ResourceName:  t.ResourceName,
				ConfigEntries: configEntries,
			})
	}

	_, err := c.alterConfigs(alterConfigsRequestV0{
		Resources:    requestV0Resource,
		ValidateOnly: config.ValidateOnly,
	})

	if err != nil {
		return err
	}
	return nil
}
