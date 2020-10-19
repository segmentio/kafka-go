package kafka

import (
	"bufio"
	"time"
)

type describeConfigsRequestV0Resource struct {
	ResourceType int8
	ResourceName string
	ConfigNames  []string
}

func (t describeConfigsRequestV0Resource) size() int32 {
	return sizeofInt8(t.ResourceType) +
		sizeofString(t.ResourceName) +
		sizeofStringArray(t.ConfigNames)
}

func (t describeConfigsRequestV0Resource) writeTo(wb *writeBuffer) {
	wb.writeInt8(t.ResourceType)
	wb.writeString(t.ResourceName)
	if t.ConfigNames == nil {
		wb.writeArrayLen(-1)
	} else {
		wb.writeStringArray([]string(t.ConfigNames))
	}
}

// See https://kafka.apache.org/protocol#The_Messages_DescribeConfigs
type describeConfigsRequestV0 struct {
	Resources []describeConfigsRequestV0Resource
}

func (t describeConfigsRequestV0) size() int32 {
	return sizeofArray(len(t.Resources), func(i int) int32 { return t.Resources[i].size() })
}

func (t describeConfigsRequestV0) writeTo(wb *writeBuffer) {
	wb.writeArray(len(t.Resources), func(i int) { t.Resources[i].writeTo(wb) })
}

type describeConfigsResponseV0ConfigEntry struct {
	ConfigName  string
	ConfigValue string
	ReadOnly    bool
	IsDefault   bool
	IsSensitive bool
}

func (t describeConfigsResponseV0ConfigEntry) size() int32 {
	return sizeofString(t.ConfigName) +
		sizeofString(t.ConfigValue) +
		sizeofBool(t.ReadOnly) +
		sizeofBool(t.IsDefault) +
		sizeofBool(t.IsSensitive)
}

func (t describeConfigsResponseV0ConfigEntry) writeTo(wb *writeBuffer) {
	wb.writeString(t.ConfigName)
	wb.writeString(t.ConfigValue)
	wb.writeBool(t.ReadOnly)
	wb.writeBool(t.IsDefault)
	wb.writeBool(t.IsSensitive)
}

func (t *describeConfigsResponseV0ConfigEntry) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.ConfigName); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ConfigValue); err != nil {
		return
	}
	if remain, err = readBool(r, remain, &t.ReadOnly); err != nil {
		return
	}
	if remain, err = readBool(r, remain, &t.IsDefault); err != nil {
		return
	}
	if remain, err = readBool(r, remain, &t.IsSensitive); err != nil {
		return
	}
	return
}

type describeConfigsResponseV0Resource struct {
	ErrorCode     int16
	ErrorMessage  string
	ResourceType  int8
	ResourceName  string
	ConfigEntries []describeConfigsResponseV0ConfigEntry
}

func (t describeConfigsResponseV0Resource) size() int32 {
	return sizeofInt16(t.ErrorCode) +
		sizeofString(t.ErrorMessage) +
		sizeofInt8(t.ResourceType) +
		sizeofString(t.ResourceName) +
		sizeofArray(len(t.ConfigEntries), func(i int) int32 { return t.ConfigEntries[i].size() })
}

func (t describeConfigsResponseV0Resource) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeString(t.ErrorMessage)
	wb.writeInt8(t.ResourceType)
	wb.writeString(t.ResourceName)
	wb.writeArray(len(t.ConfigEntries), func(i int) { t.ConfigEntries[i].writeTo(wb) })
}

func (t *describeConfigsResponseV0Resource) readFrom(r *bufio.Reader, size int) (remain int, err error) {
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
	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var entry describeConfigsResponseV0ConfigEntry
		if fnRemain, fnErr = (&entry).readFrom(r, size); err != nil {
			return
		}
		t.ConfigEntries = append(t.ConfigEntries, entry)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

//describeConfigsResponseV0 descirbe configs response V0 version
type describeConfigsResponseV0 struct {
	ThrottleTimeMs int32
	Resources      []describeConfigsResponseV0Resource
}

func (t describeConfigsResponseV0) size() int32 {
	return sizeofInt32(t.ThrottleTimeMs) +
		sizeofArray(len(t.Resources), func(i int) int32 { return t.Resources[i].size() })
}

func (t describeConfigsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt32(t.ThrottleTimeMs)
	wb.writeArray(len(t.Resources), func(i int) { t.Resources[i].writeTo(wb) })
}

func (t *describeConfigsResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMs); err != nil {
		return
	}
	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var resource describeConfigsResponseV0Resource
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

func (c *Conn) describeConfigs(request describeConfigsRequestV0) (describeConfigsResponseV0, error) {
	var response describeConfigsResponseV0
	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(describeConfigs, v0, id, request)
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
	for _, tr := range response.Resources {
		if tr.ErrorCode != 0 {
			return response, Error(tr.ErrorCode)
		}
	}
	return response, nil
}

type ResourceType int8

const (
	ResourceTypeUnknown ResourceType = 0
	ResourceTypeAny     ResourceType = 1
	ResourceTypeTopic   ResourceType = 2
	ResourceTypeGroup   ResourceType = 3
	ResourceTypeCluster ResourceType = 4
	ResourceTypeBroker  ResourceType = 5
)

//DescribeConfig
type DescribeConfig struct {
	//ResourceType
	//0: Unknown
	//1: Any
	//2: Topic
	//3: Group
	//4: Cluster
	//5: Broker (new)
	ResourceType ResourceType

	ResourceName string

	ConfigNames []string
}

//DescribeConfigsResponse
type DescribeConfigsResponse struct {
	ThrottleTimeMs int32
	Resources      []DescribeConfigsResponseResource
}

//DescribeConfigsResponseResource
type DescribeConfigsResponseResource struct {
	ErrorCode     int16
	ErrorMessage  string
	ResourceType  int8
	ResourceName  string
	ConfigEntries []DescribeConfigsResponseConfigEntry
}

//DescribeConfigsResponseConfigEntry
type DescribeConfigsResponseConfigEntry struct {
	ConfigName  string
	ConfigValue string
	ReadOnly    bool
	IsDefault   bool
	IsSensitive bool
}

//DescribeConfigs gets configuration data.
//
//Detailed info please look at https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs
func (c *Conn) DescribeConfigs(configs ...DescribeConfig) (DescribeConfigsResponse, error) {
	var requestV0Resource []describeConfigsRequestV0Resource
	for _, t := range configs {
		requestV0Resource = append(
			requestV0Resource,
			describeConfigsRequestV0Resource{
				ResourceType: int8(t.ResourceType),
				ResourceName: t.ResourceName,
				ConfigNames:  t.ConfigNames,
			})
	}

	resV0, err := c.describeConfigs(describeConfigsRequestV0{
		Resources: requestV0Resource,
	})

	res := convertToPublicAPI(resV0)

	if err != nil {
		return res, err
	}

	return res, nil
}

//convertToPublicAPI converts intenal API to publical API
func convertToPublicAPI(v0 describeConfigsResponseV0) DescribeConfigsResponse {
	resources := make([]DescribeConfigsResponseResource, 0)
	for _, r := range v0.Resources {
		configEntries := make([]DescribeConfigsResponseConfigEntry, 0)
		for _, ce := range r.ConfigEntries {
			configEntries = append(configEntries, DescribeConfigsResponseConfigEntry{
				ConfigName:  ce.ConfigName,
				ConfigValue: ce.ConfigValue,
				ReadOnly:    ce.ReadOnly,
				IsDefault:   ce.IsDefault,
				IsSensitive: ce.IsSensitive,
			})
		}
		resource := DescribeConfigsResponseResource{
			ErrorCode:     r.ErrorCode,
			ErrorMessage:  r.ErrorMessage,
			ResourceType:  r.ResourceType,
			ResourceName:  r.ResourceName,
			ConfigEntries: configEntries,
		}
		resources = append(resources, resource)
	}
	res := DescribeConfigsResponse{
		ThrottleTimeMs: v0.ThrottleTimeMs,
		Resources:      resources,
	}

	return res
}
