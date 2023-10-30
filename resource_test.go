package kafka

import "testing"

func TestResourceTypeMarshal(t *testing.T) {
	for i := ResourceTypeUnknown; i <= ResourceTypeDelegationToken; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got ResourceType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to ResourceType: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestPatternTypeMarshal(t *testing.T) {
	for i := PatternTypeUnknown; i <= PatternTypePrefixed; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got PatternType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to PatternType: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}
