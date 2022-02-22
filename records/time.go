package records

import "time"

func Time(t int64) time.Time {
	return time.Unix(t/1000, (t%1000)*int64(time.Millisecond)).UTC()
}

func Timestamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}
