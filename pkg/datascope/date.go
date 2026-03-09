package datascope

import (
	"bytes"
	"time"
)

type Date time.Time

func (d Date) MarshalJSON() ([]byte, error) {
	if time.Time(d).IsZero() {
		return []byte("null"), nil
	} else {
		return []byte(time.Time(d).Format(`"2006-01-02"`)), nil
	}
}

func (d *Date) UnmarshalJSON(buf []byte) error {
	if bytes.Equal(buf, []byte("null")) {
		*d = Date(time.Time{})
		return nil
	} else {
		t, err := time.Parse(time.DateOnly, string(buf))
		if err != nil {
			return err
		}
		*d = Date(t)
		return nil
	}
}
