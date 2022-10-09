package yt

import (
	"fmt"
	"time"
)

type YTErrorWrapper struct {
	Message string
	Module  string
	Err     error
}

func (err *YTErrorWrapper) Error() string {
	if err.Module == "" {
		return fmt.Sprintf("[YT] %s: %v", err.Message, err.Err)
	} else {
		return fmt.Sprintf("[YT] [%s] %s: %v", err.Module, err.Message, err.Err)
	}
}

func (err *YTErrorWrapper) Unwrap() error {
	return err.Err
}

func WrapWithYTError(err error, module string, message string) error {
	return &YTErrorWrapper{
		Message: message,
		Module:  module,
		Err:     err,
	}
}

func VODIndex(vods []YTVod, id string) int {
	for i, v := range vods {
		if v.ID == id {
			return i
		}
	}

	return -1
}

func CalculateEndTime(endtime string, starttime string) (string, error) {
	if endtime == "" {
		starttimeGo, err := time.Parse("2006-01-02T15:04:05Z", starttime)
		if err != nil {
			return endtime, err
		} else {
			currentTime := time.Now().UTC()
			duration := currentTime.Sub(starttimeGo) + (time.Minute * 15)
			return starttimeGo.Add(duration).Format("2006-01-02T15:04:05Z"), nil
		}
	} else {
		return endtime, nil
	}
}
