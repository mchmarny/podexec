package log

import (
	"testing"
)

func TestGetLogger_NotNil(t *testing.T) {
	l := GetLogger()
	if l == nil {
		t.Error("GetLogger returned nil")
	}
}

func TestGetDebugLogger_NotNil(t *testing.T) {
	l := GetDebugLogger()
	if l == nil {
		t.Error("GetDebugLogger returned nil")
	}
}

func TestGetTestLogger_NotNil(t *testing.T) {
	l := GetTestLogger()
	if l == nil {
		t.Error("GetTestLogger returned nil")
	}
}
