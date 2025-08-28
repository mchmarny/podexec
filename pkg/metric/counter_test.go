package metric

import (
	"testing"
)

func TestCounter_Inc_Table(t *testing.T) {
	tests := []struct {
		name       string
		help       string
		labelName  string
		labelValue string
	}{
		{"success", "success", "status", "success"},
		{"failure", "failure", "status", "failure"},
		{"pending", "pending", "status", "pending"},
	}
	for _, tt := range tests {
		c := NewCounter(tt.name, tt.help, tt.labelName)
		t.Run(tt.name, func(t *testing.T) {
			if c == nil {
				t.Fatalf("NewCounter(%q, %q, %q) returned nil", tt.name, tt.help, tt.labelName)
			}
			c.Increment(tt.labelValue)
		})
	}
}
