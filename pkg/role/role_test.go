package role

import (
	"context"
	"errors"
	"testing"

	"github.com/mchmarny/rolesetter/pkg/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func getTestNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

func TestEnsureRole_PatchVariants(t *testing.T) {
	logger := log.GetTestLogger()
	val := "worker"

	tests := []struct {
		name      string
		node      *corev1.Node
		replace   bool
		patchErr  error
		wantPatch bool
	}{
		{
			name:      "patch success, no replace",
			node:      getTestNode("n5", map[string]string{"test-label": val}),
			replace:   false,
			patchErr:  nil,
			wantPatch: true,
		},
		{
			name:      "patch failure, no replace",
			node:      getTestNode("n6", map[string]string{"test-label": val}),
			replace:   false,
			patchErr:  errors.New("patch failed"),
			wantPatch: true,
		},
		{
			name:      "patch success, with replace",
			node:      getTestNode("n7", map[string]string{"test-label": val, rolePrefix + "other": ""}),
			replace:   true,
			patchErr:  nil,
			wantPatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			var gotName string
			var gotLabels []byte
			patcher := func(_ context.Context, name string, _ types.PatchType, data []byte, _ metav1.PatchOptions, _ ...string) (*corev1.Node, error) {
				called = true
				gotName = name
				gotLabels = data
				return tt.node, tt.patchErr
			}
			h := &CacheResourceHandler{
				Patcher:   patcher,
				Logger:    logger,
				RoleLabel: "test-label",
				Replace:   tt.replace,
			}
			h.EnsureRole(tt.node)
			if tt.wantPatch && !called {
				t.Error("patcher was not called when expected")
			}
			if called && gotName != tt.node.Name {
				t.Errorf("patcher called with wrong node name: got %s, want %s", gotName, tt.node.Name)
			}
			if called && len(gotLabels) == 0 {
				t.Error("patcher called with empty patch data")
			}
		})
	}
}

func TestMakePatchMetadata(t *testing.T) {
	tests := []struct {
		name    string
		input   map[string]bool
		want    []string
		replace bool
	}{
		{
			name:  "single without replace",
			input: map[string]bool{"foo": true},
			want:  []string{`{"metadata":{"labels":{"foo":""}}}`},
		},
		{
			name:  "multiple without replace",
			input: map[string]bool{"foo": true, "bar": true},
			want: []string{
				`{"metadata":{"labels":{"foo":"","bar":""}}}`,
				`{"metadata":{"labels":{"bar":"","foo":""}}}`,
			},
		},
		{
			name:  "multiple with replace",
			input: map[string]bool{"foo": true, "bar": false},
			want: []string{
				`{"metadata":{"labels":{"foo":"","bar":null}}}`,
				`{"metadata":{"labels":{"bar":null,"foo":""}}}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(makePatchMetadata(tt.input))
			for _, want := range tt.want {
				if got == want {
					return // Found a match, no need to check further
				}
			}
			t.Errorf("makePatchMetadata() = %s, want one of %v", got, tt.want)
		})
	}
}
