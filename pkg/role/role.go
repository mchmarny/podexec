package role

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/mchmarny/rolesetter/pkg/metric"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
)

const (
	rolePrefix = "node-role.kubernetes.io/"
)

// NodePatcher defines the function signature for patching a Node.
type NodePatcher func(
	ctx context.Context,
	name string,
	pt types.PatchType,
	data []byte,
	opts metav1.PatchOptions,
	subresources ...string) (result *corev1.Node, err error)

// CacheResourceHandler handles Node events and ensures the correct role label is applied.
// It implements the cache.ResourceEventHandler interface.
type CacheResourceHandler struct {
	Patcher   NodePatcher
	Logger    *zap.Logger
	RoleLabel string
	Replace   bool
}

// validate checks if the CacheResourceHandler is properly configured.
func (h *CacheResourceHandler) validate() error {
	if h.Patcher == nil {
		return fmt.Errorf("patcher must not be nil")
	}
	if h.Logger == nil {
		return fmt.Errorf("logger must not be nil")
	}
	if h.RoleLabel == "" {
		return fmt.Errorf("role label must not be empty")
	}
	return nil
}

var (
	successCounter = metric.NewCounter("node_role_patch_success_total", "Total number of successful node role patches", "role")
	failureCounter = metric.NewCounter("node_role_patch_failure_total", "Total number of failed node role patches", "role")
)

// ensureRole checks if the Node has the correct role label and patches it if necessary.
func (h *CacheResourceHandler) EnsureRole(obj interface{}) {
	if err := h.validate(); err != nil {
		h.Logger.Error("validation error", zap.Error(err))
		return
	}

	n, ok := obj.(*corev1.Node)
	if !ok {
		h.Logger.Warn("object is not a Node")
		return
	}

	h.Logger.Debug("processing role for node",
		zap.String("name", n.Name),
		zap.String("label", h.RoleLabel),
	)

	// Check if the node has the expected role label
	val, ok := n.Labels[h.RoleLabel]
	if !ok {
		h.Logger.Debug("node does not have the expected label",
			zap.String("name", n.Name),
			zap.String("want", h.RoleLabel),
		)
		return
	}

	h.Logger.Debug("node has the expected label",
		zap.String("name", n.Name),
		zap.String("value", val),
	)

	// Check if the node already has the role label
	roleKey := rolePrefix + val
	if _, ok := n.Labels[roleKey]; ok {
		h.Logger.Debug("node already has the role label",
			zap.String("node", n.Name),
			zap.String("roleKey", roleKey),
		)
		return
	}

	// setup the labels to patch
	labels := map[string]bool{
		roleKey: true,
	}

	if h.Replace {
		// If replace is true, remove any existing role labels
		for k := range n.Labels {
			if strings.HasPrefix(k, rolePrefix) {
				h.Logger.Debug("node already has a role label, deleting",
					zap.String("node", n.Name),
					zap.String("roleKey", k),
				)
				labels[k] = false
			}
		}
	}

	// patch the node with the new roles
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	op := func() error {
		if _, err := h.Patcher(
			ctx, n.Name,
			types.StrategicMergePatchType,
			makePatchMetadata(labels),
			metav1.PatchOptions{},
		); err != nil {
			return fmt.Errorf("failed to patch node %s: %w", n.Name, err)
		}
		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = 15 * time.Second // Limit total retry duration
	if err := backoff.Retry(op, expBackoff); err != nil {
		failureCounter.Increment(val)
		h.Logger.Error("patch node failed after backoff",
			zap.String("node", n.Name),
			zap.String("roleKey", roleKey),
			zap.Bool("replace", h.Replace),
			zap.Error(err),
		)
		return
	}

	successCounter.Increment(val)

	h.Logger.Info("node role label patched successfully",
		zap.String("node", n.Name),
		zap.String("roleKey", roleKey),
		zap.Bool("replace", h.Replace),
	)
}

// makePatchMetadata creates a patch metadata for the given roles.
// https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-k
func makePatchMetadata(roles map[string]bool) []byte {
	patch := `{"metadata":{"labels":{`

	for k, v := range roles {
		if patch[len(patch)-1] != '{' {
			patch += ","
		}

		if v {
			// regular label with value
			// empty string indicates a regular label with no value
			patch += `"` + k + `":""`
		} else {
			// label to be removed
			// null value indicates deletion in the patch
			patch += `"` + k + `":null`
		}
	}

	patch += "}}}"
	return []byte(patch)
}
