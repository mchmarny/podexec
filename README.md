# Kubernetes Node Role Labeler

Kubernetes controller that automatically assign node role based on a value of specific node label (e.g., `nodeGroup=gpu-worker` == `node-role.kubernetes.io/gpu-worker`).

## Why 

By default, `kubeadm` enables the NodeRestriction admission controller that restricts what labels can be self-applied by `kubelet` on node registration. The `node-role.kubernetes.io/*` label is a restricted label and thus can't be set in the cloud init script or during other node inception process.

## Features

- Watches for node add/update events in the cluster.
- Checks if the node has the specified label value (configurable via launch parameter).
- If the node is missing the corresponding Kubernetes role label, it patches the node to add it.
- Uses exponential backoff for patch operations to handle transient API errors.
- Emits Prometheus metrics for successful and failed node patch operations.
- Gracefully handles shutdown signals (SIGINT/SIGTERM).

## Requirements

- Runs inside a Kubernetes cluster
- Requires RBAC permissions to patch node resources

## Usage

Update [patch-configmap.yaml](deployment/overlays/prod/patch-configmap.yaml) to define the node label you want to use as source for the node role. Fro example: 

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: node-role-controller-config
  namespace: node-labeler
data:
  roleLabel: "nodeGroup" # value of this label will be the node role 
  roleReplace: "false"   # whether to replace the existing node role if one exists
```

Then apply to the cluster: 

```sh
kubectl apply -k deployment/overlays/prod
```

This will ensure all nodes with `nodeGroup=customer-gpu` are labeled with `node-role.kubernetes.io/customer-gpu`.

> If you change ConfigMap value after the deployment remember to restart the deployment: `kubectl -n node-labeler rollout restart deployment node-role-controller`

## Metrics

The `node-role-controller` emits following metrics: 

- `node_role_patch_success_total`: Number of successful node patch operations.
- `node_role_patch_failure_total`: Number of failed node patch operations.

## Validation (optional)

The image produced by this repo comes with SLSA attestation which verifies that node role setter image was built in this repo. You can either verify that manually via [Sigstore](https://docs.sigstore.dev/about/overview/)  CLI or in the cluster suing [Sigstore](https://docs.sigstore.dev/about/overview/) policy controller.

### Manual 

> Update the image digest to the version you end up using.

```shell
export IMAGE=ghcr.io/mchmarny/node-role-controller:v0.5.0@sha256:345638126a65cff794a59c620badcd02cdbc100d45f7745b4b42e32a803ff645

cosign verify-attestation \
    --output json \
    --type slsaprovenance \
    --certificate-identity-regexp 'https://github.com/.*/.*/.github/workflows/.*' \
    --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
    $IMAGE 
```

### In Cluster

To to ensure the image used in the node role setter was built in this repo, you can enroll that one namespace (default: `node-labeler`):

```shell
kubectl label namespace node-labeler policy.sigstore.dev/include=true
```

And then add ClusterImagePolicy:

```shell
kubectl apply -f policy/clusterimagepolicy.yaml
```

And then test admission policy: 

```shell
 kubectl -n node-labeler run test --image=$IMAGE
```

If you don't already have [Sigstore](https://docs.sigstore.dev/about/overview/) policy controller, you add it into your cluster:

```shell
kubectl create namespace cosign-system
helm repo add sigstore https://sigstore.github.io/helm-charts
helm repo update
helm install policy-controller -n cosign-system sigstore/policy-controller
```

## Demo 

> Requires [Kind](https://kind.sigs.k8s.io/)

To demo the functionality of this controller, first create a simple Kind configuration file (e.g. `kind.yaml`) to ensure multiple nodes

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4

nodes:
  - role: control-plane
    labels:
      nodeGroup: system
  - role: worker
    labels:
      nodeGroup: worker
  - role: worker
    labels:
      nodeGroup: worker
```

Next launch a Kind cluster using that config:

```shell
kind create cluster --config kind.yaml
```

Set your cluster context: 

```shell
kubectl cluster-info --context kind
```

Node, when you run `kubectl get nodes` you should see `3` nodes:

```shell
NAME                                 STATUS   ROLES           AGE    VERSION
node-role-controller-control-plane   Ready    control-plane   2m9s   v1.33.1
node-role-controller-worker          Ready    <none>          114s   v1.33.1
node-role-controller-worker2         Ready    <none>          114s   v1.33.1
```

Next, deploy the `node-role-controller`:

```shell
kubectl apply -k deployment/overlays/prod
```

When you run the same list nodes command, you will see the roles of the nodes updated based on the value of the `nodeGroup` label: 

```shell
NAME                                 STATUS   ROLES                  AGE     VERSION
node-role-controller-control-plane   Ready    control-plane,system   3m12s   v1.33.1
node-role-controller-worker          Ready    worker                 2m57s   v1.33.1
node-role-controller-worker2         Ready    worker                 2m57s   v1.33.1
```

Any new node that joins the cluster will automatically have its role set on a value of that label. 

You can also `kubectl edit node node-role-controller-worker` and change the value of the `nodeGroup` label to see new role being assigned to that node. 

> Note: technically, node can have multiple roles so the `kind-node-role-controller` just adds new one. 

## Disclaimer

This is my personal project and it does not represent my employer. While I do my best to ensure that everything works, I take no responsibility for issues caused by this code.