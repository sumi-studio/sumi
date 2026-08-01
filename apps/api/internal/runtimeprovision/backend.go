package runtimeprovision

import "context"

// Backend is the machine-runtime seam. DockerBackend is one implementation;
// a Firecracker implementation can satisfy the same prepare/activate contract
// without changing callers or granting them host-runtime access.
type Backend interface {
	Prepare(context.Context, PrepareRequest) (PreparedEpoch, error)
	Activate(context.Context, ActivateRequest) error
	Abort(context.Context, PreparedEpoch) error
	Inspect(context.Context, string) (Inspection, error)
	Stop(context.Context, PreparedEpoch) error
	Reconcile(context.Context, string) (Inspection, error)
}
