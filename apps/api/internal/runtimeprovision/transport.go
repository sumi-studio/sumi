package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
)

const maxRequestBytes = 1 << 20

type Handler struct {
	service *Service
}

func NewHandler(service *Service) (http.Handler, error) {
	if service == nil {
		return nil, errors.New("runtime provision handler requires a service")
	}
	return &Handler{service: service}, nil
}

func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "POST is required")
		return
	}
	response.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/v1/prepare":
		var input PrepareRequest
		if !decodeRequest(response, request, &input) {
			return
		}
		epoch, err := handler.service.Prepare(request.Context(), input)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, epoch)
	case "/v1/activate":
		var input ActivateRequest
		if !decodeRequest(response, request, &input) {
			return
		}
		inspection, err := handler.service.Activate(request.Context(), input)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, OperationResponse{Inspection: inspection})
	case "/v1/abort":
		var input AbortRequest
		if !decodeRequest(response, request, &input) {
			return
		}
		inspection, err := handler.service.Abort(request.Context(), input)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, OperationResponse{Inspection: inspection})
	case "/v1/inspect":
		var input InspectRequest
		if !decodeRequest(response, request, &input) {
			return
		}
		inspection, err := handler.service.Inspect(request.Context(), input)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, OperationResponse{Inspection: inspection})
	case "/v1/stop":
		var input StopRequest
		if !decodeRequest(response, request, &input) {
			return
		}
		inspection, err := handler.service.Stop(request.Context(), input)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, OperationResponse{Inspection: inspection})
	case "/v1/reconcile":
		var input ReconcileRequest
		if !decodeRequest(response, request, &input) {
			return
		}
		inspection, err := handler.service.Reconcile(request.Context(), input)
		if err != nil {
			writeServiceError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, OperationResponse{Inspection: inspection})
	default:
		writeError(response, http.StatusNotFound, "not_found", "unknown provisioner operation")
	}
}

func decodeRequest(response http.ResponseWriter, request *http.Request, destination any) bool {
	reader := http.MaxBytesReader(response, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "request body is not valid protocol JSON")
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(response, http.StatusBadRequest, "invalid_request", "request body must contain one JSON value")
		return false
	}
	return true
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeServiceError(response http.ResponseWriter, err error) {
	if errors.Is(err, ErrConflict) {
		writeError(response, http.StatusConflict, "conflict", err.Error())
		return
	}
	writeError(response, http.StatusBadRequest, "operation_failed", err.Error())
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, errorResponse{Code: code, Message: message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

type UnixListenerConfig struct {
	SocketPath           string
	SocketGID            int
	SocketMode           os.FileMode
	AllowNonRootForTests bool
}

// ListenUnix creates a non-symlink socket below a canonical, trusted parent.
// Production callers must be root; the test escape hatch is intentionally
// explicit and does not change production defaults.
func ListenUnix(config UnixListenerConfig) (net.Listener, error) {
	if !filepath.IsAbs(config.SocketPath) || filepath.Clean(config.SocketPath) != config.SocketPath {
		return nil, errors.New("provisioner socket path must be canonical and absolute")
	}
	if os.Geteuid() != 0 && !config.AllowNonRootForTests {
		return nil, errors.New("runtime provisioner must run as root")
	}
	if config.SocketMode == 0 {
		config.SocketMode = 0o660
	}
	if config.SocketMode != config.SocketMode.Perm() {
		return nil, errors.New("provisioner socket mode must contain permission bits only")
	}
	if config.SocketMode.Perm()&0o007 != 0 {
		return nil, errors.New("provisioner socket must not grant other permissions")
	}
	if config.SocketGID < 0 || uint64(config.SocketGID) > uint64(^uint32(0)-1) {
		return nil, errors.New("provisioner socket gid is outside the Linux gid domain")
	}
	parent := filepath.Dir(config.SocketPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("create provisioner socket parent: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil || canonical != parent {
		return nil, errors.New("provisioner socket parent must be canonical and contain no symlink")
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || (!config.AllowNonRootForTests && stat.Uid != 0) || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("provisioner socket parent must be root-owned and not group/other writable")
	}
	if stale, err := os.Lstat(config.SocketPath); err == nil {
		staleStat, statOK := stale.Sys().(*syscall.Stat_t)
		if stale.Mode()&os.ModeSocket == 0 || stale.Mode()&os.ModeSymlink != 0 || !statOK || (!config.AllowNonRootForTests && staleStat.Uid != 0) {
			return nil, errors.New("refusing to replace untrusted provisioner socket path")
		}
		if err := os.Remove(config.SocketPath); err != nil {
			return nil, fmt.Errorf("remove stale provisioner socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", config.SocketPath)
	if err != nil {
		return nil, err
	}
	cleanup := func(cause error) (net.Listener, error) {
		_ = listener.Close()
		_ = os.Remove(config.SocketPath)
		return nil, cause
	}
	if err := os.Chmod(config.SocketPath, config.SocketMode); err != nil {
		return cleanup(err)
	}
	uid := 0
	if config.AllowNonRootForTests {
		uid = os.Geteuid()
	}
	if err := os.Chown(config.SocketPath, uid, config.SocketGID); err != nil {
		return cleanup(err)
	}
	return listener, nil
}

type Client struct {
	http *http.Client
}

func NewUnixClient(socketPath string) (*Client, error) {
	if !filepath.IsAbs(socketPath) {
		return nil, errors.New("provisioner socket path must be absolute")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{http: &http.Client{Transport: transport}}, nil
}
