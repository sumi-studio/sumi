package filesvc

// capture_s3.go — the production-relevant object backend: a narrow,
// dependency-free S3/MinIO GET client with AWS Signature V4 header
// auth. The service's private configuration supplies endpoint, region,
// credentials and the EXPECTED bucket/prefix identity; the captured
// volume format must name that same identity or the capture is
// refused, so an independently configured volume cannot silently point
// reads at another bucket or volume.
//
// Only read primitives exist here — capture never writes objects.
// Credential values are never placed in errors, manifests or logs.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// S3Config is the private object-backend provisioning. The caller never
// supplies any of it — it comes only from service environment.
type S3Config struct {
	Endpoint  string   // scheme://host[:port], authority only — path-style requests
	Aliases   []string // optional trusted endpoint spellings naming the same server
	Region    string   // SigV4 credential scope; MinIO ignores the value
	AccessKey string
	SecretKey string
	Bucket    string // expected bucket the captured format must name
	Prefix    string // expected object prefix inside the bucket ("name/")
	client    *http.Client
}

// s3ObjStore binds one validated endpoint+bucket+prefix to the private
// client. The prefix is the JuiceFS object root: format.Name + "/"
// (juicefs wraps every object storage in WithPrefix(format.Name+"/"),
// so the volume name — not any caller input — is the key prefix).
type s3ObjStore struct {
	cfg      *S3Config
	endpoint string // canonical request base: scheme://host[:port]
	bucket   string
	prefix   string
	client   *http.Client
}

// s3Naming is the identity a volume format's Bucket URI names: the
// canonical endpoint authority plus the bucket label. Two URIs name
// the same source only when BOTH match the configured identity.
type s3Naming struct {
	endpoint string // canonical scheme://host[:port]
	bucket   string
}

// canonicalAuthority renders scheme://host[:port] in one comparable
// spelling: lowercased, explicit port (scheme default filled in),
// IPv6 hosts bracketed via JoinHostPort.
func canonicalAuthority(u *url.URL) string {
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return scheme + "://" + net.JoinHostPort(host, port)
}

// parseConfigEndpoint validates a trusted endpoint from private
// provisioning and returns its canonical authority. The endpoint is
// authority-only: userinfo, path, query and fragment are refused so a
// provisioning mistake cannot smuggle credentials or retarget the
// object root. A missing scheme defaults to http, matching juicefs
// newMinio's own convention.
func parseConfigEndpoint(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" {
		return "", fmt.Errorf("%w: malformed object endpoint", ErrCaptureUnconfigured)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("%w: object endpoint scheme %q unsupported",
			ErrCaptureUnconfigured, u.Scheme)
	}
	if u.Host == "" || u.Hostname() == "" {
		return "", fmt.Errorf("%w: object endpoint has no host", ErrCaptureUnconfigured)
	}
	if u.User != nil {
		return "", fmt.Errorf("%w: object endpoint must not carry userinfo",
			ErrCaptureUnconfigured)
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("%w: object endpoint must be authority-only",
			ErrCaptureUnconfigured)
	}
	if u.RawQuery != "" || u.Fragment != "" || u.RawFragment != "" {
		return "", fmt.Errorf("%w: object endpoint must not carry query/fragment",
			ErrCaptureUnconfigured)
	}
	return canonicalAuthority(u), nil
}

// trustedEndpoints returns the canonical authority set the captured
// format's endpoint may name: the configured endpoint plus any
// explicitly provisioned aliases.
func trustedEndpoints(cfg *S3Config) (map[string]bool, error) {
	ep, err := parseConfigEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{ep: true}
	for _, a := range cfg.Aliases {
		ep, err := parseConfigEndpoint(a)
		if err != nil {
			return nil, err
		}
		set[ep] = true
	}
	return set, nil
}

// parseS3Naming splits a captured JuiceFS s3-family Bucket URI into the
// endpoint authority and bucket it names, mirroring juicefs
// pkg/object semantics exactly:
//   - minio (newMinio): bucket is uri.Path[1:] (with the "minio/"
//     compatibility strip); endpoint is uri.Scheme+"://"+uri.Host. A
//     scheme-less URI defaults to http; a path shorter than one
//     segment is an error upstream — refused here too.
//   - s3 (newS3): [endpoint]/[bucket] path-style takes the FIRST path
//     segment; [bucket].[endpoint] virtual-host splits on the first
//     dot. A single-label host is not a usable source — refused.
//
// Userinfo and fragments are refused: the captured URI must describe a
// location, never carry credentials.
func parseS3Naming(storage, bucketURI string) (s3Naming, error) {
	raw := strings.TrimSpace(bucketURI)
	if !strings.Contains(raw, "://") {
		// juicefs default-scheme rules: minio always http; s3 uses
		// http for multi-dot non-amazonaws hosts, https otherwise.
		scheme := "http"
		if storage == "s3" && (len(strings.Split(raw, ".")) <= 1 ||
			strings.HasSuffix(raw, ".amazonaws.com")) {
			scheme = "https"
		}
		raw = scheme + "://" + raw
	}
	u, err := url.Parse(strings.TrimRight(raw, "/"))
	if err != nil || u.Opaque != "" || u.Host == "" {
		return s3Naming{}, fmt.Errorf("%w: malformed volume bucket uri",
			ErrCaptureRefused)
	}
	if u.User != nil {
		return s3Naming{}, fmt.Errorf("%w: volume bucket uri carries userinfo",
			ErrCaptureRefused)
	}
	if u.Fragment != "" || u.RawFragment != "" {
		return s3Naming{}, fmt.Errorf("%w: volume bucket uri carries a fragment",
			ErrCaptureRefused)
	}
	n := s3Naming{endpoint: canonicalAuthority(u)}
	path := strings.TrimSuffix(u.Path, "/")
	switch storage {
	case "minio":
		if len(u.Path) < 2 {
			return s3Naming{}, fmt.Errorf("%w: volume bucket uri names no bucket",
				ErrCaptureRefused)
		}
		n.bucket = u.Path[1:]
		if strings.Contains(n.bucket, "/") && strings.HasPrefix(n.bucket, "minio/") {
			n.bucket = n.bucket[len("minio/"):]
		}
	default: // s3-compatible
		if path != "" {
			n.bucket = strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)[0]
			break
		}
		host := u.Hostname()
		if i := strings.Index(host, "."); i > 0 {
			n.bucket, n.endpoint = host[:i], canonicalAuthority(&url.URL{
				Scheme: u.Scheme, Host: host[i+1:] + portSuffix(u)})
			break
		}
		return s3Naming{}, fmt.Errorf("%w: cannot determine bucket from volume bucket uri",
			ErrCaptureRefused)
	}
	if n.bucket == "" {
		return s3Naming{}, fmt.Errorf("%w: volume bucket uri names no bucket",
			ErrCaptureRefused)
	}
	return n, nil
}

// portSuffix preserves an explicit port when re-assembling a
// virtual-host endpoint authority.
func portSuffix(u *url.URL) string {
	if p := u.Port(); p != "" {
		return ":" + p
	}
	return ""
}

// noFollowClient returns a client that never follows redirects: a 3xx
// surfaces as a bounded failure instead of silently retargeting the
// object source. The base client is copied, never mutated.
func noFollowClient(base *http.Client) *http.Client {
	c := http.Client{}
	if base != nil {
		c = *base
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &c
}

func (s *s3ObjStore) httpClient() *http.Client {
	if s.client != nil {
		return s.client
	}
	return noFollowClient(s.cfg.client)
}

// open streams the object body plus its exact stored length. A missing
// object is errObjMissing (mapped to ErrCapturePending upstream); any
// other failure is an opaque error — never credentials or endpoint
// internals in the message.
func (s *s3ObjStore) open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return nil, 0, fmt.Errorf("%w: bad object key", ErrCaptureRefused)
	}
	base := s.endpoint
	if base == "" { // 直接构造的 store（测试/签名辅助）：同样走规范校验
		ep, err := parseConfigEndpoint(s.cfg.Endpoint)
		if err != nil {
			return nil, 0, err
		}
		base = ep
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/"+s3Escape(s.bucket)+"/"+s3EscapePath(s.prefix+key), nil)
	if err != nil {
		return nil, 0, err
	}
	s.sign(req)
	resp, err := s.httpClient().Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, 0, errObjMissing
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("object get: status %d", resp.StatusCode)
	}
	n, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil || n < 0 {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("object get: bad content-length")
	}
	return resp.Body, n, nil
}

// sign applies AWS Signature V4 header auth for an empty-payload
// request (GET/DELETE).
func (s *s3ObjStore) sign(req *http.Request) {
	s.signPayload(req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
}

// signPayload signs req with the given hex sha256 of the request body.
// Signed headers: host, x-amz-content-sha256, x-amz-date.
func (s *s3ObjStore) signPayload(req *http.Request, payloadSHA string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadSHA)

	canonURI := req.URL.EscapedPath()
	if canonURI == "" {
		canonURI = "/"
	}
	canon := strings.Join([]string{
		req.Method,
		canonURI,
		"", // no query params
		"host:" + req.URL.Host + "\n" +
			"x-amz-content-sha256:" + payloadSHA + "\n" +
			"x-amz-date:" + amzDate + "\n",
		"host;x-amz-content-sha256;x-amz-date",
		payloadSHA,
	}, "\n")
	scope := date + "/" + s.cfg.Region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" +
		hex.EncodeToString(sum256([]byte(canon)))
	kDate := hmac256([]byte("AWS4"+s.cfg.SecretKey), date)
	kRegion := hmac256(kDate, s.cfg.Region)
	kService := hmac256(kRegion, "s3")
	kSigning := hmac256(kService, "aws4_request")
	sig := hex.EncodeToString(hmac256(kSigning, sts))
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+s.cfg.AccessKey+"/"+scope+
			", SignedHeaders=host;x-amz-content-sha256;x-amz-date"+
			", Signature="+sig)
}

func hmac256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sum256(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// s3Escape encodes one path segment per RFC 3986 unreserved rules —
// the encoding SigV4 canonicalization expects.
func s3Escape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// s3EscapePath escapes each '/'-separated segment, preserving slashes.
func s3EscapePath(p string) string {
	parts := strings.Split(p, "/")
	for i := range parts {
		parts[i] = s3Escape(parts[i])
	}
	return strings.Join(parts, "/")
}

// objectStoreFor validates the captured volume format's object-storage
// identity against the service's private configuration and returns the
// bound store. This is the choke point that stops an independently
// configured volume format from silently naming a different bucket or
// volume: mismatch is a capture refusal, not a retarget.
func (c *CaptureService) objectStoreFor(f captureFormat) (objectStore, error) {
	switch c.cfg.ObjKind {
	case "file":
		if f.Storage != "file" {
			return nil, fmt.Errorf("%w: volume storage %q does not match configured kind file",
				ErrCaptureRefused, f.Storage)
		}
		// Objects live under Bucket + "/" + Name + "/" — the format's
		// declared root, which must equal the configured root exactly.
		root := filepath.Join(f.Bucket, f.Name)
		if root != filepath.Clean(c.cfg.ObjRoot) {
			return nil, fmt.Errorf("%w: volume object root does not match configured root",
				ErrCaptureRefused)
		}
		return &fileObjStore{root: root}, nil
	case "s3":
		if c.cfg.S3 == nil {
			return nil, fmt.Errorf("%w: s3 object config missing", ErrCaptureUnconfigured)
		}
		if f.Storage != "s3" && f.Storage != "minio" {
			return nil, fmt.Errorf("%w: volume storage %q does not match configured kind s3",
				ErrCaptureRefused, f.Storage)
		}
		naming, err := parseS3Naming(f.Storage, f.Bucket)
		if err != nil {
			return nil, err
		}
		trusted, err := trustedEndpoints(c.cfg.S3)
		if err != nil {
			return nil, err
		}
		prefix := f.Name + "/"
		if !trusted[naming.endpoint] {
			return nil, fmt.Errorf("%w: volume object endpoint does not match configured identity",
				ErrCaptureRefused)
		}
		if naming.bucket != c.cfg.S3.Bucket || prefix != normalizeS3Prefix(c.cfg.S3.Prefix) {
			return nil, fmt.Errorf("%w: volume bucket/prefix does not match configured identity",
				ErrCaptureRefused)
		}
		base, err := parseConfigEndpoint(c.cfg.S3.Endpoint)
		if err != nil {
			return nil, err
		}
		return &s3ObjStore{cfg: c.cfg.S3, endpoint: base, bucket: naming.bucket,
			prefix: prefix, client: noFollowClient(c.cfg.S3.client)}, nil
	default:
		return nil, fmt.Errorf("%w: object store kind %q unsupported",
			ErrCaptureUnconfigured, c.cfg.ObjKind)
	}
}

func normalizeS3Prefix(p string) string {
	p = strings.TrimPrefix(p, "/")
	if p != "" && !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}
