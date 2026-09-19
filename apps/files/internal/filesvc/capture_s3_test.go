package filesvc

// capture_s3_test.go — 端点身份绑定测试（纯单元 + 本地 httptest 传输
// fixture，不打 MinIO）。覆盖 root 反例：相同 bucket/prefix 标签下
// 的异端点必须被拒；URL 拼写规范化；重定向不得静默改源。

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func capSvcForEndpoint(ep string, aliases ...string) *CaptureService {
	return &CaptureService{cfg: CaptureConfig{ObjKind: "s3", S3: &S3Config{
		Endpoint: ep, Aliases: aliases, Bucket: "same-bucket", Prefix: "same-volume/",
	}}}
}

func minioFormat(bucketURI string) captureFormat {
	return captureFormat{Storage: "minio", Name: "same-volume", Bucket: bucketURI}
}

// Root counterexample: 相同 bucket/prefix 标签下的异端点必须拒绝。
// failure-before 证据见 root-probes/RUN.log（err=nil accepted）。
func TestS3ForeignEndpointRefused(t *testing.T) {
	c := capSvcForEndpoint("http://foreign.invalid:9000")
	f := minioFormat("http://authoritative.invalid:9000/same-bucket")
	if _, err := c.objectStoreFor(f); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("foreign endpoint accepted: err=%v want refused", err)
	}
}

// 相同端点不同拼写（大小写/默认端口/scheme 省略）必须规范化为同一身份。
func TestS3EndpointCanonicalization(t *testing.T) {
	f := minioFormat("http://obj.internal:9000/same-bucket")
	for _, ep := range []string{
		"http://obj.internal:9000",
		"HTTP://OBJ.INTERNAL:9000",
		"http://obj.internal:9000/",
		"obj.internal:9000", // scheme 省略 → juicefs newMinio 默认 http
	} {
		c := capSvcForEndpoint(ep)
		st, err := c.objectStoreFor(f)
		if err != nil {
			t.Fatalf("endpoint spelling %q refused: %v", ep, err)
		}
		if st == nil {
			t.Fatalf("endpoint spelling %q: nil store", ep)
		}
	}
	// 默认端口规范化：http://h == http://h:80
	c := capSvcForEndpoint("http://obj.internal")
	if _, err := c.objectStoreFor(minioFormat("http://obj.internal:80/same-bucket")); err != nil {
		t.Fatalf("default-port canonicalization failed: %v", err)
	}
}

// 显式可信别名： provisioning 声明的别名接受；未声明的同标签异端点拒绝。
func TestS3EndpointAliases(t *testing.T) {
	f := minioFormat("http://obj.internal:9000/same-bucket")
	// 未声明别名 → 拒绝
	c := capSvcForEndpoint("http://deploy.invalid:9000")
	if _, err := c.objectStoreFor(f); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("undeclared alias accepted: %v", err)
	}
	// 显式声明 → 接受（请求仍发往配置的 Endpoint，别名仅是身份等价）
	c = capSvcForEndpoint("http://deploy.invalid:9000", "http://obj.internal:9000")
	if _, err := c.objectStoreFor(f); err != nil {
		t.Fatalf("declared alias refused: %v", err)
	}
}

// 畸形/危险 URL 形态拒绝。
func TestS3EndpointMalformed(t *testing.T) {
	f := minioFormat("http://obj.internal:9000/same-bucket")
	for _, ep := range []string{
		"http://user:secret@obj.internal:9000", // userinfo 带凭证
		"ftp://obj.internal:9000",              // 非 http/https 方案
		"http://obj.internal:9000/some/path",   // 带路径
		"http://obj.internal:9000?token=x",     // 带 query
		"http://obj.internal:9000#frag",        // 带 fragment
		":://broken",                           // 无法解析
	} {
		c := capSvcForEndpoint(ep)
		_, err := c.objectStoreFor(f)
		if err == nil {
			t.Fatalf("malformed endpoint %q accepted", ep)
		}
		if !errors.Is(err, ErrCaptureRefused) && !errors.Is(err, ErrCaptureUnconfigured) {
			t.Fatalf("endpoint %q: unexpected err %v", ep, err)
		}
	}
	// 元数据侧的脏 URI 同样拒绝
	for _, uri := range []string{
		"http://u:p@obj.internal:9000/same-bucket",
		"http://obj.internal:9000/same-bucket#x",
	} {
		c := capSvcForEndpoint("http://obj.internal:9000")
		if _, err := c.objectStoreFor(minioFormat(uri)); !errors.Is(err, ErrCaptureRefused) {
			t.Fatalf("dirty bucket uri %q: %v want refused", uri, err)
		}
	}
	// scheme 不一致 → 不同端点
	c := capSvcForEndpoint("https://obj.internal:9000")
	if _, err := c.objectStoreFor(f); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("scheme mismatch accepted: %v", err)
	}
}

// 重定向不得静默改源：302 必须成为有界失败，且重定向目标收不到请求。
func TestS3RedirectNotFollowed(t *testing.T) {
	var decoyHits int64
	decoy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&decoyHits, 1)
		w.Write([]byte("decoy-bytes"))
	}))
	defer decoy.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", decoy.URL+"/capmini/capvolmin/chunks/0/0/1_0_10")
		w.WriteHeader(http.StatusFound)
	}))
	defer origin.Close()

	store := &s3ObjStore{cfg: &S3Config{
		Endpoint: origin.URL, Bucket: "capmini", Prefix: "capvolmin/",
		AccessKey: "k", SecretKey: "s",
	}}
	rc, _, err := store.open(context.Background(), "chunks/0/0/1_0_10")
	if err == nil {
		rc.Close()
		t.Fatal("redirect silently followed — decoy bytes would be served")
	}
	if atomic.LoadInt64(&decoyHits) != 0 {
		t.Fatalf("redirect target received %d requests — silent retarget", decoyHits)
	}
}

// 同源 200 正常流（对照组）：端点一致时对象照常可读。
func TestS3DirectGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		io.WriteString(w, "hello")
	}))
	defer srv.Close()
	store := &s3ObjStore{cfg: &S3Config{
		Endpoint: srv.URL, Bucket: "b", Prefix: "p/", AccessKey: "k", SecretKey: "s",
	}}
	rc, n, err := store.open(context.Background(), "p/x")
	if err != nil {
		t.Fatalf("direct get: %v", err)
	}
	defer rc.Close()
	if n != 5 {
		t.Fatalf("content-length %d want 5", n)
	}
}
