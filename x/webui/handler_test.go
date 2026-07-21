package webui

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"go-slim.dev/sdq"
	"go-slim.dev/sdq/x/memory"
)

func TestHandlerReplacesDefaultBasePath(t *testing.T) {
	handler := NewHandlerWithBasePath(nil, "/sdq")
	request := httptest.NewRequest(http.MethodGet, "/topics/example", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	body := response.Body.String()
	if count := strings.Count(body, `<base href="/sdq/">`); count != 1 {
		t.Fatalf("expected one configured base tag, got %d", count)
	}
	if strings.Contains(body, `<base href="/">`) {
		t.Fatal("expected default base tag to be replaced")
	}
	if !strings.Contains(body, `type="importmap"`) {
		t.Fatal("expected browser importmap")
	}
	if !strings.Contains(body, `src="./src/main.js"`) {
		t.Fatal("expected direct ES module entrypoint")
	}
}

func TestHandlerEscapesBasePath(t *testing.T) {
	handler := NewHandlerWithBasePath(nil, `/"><script>alert(1)</script>`)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	body := response.Body.String()
	if strings.Contains(body, `<base href="/"><script>alert(1)</script>/">`) {
		t.Fatal("base path was inserted without escaping")
	}
	if !strings.Contains(body, `&#34;&gt;&lt;script&gt;alert(1)&lt;/script&gt;`) {
		t.Fatalf("expected escaped base path, got %s", body)
	}
}

func TestHandlerServesFrontendSourceModules(t *testing.T) {
	handler := NewHandler(nil)

	for _, path := range []string{
		"/style.css",
		"/theme.css",
		"/chart.css",
		"/src/main.js",
		"/src/i18n.js",
		"/src/views/dashboard.js",
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			t.Fatalf("expected %s status %d, got %d", path, http.StatusOK, response.Code)
		}
		if response.Body.Len() == 0 {
			t.Fatalf("expected %s response body", path)
		}
	}
}

func TestHandlerServesRuntimeData(t *testing.T) {
	queue := newHandlerTestQueue(t)
	textID, err := queue.Put("email", []byte(`{"message":"hello"}`), 1, 0, time.Minute)
	if err != nil {
		t.Fatalf("put text job: %v", err)
	}
	binaryBody := []byte{0xff, 0x00, 0x7f}
	binaryID, err := queue.Put("binary", binaryBody, 1, 0, time.Minute)
	if err != nil {
		t.Fatalf("put binary job: %v", err)
	}

	handler := NewHandler(NewQuery(queue))

	t.Run("metrics", func(t *testing.T) {
		response := requestHandler(t, handler, "/api/metrics")
		var snapshot Snapshot
		if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
			t.Fatalf("decode metrics: %v", err)
		}
		if snapshot.Overview == nil || snapshot.Overview.TotalJobs != 2 {
			t.Fatalf("expected two runtime jobs, got %#v", snapshot.Overview)
		}
		if len(snapshot.Topics) != 2 {
			t.Fatalf("expected two runtime topics, got %d", len(snapshot.Topics))
		}
	})

	t.Run("storage", func(t *testing.T) {
		response := requestHandler(t, handler, "/api/storage")
		var info StorageInfo
		if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
			t.Fatalf("decode storage: %v", err)
		}
		if info.Name != "memory" || info.TotalJobs != 2 || info.TotalSize == 0 {
			t.Fatalf("unexpected storage info: %#v", info)
		}
	})

	t.Run("text body", func(t *testing.T) {
		response := requestHandler(t, handler, "/api/jobs/"+strconv.FormatUint(textID, 10)+"/body")
		var body jobBodyResponse
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatalf("decode text body: %v", err)
		}
		if body.Encoding != "utf-8" || body.Body != `{"message":"hello"}` {
			t.Fatalf("unexpected text body: %#v", body)
		}
	})

	t.Run("binary body", func(t *testing.T) {
		response := requestHandler(t, handler, "/api/jobs/"+strconv.FormatUint(binaryID, 10)+"/body")
		var body jobBodyResponse
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatalf("decode binary body: %v", err)
		}
		if body.Encoding != "base64" || body.Body != base64.StdEncoding.EncodeToString(binaryBody) {
			t.Fatalf("unexpected binary body: %#v", body)
		}
	})

	t.Run("invalid state", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/api/topics/email/jobs?state=unknown", nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("expected status %d, got %d", http.StatusBadRequest, response.Code)
		}
		if cacheControl := response.Header().Get("Cache-Control"); cacheControl != "no-store" {
			t.Fatalf("expected Cache-Control no-store, got %q", cacheControl)
		}
	})
}

func newHandlerTestQueue(t *testing.T) *sdq.Queue {
	t.Helper()
	config := sdq.DefaultConfig()
	config.Storage = memory.New()
	config.Ticker = &noOpTicker{}
	queue, err := sdq.New(config)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		if err := queue.Stop(); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})
	return queue
}

func requestHandler(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected %s status %d, got %d: %s", path, http.StatusOK, response.Code, response.Body.String())
	}
	return response
}
