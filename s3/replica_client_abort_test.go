package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// abortServer answers a multipart upload whose parts never succeed: each part
// either waits for its request to be cancelled or fails outright. It records
// the AbortMultipartUpload calls it receives.
type abortServer struct {
	failParts bool
	partSeen  chan struct{}
	release   chan struct{}

	mu     sync.Mutex
	aborts []string
}

func (s *abortServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>k</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		s.mu.Lock()
		s.aborts = append(s.aborts, q.Get("uploadId"))
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		select {
		case s.partSeen <- struct{}{}:
		default:
		}
		if s.failParts {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-s.release:
		}
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (s *abortServer) abortCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.aborts...)
}

func writeMultipart(t *testing.T, srv *abortServer, ctx context.Context) error {
	t.Helper()
	srv.release = make(chan struct{})
	server := httptest.NewServer(srv)
	t.Cleanup(server.Close)
	// Cleanups run last-first: parked part handlers go before Close waits on them.
	t.Cleanup(func() { close(srv.release) })
	client := newTestReplicaClient(t, server)
	client.PartSize = 5 << 20
	if err := client.Init(context.Background()); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	data := mustLTXWithSize(t, 11<<20)
	_, err := client.WriteLTXFile(ctx, 9, 1, 2, bytes.NewReader(data))
	return err
}

// A shutdown cancels the upload's context mid-part. The uploader's own abort
// would reuse that context and never reach S3; the upload must still be
// aborted so its parts do not stay in the bucket.
func TestReplicaClient_WriteLTXFile_CancelledUploadIsAborted(t *testing.T) {
	srv := &abortServer{partSeen: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-srv.partSeen:
		case <-time.After(10 * time.Second):
		}
		cancel()
	}()

	if err := writeMultipart(t, srv, ctx); err == nil {
		t.Fatal("WriteLTXFile() succeeded on a cancelled upload")
	}
	if got := srv.abortCalls(); len(got) != 1 || got[0] != "upload-1" {
		t.Fatalf("abort calls = %v, want one for upload-1", got)
	}
}

func TestReplicaClient_WriteLTXFile_FailedUploadIsAbortedOnce(t *testing.T) {
	srv := &abortServer{failParts: true, partSeen: make(chan struct{}, 1)}
	if err := writeMultipart(t, srv, context.Background()); err == nil {
		t.Fatal("WriteLTXFile() succeeded with every part failing")
	}
	if got := srv.abortCalls(); len(got) != 1 || got[0] != "upload-1" {
		t.Fatalf("abort calls = %v, want exactly one for upload-1", got)
	}
}
