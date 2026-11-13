package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesStaticAssets(t *testing.T) {
	h := Handler()

	for _, tc := range []struct {
		path    string
		wantSub string
	}{
		{"/", "<title>ARCADE"},
		{"/style.css", "--bg-felt"},
		{"/app.js", "state_broadcast"},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", tc.path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), tc.wantSub) {
			t.Errorf("GET %s body missing %q", tc.path, tc.wantSub)
		}
	}
}
