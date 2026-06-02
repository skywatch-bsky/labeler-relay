package admin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequireBearer(t *testing.T) {
	testToken := "super-secret-token-12345"

	// Stub handler that returns 200 OK when reached.
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("next"))
	})

	// Middleware wrapped handler.
	mw := RequireBearer(testToken, nextHandler)

	t.Run("no Authorization header returns 401", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)

		mw.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "error")
	})

	t.Run("wrong token returns 401", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")

		mw.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "error")
	})

	t.Run("correct Bearer token allows request through", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)

		mw.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Equal(t, "next", string(body))
	})

	t.Run("malformed Bearer header returns 401", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer")

		mw.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "error")
	})

	t.Run("Bearer prefix case-sensitive", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "bearer "+testToken)

		mw.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}
