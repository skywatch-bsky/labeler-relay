// pattern: Imperative Shell

package admin

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// RequireBearer returns middleware that enforces bearer token authentication.
// It rejects requests missing or mismatching the Authorization header with 401.
// Uses constant-time comparison to prevent timing attacks.
func RequireBearer(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")

		// Check for "Bearer <token>" format.
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "missing or malformed Authorization header"})
			return
		}

		provided := parts[1]

		// Constant-time comparison: compare as byte slices.
		// ConstantTimeCompare returns 1 if equal, 0 otherwise.
		// It does NOT early-return on length mismatch — the lengths are equal,
		// so we don't leak length. But guard against differing lengths for robustness.
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid token"})
			return
		}

		// Token matches; proceed to next handler.
		next.ServeHTTP(w, r)
	})
}
