package labelerendpoint

// pattern: Functional Core

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const SubscribeLabelsPath = "/xrpc/com.atproto.label.subscribeLabels"

// Host extracts the host and optional port, without a scheme, path, or query.
func Host(serviceEndpoint string) (string, error) {
	u, err := url.Parse(serviceEndpoint)
	if (err != nil || u.Host == "") && !strings.Contains(serviceEndpoint, "://") {
		u, err = url.Parse("//" + serviceEndpoint)
	}
	if err != nil {
		return "", fmt.Errorf("invalid labeler endpoint: %w", err)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("labeler endpoint has no host")
	}
	return u.Host, nil
}

// SubscribeURL builds a secure WebSocket URL with an optional resume cursor.
func SubscribeURL(endpoint string, cursor *int64) (string, error) {
	host, err := Host(endpoint)
	if err != nil {
		return "", err
	}
	u := url.URL{Scheme: "wss", Host: host, Path: SubscribeLabelsPath}
	if cursor != nil {
		u.RawQuery = "cursor=" + strconv.FormatInt(*cursor, 10)
	}
	return u.String(), nil
}
