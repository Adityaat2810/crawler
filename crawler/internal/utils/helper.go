package utils

import (
	"fmt"
	"net/url"
	"strings"
)

func OriginOf(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("missing scheme or host in %q", rawURL)
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)
	return scheme + "://" + host, nil
}