package fetcher

import (
	"errors"
	"net"
	"net/url"
	"strings"
)

type errorClass int

const (
	errorNone errorClass = iota
	errorRetriable
	errorTerminal
)

// classifyError determines if an error/status is retriable, terminal, or no error
func classifyError(err error, statusCode int) errorClass {
	if err != nil {
		return classifyErrorByError(err)
	}
	return classifyErrorByStatus(statusCode)
}

func classifyErrorByError(err error) errorClass {
	// URL parsing errors are terminal
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		// Check if underlying error is retriable
		if urlErr.Timeout() {
			return errorRetriable
		}
		if urlErr.Temporary() {
			return errorRetriable
		}
	}

	// Network errors
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return errorRetriable
		}
		// Temporary() is deprecated but still useful
		if netErr.Temporary() {
			return errorRetriable
		}
	}

	// DNS errors
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsTemporary || dnsErr.IsTimeout {
			return errorRetriable
		}
		// DNS not found is terminal
		if dnsErr.IsNotFound {
			return errorTerminal
		}
	}

	// String-based classification for common error messages
	msg := strings.ToLower(err.Error())

	// Retriable patterns
	retriablePatterns := []string{
		"timeout",
		"context deadline exceeded",
		"connection reset",
		"connection refused",
		"temporarily unavailable",
		"temporary failure",
		"i/o timeout",
		"tls handshake timeout",
		"no such host", // DNS might be temporary
		"network is unreachable",
	}
	for _, pattern := range retriablePatterns {
		if strings.Contains(msg, pattern) {
			return errorRetriable
		}
	}

	// Terminal patterns
	terminalPatterns := []string{
		"too many redirects",
		"certificate",
		"x509",
		"invalid url",
		"malformed",
		"unsupported protocol",
	}
	for _, pattern := range terminalPatterns {
		if strings.Contains(msg, pattern) {
			return errorTerminal
		}
	}

	// Default to terminal for unknown errors
	return errorTerminal
}

func classifyErrorByStatus(statusCode int) errorClass {
	if statusCode == 0 {
		return errorNone
	}

	// Success
	if statusCode >= 200 && statusCode < 300 {
		return errorNone
	}

	// Retriable status codes
	switch statusCode {
	case 429: // Too Many Requests
		return errorRetriable
	case 408: // Request Timeout
		return errorRetriable
	case 502: // Bad Gateway
		return errorRetriable
	case 503: // Service Unavailable
		return errorRetriable
	case 504: // Gateway Timeout
		return errorRetriable
	case 520, 521, 522, 523, 524: // Cloudflare errors
		return errorRetriable
	}

	// All other 5xx errors are retriable
	if statusCode >= 500 && statusCode < 600 {
		return errorRetriable
	}

	// Terminal 4xx errors
	switch statusCode {
	case 400: // Bad Request
		return errorTerminal
	case 401: // Unauthorized
		return errorTerminal
	case 403: // Forbidden
		return errorTerminal
	case 404: // Not Found
		return errorTerminal
	case 405: // Method Not Allowed
		return errorTerminal
	case 410: // Gone
		return errorTerminal
	case 414: // URI Too Long
		return errorTerminal
	case 451: // Unavailable For Legal Reasons
		return errorTerminal
	}

	// Redirects that escaped our redirect handler
	if statusCode >= 300 && statusCode < 400 {
		return errorRetriable // might be a temporary redirect
	}

	// Default to terminal for other status codes
	return errorTerminal
}
