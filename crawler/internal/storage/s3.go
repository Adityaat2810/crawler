package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// S3Config holds S3/MinIO configuration
type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string
	Timeout   time.Duration
}

// S3Store provides S3-compatible object storage operations
type S3Store struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	bucket    string
	http      *http.Client
}

// NewS3Store creates a new S3-compatible storage client
func NewS3Store(cfg S3Config) (*S3Store, error) {
	endpoint := cfg.Endpoint
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		scheme := "http://"
		if cfg.UseSSL {
			scheme = "https://"
		}
		endpoint = scheme + endpoint
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &S3Store{
		endpoint:  strings.TrimRight(endpoint, "/"),
		region:    region,
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
		bucket:    cfg.Bucket,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}, nil
}

// Put uploads data to S3
func (s *S3Store) Put(ctx context.Context, key, contentType string, data []byte) error {
	return s.PutToBucket(ctx, s.bucket, key, contentType, data)
}

// PutToBucket uploads data to a specific bucket
func (s *S3Store) PutToBucket(ctx context.Context, bucket, key, contentType string, data []byte) error {
	u, err := url.Parse(s.endpoint)
	if err != nil {
		return fmt.Errorf("parse endpoint: %w", err)
	}

	canonicalURI := "/" + path.Join(bucket, key)
	fullURL := s.endpoint + canonicalURI

	payloadHash := sha256.Sum256(data)
	payloadHashHex := hex.EncodeToString(payloadHash[:])

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)

	canonicalHeaders := fmt.Sprintf(
		"host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		u.Host, payloadHashHex, amzDate,
	)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		"PUT",
		canonicalURI,
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHashHex,
	}, "\n")

	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(canonicalRequestHash[:]),
	}, "\n")

	signingKey := s.deriveSigningKey(dateStamp)
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKey, credentialScope, signedHeaders, signature,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fullURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", authHeader)
	req.Header.Set("x-amz-content-sha256", payloadHashHex)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = int64(len(data))

	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return fmt.Errorf("s3 put failed: status=%d body=%s", resp.StatusCode, string(body))
}

// Get retrieves data from S3
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.GetFromBucket(ctx, s.bucket, key)
}

// GetFromBucket retrieves data from a specific bucket
func (s *S3Store) GetFromBucket(ctx context.Context, bucket, key string) ([]byte, error) {
	u, err := url.Parse(s.endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}

	canonicalURI := "/" + path.Join(bucket, key)
	fullURL := s.endpoint + canonicalURI

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)

	// Empty payload hash for GET
	payloadHashHex := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	canonicalHeaders := fmt.Sprintf(
		"host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		u.Host, payloadHashHex, amzDate,
	)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		"GET",
		canonicalURI,
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHashHex,
	}, "\n")

	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(canonicalRequestHash[:]),
	}, "\n")

	signingKey := s.deriveSigningKey(dateStamp)
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKey, credentialScope, signedHeaders, signature,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", authHeader)
	req.Header.Set("x-amz-content-sha256", payloadHashHex)
	req.Header.Set("x-amz-date", amzDate)

	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("object not found: %s", key)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return io.ReadAll(resp.Body)
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return nil, fmt.Errorf("s3 get failed: status=%d body=%s", resp.StatusCode, string(body))
}

// Exists checks if an object exists in S3
func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	u, err := url.Parse(s.endpoint)
	if err != nil {
		return false, fmt.Errorf("parse endpoint: %w", err)
	}

	canonicalURI := "/" + path.Join(s.bucket, key)
	fullURL := s.endpoint + canonicalURI

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)

	payloadHashHex := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	canonicalHeaders := fmt.Sprintf(
		"host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		u.Host, payloadHashHex, amzDate,
	)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		"HEAD",
		canonicalURI,
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHashHex,
	}, "\n")

	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(canonicalRequestHash[:]),
	}, "\n")

	signingKey := s.deriveSigningKey(dateStamp)
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKey, credentialScope, signedHeaders, signature,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fullURL, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", authHeader)
	req.Header.Set("x-amz-content-sha256", payloadHashHex)
	req.Header.Set("x-amz-date", amzDate)

	resp, err := s.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

func (s *S3Store) deriveSigningKey(dateStamp string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, s.region)
	kService := hmacSHA256(kRegion, "s3")
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
