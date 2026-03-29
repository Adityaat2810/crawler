package fetcher

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (f *Fetcher) handleRecord(ctx context.Context, rec *kgo.Record) (bool, error) {
	var job FetchJob

	if err := json.Unmarshal(rec.Value, &job); err != nil {
		f.logger.Printf("Failed to unmarshal job: %v", err)
		return true, nil // Skip malformed job
	}

	if job.URL == "" {
		f.logger.Printf("empty url in job")
		return true, nil
	}

	if job.Attempt == 0 {
		job.Attempt = 1
	}

	// Step 1: Check robots.txt
	allowed, err := f.robots.Allowed(ctx, job.URL)
	if err != nil {
		// Robots fetch failed - defer the job (don't crawl blindly)
		return f.retry(ctx, job, fmt.Sprintf("robots fetch: %v", err))
	}
	if !allowed {
		return f.dlq(ctx, job, "robots disallow")
	}

	// Step 2: Get crawl delay and enforce politeness
	delay := f.robots.CrawlDelay(ctx, job.URL, time.Second, 10*time.Second)
	if err := f.enforcePoliteness(ctx, job.URL, delay); err != nil {
		return false, err // don't commit if politeness enforcement fails
	}


	// Step 3: Fetch the URL
	resp, body, fetchErr := f.fetch(ctx, job.URL)
	if fetchErr != nil {
		class := classifyError(fetchErr, statusCode(resp))
		switch class {
		case errorRetriable:
			return f.retry(ctx, job, fetchErr.Error())
		case errorTerminal:
			return f.dlq(ctx, job, fetchErr.Error())
		default:
			return f.retry(ctx, job, fetchErr.Error())
		}
	}


	// Step 4: Classify status code
	status := resp.StatusCode
	class := classifyError(nil, status)
	if class == errorRetriable {
		return f.retry(ctx, job, fmt.Sprintf("status %d", status))
	}
	if class == errorTerminal {
		return f.dlq(ctx, job, fmt.Sprintf("status %d", status))
	}

	// Step 5: Store to S3
	contentType := resp.Header.Get("Content-Type")
	hash := sha256.Sum256(body)
	hashHex := hex.EncodeToString(hash[:])

	urlHash := sha256.Sum256([]byte(job.URL))
	s3Key := path.Join("raw", job.JobID, hex.EncodeToString(urlHash[:]))


	storePayload := map[string]interface{}{
		"url":                 job.URL,
		"job_id":              job.JobID,
		"status_code":         status,
		"headers":             resp.Header,
		"body_b64":            base64.StdEncoding.EncodeToString(body),
		"content_type":        contentType,
		"fetch_timestamp_utc": time.Now().UTC().Format(time.RFC3339Nano),
		"content_hash_sha256": hashHex,
		"attempt":             job.Attempt,
	}

	storeBytes, _ := json.Marshal(storePayload)

	if err := f.s3.Put(ctx, s3Key, "application/json", storeBytes); err != nil {
		f.logger.Printf("s3 put failed: %v (will retry)", err)
		return false, fmt.Errorf("s3 put: %w", err)
	}


	// Step 6: Emit success event
	success := successPayload{
		JobID:             job.JobID,
		URL:               job.URL,
		S3Key:             s3Key,
		StatusCode:        status,
		ContentType:       contentType,
		FetchTimestampUTC: time.Now().UTC(),
		ContentHash:       "sha256:" + hashHex,
	}
	if err := f.emit(ctx, f.cfg.FetchSuccessTopic, success); err != nil {
		return false, fmt.Errorf("emit success: %w", err)
	}

	f.logger.Printf("fetched url=%s status=%d size=%d", job.URL, status, len(body))
	return true, nil

}


func (f *Fetcher) fetch(ctx context.Context, rawURL string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("User-Agent", f.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	req.Header.Set("Connection", "keep-alive")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBodySize))
	if err != nil {
		return resp, nil, fmt.Errorf("read body: %w", err)
	}

	return resp, body, nil
}

func (f *Fetcher) retry(ctx context.Context, job FetchJob, reason string) (bool, error) {
	if job.Attempt >= f.maxRetries {
		return f.dlq(ctx, job, "retry window exhausted: "+reason)
	}

	backoff := backoffDuration(job.Attempt)
	payload := retryPayload{
		URL:           job.URL,
		Attempt:       job.Attempt + 1,
		JobID:         job.JobID,
		NextAttemptAt: time.Now().Add(backoff).UTC(),
		Reason:        reason,
	}

	f.logger.Printf("retry url=%s attempt=%d backoff=%v reason=%s",
		job.URL, job.Attempt+1, backoff, reason)

	if err := f.emit(ctx, f.cfg.FetchRetryTopic, payload); err != nil {
		return false, err
	}
	return true, nil
}


func (f *Fetcher) dlq(ctx context.Context, job FetchJob, reason string) (bool, error) {
	payload := dlqPayload{
		JobID:   job.JobID,
		URL:     job.URL,
		Attempt: job.Attempt,
		Reason:  reason,
	}

	f.logger.Printf("dlq url=%s reason=%s", job.URL, reason)

	if err := f.emit(ctx, f.cfg.FetchDLQTopic, payload); err != nil {
		return false, err
	}
	return true, nil
}

func (f *Fetcher) emit(ctx context.Context, topic string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	rec := &kgo.Record{
		Topic: topic,
		Value: b,
	}
	return f.kafkaClient.ProduceSync(ctx, rec).FirstErr()
}


// backoffDuration calculates exponential backoff with jitter
func backoffDuration(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// Base: 1min, 2min, 4min, 8min, 16min...
	base := time.Duration(1<<uint(min(attempt-1, 8))) * time.Minute

	// Add jitter (up to 25% of base)
	jitter := time.Duration(rand.Int63n(int64(base / 4)))

	delay := base + jitter
	maxDelay := 24 * time.Hour
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}


func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseInt64(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func statusCode(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}


func (f *Fetcher) enforcePoliteness(ctx context.Context, rawURL string, delay time.Duration) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}

	host := strings.ToLower(u.Hostname())
	key := "politeness:" + host

	for {
		val, err := f.valkey.Get(ctx, key).Result()
		if err != nil && err != redis.Nil {
			f.logger.Printf("politeness get error: %v (ignoring)", err)
			break
		}
		if err == redis.Nil {
			break // no existing politeness entry
		}

		ts, convErr := parseInt64(val)
		if convErr != nil {
			f.logger.Printf("politeness parse error: %v (ignoring)", convErr)
			break
		}

		wait := time.Until(time.Unix(ts, 0))
		if wait <= 0 {
			break
		}

		f.logger.Printf("politeness: waiting %v for %s", wait, host)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}

	}

		// Set next allowed fetch time
	next := time.Now().Add(delay).Unix()
	if err := f.valkey.Set(ctx, key, fmt.Sprintf("%d", next), 24*time.Hour).Err(); err != nil {
		f.logger.Printf("politeness set error: %v", err)
	}
	return nil
}