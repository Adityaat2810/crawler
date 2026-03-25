package fetcher

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/example/consumer/internal/utils"
	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
)


type RobotsCache struct {
	valkey    *redis.Client
	http      *http.Client
	userAgent  string
	ttl     time.Duration
	logger *log.Logger

}

type RobotsCacheOptions struct {
	Valkey    *redis.Client
	UserAgent  string
	TTL    time.Duration
	Timeout  time.Duration  // HTTP timeout for robots.txt requests
	Logger *log.Logger
}

func NewRobotsCache(opts RobotsCacheOptions) *RobotsCache {
	if opts.TTL == 0 {
		opts.TTL = 24 * time.Hour
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	return  &RobotsCache{
		valkey: opts.Valkey,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		userAgent: opts.UserAgent,
		ttl: opts.TTL,
		logger: opts.Logger,
	}
}

func (r *RobotsCache) Policy(ctx context.Context, rawUrl string) (*robotstxt.Group, error){

	origin, err := utils.OriginOf(rawUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	key := "robots:" + origin

	cached, err := r.valkey.Get(ctx, key).Bytes()
	if err == nil {
		return r.parseGroup(cached), nil

	}

	if err != redis.Nil {
		r.logger.Printf("robots: valkey get error for %s: %v (allowing fetch)", origin, err)
		return allowAll(r.userAgent), nil
	}

	// Cache miss — fetch robots.txt
	robotsURL := origin + "/robots.txt"
	data, fetchErr := r.fetchRobots(ctx, robotsURL)
	if fetchErr != nil {
		// Transient fetch failure — return error so caller can defer the URL.
		// Do NOT cache this; we want to retry the robots fetch later.
		return nil, fmt.Errorf("robots: fetch %s: %w", robotsURL, fetchErr)
	}

	// Cache the raw bytes (empty = allow-all policy)
	if setErr := r.valkey.Set(ctx, key, data, r.ttl).Err(); setErr != nil {
		r.logger.Printf("robots: valkey set error for %s: %v (continuing)", origin, setErr)
	}

	return r.parseGroup(data), nil
}

func (r *RobotsCache) fetchRobots(ctx context.Context, robotsURL string) ([]byte, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("User-Agent", r.userAgent)
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request: %w", err)
	}

	defer resp.Body.Close()

	// 404/410: treat as allow-all (e.g. no robots.txt)
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return  []byte{}, nil
	}

	// 5xx -> transient, do not cache
	if resp.StatusCode >= 500 && resp.StatusCode < 600 {
		return nil, fmt.Errorf("server error: %d", resp.StatusCode)
	}

	// 4xx other than 404/410 → treat as allow-all (not our problem)
	if resp.StatusCode >= 400 {
		return []byte{}, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	return body, nil

}



// fetchRobots performs an HTTP GET for the given robots.txt URL and returns the raw body bytes.
func (r *RobotsCache) parseGroup(data []byte) *robotstxt.Group {
	if len(data) == 0 {
		return allowAll(r.userAgent)
	}
	parsed, err := robotstxt.FromString(string(data))

	if err != nil {
		r.logger.Printf("robots: parse error: %v (allowing all)", err)
		return allowAll(r.userAgent)
	}

	group := parsed.FindGroup(r.userAgent)
	if group == nil {
		return allowAll(r.userAgent)
	}
	return group
}


func (r *RobotsCache) Allowed(ctx context.Context, rawUrl string) (bool, error) {
	group, err := r.Policy(ctx, rawUrl)

	if err != nil {
		return false, err
	}

	return group.Test(rawUrl), nil
}

// allowAll returns a Group that permits everything for the given user agent
func allowAll(userAgent string) *robotstxt.Group {
	parsed, _ := robotstxt.FromString("")
	return parsed.FindGroup(userAgent)
}


// CrawlDelay ( return crawl delay )
func (r *RobotsCache) CrawlDelay(
	ctx context.Context, rawUrl string, defaultDelay time.Duration,
	maxDelay time.Duration,
) time.Duration {

	group, err := r.Policy(ctx, rawUrl)
	if err != nil {
		return defaultDelay
	}

	d := group.CrawlDelay
	if d <= 0 {
		return defaultDelay
	}

	delay := group.CrawlDelay
	if delay > maxDelay {
		return maxDelay
	}

	return delay

}


// InvalidateOrigin removes the cached policy for the given origin.
// Useful when robots fetch previously failed and we want to force a re-fetch.
func (r *RobotsCache) InvalidateOrigin(ctx context.Context, rawURL string) {
	origin, err := utils.OriginOf(rawURL)
	if err != nil {
		return
	}
	r.valkey.Del(ctx, "robots:"+origin)
}
