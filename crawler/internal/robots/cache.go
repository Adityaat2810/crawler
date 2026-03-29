package robots

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
)

// Cache provides robots.txt caching with Valkey backend
type Cache struct {
	valkey    *redis.Client
	http      *http.Client
	userAgent string
	ttl       time.Duration
	logger    *log.Logger
}

// CacheOptions configures the robots cache
type CacheOptions struct {
	Valkey    *redis.Client
	UserAgent string
	TTL       time.Duration
	Timeout   time.Duration
	Logger    *log.Logger
}

// NewCache creates a new robots.txt cache
func NewCache(opts CacheOptions) *Cache {
	if opts.TTL == 0 {
		opts.TTL = 24 * time.Hour
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}

	return &Cache{
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
		ttl:       opts.TTL,
		logger:    logger,
	}
}

// OriginOf extracts the origin (scheme://host) from a URL
func OriginOf(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("missing scheme or host in %q", rawURL)
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), nil
}

// Policy fetches and caches the robots.txt policy for a URL
func (c *Cache) Policy(ctx context.Context, rawURL string) (*robotstxt.Group, error) {
	origin, err := OriginOf(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	key := "robots:" + origin

	// Try cache first
	cached, err := c.valkey.Get(ctx, key).Bytes()
	if err == nil {
		return c.parseGroup(cached), nil
	}

	if err != redis.Nil {
		c.logger.Printf("robots: valkey get %s: %v (allow)", origin, err)
		return c.allowAll(), nil
	}

	// Cache miss - fetch robots.txt
	robotsURL := origin + "/robots.txt"
	data, fetchErr := c.fetchRobots(ctx, robotsURL)
	if fetchErr != nil {
		return nil, fmt.Errorf("robots fetch: %w", fetchErr)
	}

	// Cache the result
	if setErr := c.valkey.Set(ctx, key, data, c.ttl).Err(); setErr != nil {
		c.logger.Printf("robots: valkey set %s: %v", origin, setErr)
	}

	return c.parseGroup(data), nil
}

func (c *Cache) fetchRobots(ctx context.Context, robotsURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		// No robots.txt = allow all
		return []byte{}, nil
	}

	if resp.StatusCode >= 500 && resp.StatusCode < 600 {
		// Server error - don't cache, return error for retry
		return nil, fmt.Errorf("server %d", resp.StatusCode)
	}

	if resp.StatusCode >= 400 {
		// Other 4xx - treat as allow all
		return []byte{}, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return body, nil
}

func (c *Cache) parseGroup(data []byte) *robotstxt.Group {
	if len(data) == 0 {
		return c.allowAll()
	}

	parsed, err := robotstxt.FromString(string(data))
	if err != nil {
		c.logger.Printf("robots: parse: %v (allow all)", err)
		return c.allowAll()
	}

	group := parsed.FindGroup(c.userAgent)
	if group == nil {
		return c.allowAll()
	}
	return group
}

// Allowed checks if a URL is allowed by robots.txt
func (c *Cache) Allowed(ctx context.Context, rawURL string) (bool, error) {
	group, err := c.Policy(ctx, rawURL)
	if err != nil {
		return false, err
	}
	return group.Test(rawURL), nil
}

// CrawlDelay returns the crawl delay for a URL
func (c *Cache) CrawlDelay(ctx context.Context, rawURL string, defaultDelay, maxDelay time.Duration) time.Duration {
	group, err := c.Policy(ctx, rawURL)
	if err != nil {
		return defaultDelay
	}

	delay := group.CrawlDelay
	if delay <= 0 {
		return defaultDelay
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

// Invalidate removes the cached policy for a URL's origin
func (c *Cache) Invalidate(ctx context.Context, rawURL string) {
	origin, err := OriginOf(rawURL)
	if err != nil {
		return
	}
	c.valkey.Del(ctx, "robots:"+origin)
}

func (c *Cache) allowAll() *robotstxt.Group {
	parsed, _ := robotstxt.FromString("")
	return parsed.FindGroup(c.userAgent)
}
