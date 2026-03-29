package parser

import (
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/net/html"
)

// Extractor extracts text and links from HTML content
type Extractor struct {
	// Tags to skip when extracting text
	skipTags map[string]bool
	// Tags that are block-level (add newlines)
	blockTags map[string]bool
}

// NewExtractor creates a new HTML extractor
func NewExtractor() *Extractor {
	return &Extractor{
		skipTags: map[string]bool{
			"script":   true,
			"style":    true,
			"noscript": true,
			"iframe":   true,
			"object":   true,
			"embed":    true,
			"svg":      true,
			"math":     true,
			"template": true,
			"head":     true,
		},
		blockTags: map[string]bool{
			"p":          true,
			"div":        true,
			"br":         true,
			"hr":         true,
			"h1":         true,
			"h2":         true,
			"h3":         true,
			"h4":         true,
			"h5":         true,
			"h6":         true,
			"ul":         true,
			"ol":         true,
			"li":         true,
			"table":      true,
			"tr":         true,
			"td":         true,
			"th":         true,
			"blockquote": true,
			"pre":        true,
			"article":    true,
			"section":    true,
			"header":     true,
			"footer":     true,
			"nav":        true,
			"aside":      true,
			"main":       true,
			"form":       true,
			"fieldset":   true,
			"address":    true,
		},
	}
}

// ExtractResult holds extracted content from HTML
type ExtractResult struct {
	Title       string
	Description string
	Text        string
	Language    string
	Links       []RawLink
	Metadata    map[string]string
}

// RawLink represents a raw extracted link before normalization
type RawLink struct {
	Href string
	Text string
	Rel  string
}

// Extract parses HTML and extracts text and links
func (e *Extractor) Extract(htmlContent []byte, baseURL *url.URL) (*ExtractResult, error) {
	doc, err := html.Parse(strings.NewReader(string(htmlContent)))
	if err != nil {
		return nil, err
	}

	result := &ExtractResult{
		Metadata: make(map[string]string),
		Links:    make([]RawLink, 0),
	}

	var textBuilder strings.Builder
	e.extractNode(doc, &textBuilder, result, baseURL)

	// Clean up extracted text
	result.Text = cleanText(textBuilder.String())

	return result, nil
}

func (e *Extractor) extractNode(n *html.Node, text *strings.Builder, result *ExtractResult, baseURL *url.URL) {
	if n.Type == html.ElementNode {
		tagName := strings.ToLower(n.Data)

		// Skip certain tags entirely
		if e.skipTags[tagName] {
			return
		}

		// Add newline before block elements
		if e.blockTags[tagName] {
			text.WriteString("\n")
		}

		// Handle specific tags
		switch tagName {
		case "title":
			result.Title = getTextContent(n)
			return
		case "meta":
			e.extractMeta(n, result)
		case "a":
			link := e.extractLink(n, baseURL)
			if link.Href != "" {
				result.Links = append(result.Links, link)
			}
		case "html":
			// Extract language from html tag
			for _, attr := range n.Attr {
				if attr.Key == "lang" {
					result.Language = attr.Val
				}
			}
		}
	}

	if n.Type == html.TextNode {
		// Add text content
		content := strings.TrimSpace(n.Data)
		if content != "" {
			text.WriteString(content)
			text.WriteString(" ")
		}
	}

	// Recursively process children
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		e.extractNode(c, text, result, baseURL)
	}

	// Add newline after block elements
	if n.Type == html.ElementNode && e.blockTags[strings.ToLower(n.Data)] {
		text.WriteString("\n")
	}
}

func (e *Extractor) extractMeta(n *html.Node, result *ExtractResult) {
	var name, content string
	for _, attr := range n.Attr {
		switch strings.ToLower(attr.Key) {
		case "name", "property":
			name = strings.ToLower(attr.Val)
		case "content":
			content = attr.Val
		}
	}

	if name == "" || content == "" {
		return
	}

	switch name {
	case "description", "og:description", "twitter:description":
		if result.Description == "" {
			result.Description = content
		}
	case "og:title", "twitter:title":
		if result.Title == "" {
			result.Title = content
		}
	default:
		// Store other metadata
		result.Metadata[name] = content
	}
}

func (e *Extractor) extractLink(n *html.Node, baseURL *url.URL) RawLink {
	link := RawLink{
		Text: strings.TrimSpace(getTextContent(n)),
	}

	for _, attr := range n.Attr {
		switch strings.ToLower(attr.Key) {
		case "href":
			link.Href = attr.Val
		case "rel":
			link.Rel = attr.Val
		}
	}

	return link
}

// getTextContent recursively extracts text from a node
func getTextContent(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}

	var result strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		result.WriteString(getTextContent(c))
	}
	return result.String()
}

// cleanText normalizes extracted text
func cleanText(s string) string {
	// Normalize whitespace
	s = regexp.MustCompile(`[ \t]+`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`\n\s*\n`).ReplaceAllString(s, "\n\n")

	// Trim each line
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	s = strings.Join(lines, "\n")

	// Remove excessive newlines
	s = regexp.MustCompile(`\n{3,}`).ReplaceAllString(s, "\n\n")

	return strings.TrimSpace(s)
}

// NormalizeURL normalizes and resolves a URL against a base URL
func NormalizeURL(href string, baseURL *url.URL) (string, error) {
	if href == "" {
		return "", nil
	}

	// Skip non-http(s) schemes
	href = strings.TrimSpace(href)
	lowerHref := strings.ToLower(href)
	if strings.HasPrefix(lowerHref, "javascript:") ||
		strings.HasPrefix(lowerHref, "mailto:") ||
		strings.HasPrefix(lowerHref, "tel:") ||
		strings.HasPrefix(lowerHref, "data:") ||
		strings.HasPrefix(lowerHref, "blob:") ||
		strings.HasPrefix(lowerHref, "#") {
		return "", nil
	}

	// Parse the href
	parsed, err := url.Parse(href)
	if err != nil {
		return "", err
	}

	// Resolve against base URL
	resolved := baseURL.ResolveReference(parsed)

	// Only allow http/https
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return "", nil
	}

	// Normalize
	resolved.Fragment = ""                         // Remove fragment
	resolved.Host = strings.ToLower(resolved.Host) // Lowercase host
	resolved.Path = normalizePath(resolved.Path)   // Normalize path

	// Remove default ports
	if (resolved.Scheme == "http" && resolved.Port() == "80") ||
		(resolved.Scheme == "https" && resolved.Port() == "443") {
		resolved.Host = resolved.Hostname()
	}

	// Remove trailing slash for non-directory paths
	if resolved.Path != "/" && strings.HasSuffix(resolved.Path, "/") {
		// Keep trailing slash only if it looks like a directory
		if !strings.Contains(resolved.Path[strings.LastIndex(resolved.Path, "/")+1:], ".") {
			// No extension, probably a directory - keep slash
		} else {
			resolved.Path = strings.TrimSuffix(resolved.Path, "/")
		}
	}

	// Sort query parameters for consistency
	if resolved.RawQuery != "" {
		resolved.RawQuery = sortQueryParams(resolved.RawQuery)
	}

	return resolved.String(), nil
}

// normalizePath normalizes a URL path
func normalizePath(path string) string {
	if path == "" {
		return "/"
	}

	// Decode percent-encoded characters that don't need encoding
	// (this normalizes %2F -> /, %20 -> %20, etc.)
	decoded, err := url.PathUnescape(path)
	if err == nil {
		path = decoded
	}

	// Re-encode properly
	return (&url.URL{Path: path}).EscapedPath()
}

// sortQueryParams sorts query parameters for consistent ordering
func sortQueryParams(query string) string {
	values, err := url.ParseQuery(query)
	if err != nil {
		return query
	}
	return values.Encode()
}

// ClassifyLink determines if a link is internal, external, or a resource
func ClassifyLink(linkURL string, baseURL *url.URL) string {
	parsed, err := url.Parse(linkURL)
	if err != nil {
		return "unknown"
	}

	// Check if it's a resource (css, js, images, etc.)
	lowerPath := strings.ToLower(parsed.Path)
	resourceExts := []string{".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
		".woff", ".woff2", ".ttf", ".eot", ".pdf", ".zip", ".tar", ".gz"}
	for _, ext := range resourceExts {
		if strings.HasSuffix(lowerPath, ext) {
			return "resource"
		}
	}

	// Check if same domain
	if strings.EqualFold(parsed.Host, baseURL.Host) {
		return "internal"
	}

	return "external"
}

// IsValidCrawlURL checks if a URL should be crawled
func IsValidCrawlURL(urlStr string) bool {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return false
	}

	// Must be http or https
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}

	// Must have a host
	if parsed.Host == "" {
		return false
	}

	// Skip certain file types
	lowerPath := strings.ToLower(parsed.Path)
	skipExts := []string{".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
		".woff", ".woff2", ".ttf", ".eot", ".pdf", ".zip", ".tar", ".gz", ".mp3",
		".mp4", ".avi", ".mov", ".wmv", ".flv", ".webp", ".webm"}
	for _, ext := range skipExts {
		if strings.HasSuffix(lowerPath, ext) {
			return false
		}
	}

	// Skip URLs with too many path segments (likely deep/dynamic)
	if strings.Count(parsed.Path, "/") > 10 {
		return false
	}

	// Skip URLs with very long query strings (likely tracking/session)
	if len(parsed.RawQuery) > 500 {
		return false
	}

	return true
}

// IsPrintable checks if a rune is printable
func IsPrintable(r rune) bool {
	return unicode.IsPrint(r) || unicode.IsSpace(r)
}
