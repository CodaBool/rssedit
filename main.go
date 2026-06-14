package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultPort          = "9933"
	defaultPollSeconds   = 3600
	redditTokenURL       = "https://www.reddit.com/api/v1/access_token"
	redditOAuthBase      = "https://oauth.reddit.com"
	defaultFetchLimit    = 100
	defaultCacheCapacity = 256
)

var imageExtRE = regexp.MustCompile(`(?i)\.(jpg|jpeg|png|webp|gif)(\?|$)`)

type app struct {
	clientID     string
	clientSecret string
	pollInterval time.Duration
	userAgent    string
	httpClient   *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
	cache       map[string]cacheEntry
}

type cacheEntry struct {
	xml       []byte
	fetchedAt time.Time
}

type redditTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

type listingResponse struct {
	Kind string `json:"kind"`
	Data struct {
		After    string        `json:"after"`
		Children []listingItem `json:"children"`
	} `json:"data"`
}

type listingItem struct {
	Kind string     `json:"kind"`
	Data redditPost `json:"data"`
}

type redditPost struct {
	ID                  string                   `json:"id"`
	Name                string                   `json:"name"`
	Title               string                   `json:"title"`
	Author              string                   `json:"author"`
	Subreddit           string                   `json:"subreddit"`
	Permalink           string                   `json:"permalink"`
	URL                 string                   `json:"url"`
	URLOverriddenByDest string                   `json:"url_overridden_by_dest"`
	Domain              string                   `json:"domain"`
	PostHint            string                   `json:"post_hint"`
	IsSelf              bool                     `json:"is_self"`
	IsVideo             bool                     `json:"is_video"`
	CreatedUTC          float64                  `json:"created_utc"`
	GalleryData         *galleryData             `json:"gallery_data"`
	MediaMetadata       map[string]mediaMetadata `json:"media_metadata"`
	Preview             *previewData             `json:"preview"`
}

type galleryData struct {
	Items []galleryItem `json:"items"`
}

type galleryItem struct {
	MediaID   string `json:"media_id"`
	IsDeleted bool   `json:"is_deleted"`
}

type mediaMetadata struct {
	Status string `json:"status"`
	E      string `json:"e"`
	M      string `json:"m"`
	S      struct {
		URL string `json:"u"`
		X   int    `json:"x"`
		Y   int    `json:"y"`
	} `json:"s"`
}

type previewData struct {
	Images []struct {
		Source struct {
			URL    string `json:"url"`
			Width  int    `json:"width"`
			Height int    `json:"height"`
		} `json:"source"`
	} `json:"images"`
}

type feedItem struct {
	Title     string
	ImageURL  string
	Link      string
	GUID      string
	CreatedAt time.Time
}

func main() {
	clientID := strings.TrimSpace(os.Getenv("REDDIT_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("REDDIT_CLIENT_SECRET"))

	if clientID == "" {
		log.Fatal("missing REDDIT_CLIENT_ID")
	}
	if clientSecret == "" {
		log.Fatal("missing REDDIT_CLIENT_SECRET")
	}

	pollSeconds := envInt("POLL_INTERVAL_IN_SECONDS", defaultPollSeconds)
	if pollSeconds < 60 {
		log.Printf("POLL_INTERVAL_IN_SECONDS=%d is pretty aggressive; forcing minimum 60 seconds", pollSeconds)
		pollSeconds = 60
	}

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = defaultPort
	}

	a := &app{
		clientID:     clientID,
		clientSecret: clientSecret,
		pollInterval: time.Duration(pollSeconds) * time.Second,
		userAgent:    "linux:rssedit:v0.1 by /u/codabool",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache: make(map[string]cacheEntry, defaultCacheCapacity),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleFeed)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	addr := ":" + port
	log.Printf("rssedit listening on %s", addr)
	log.Printf("cache/poll interval: %s", a.pollInterval)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (a *app) handleFeed(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	user := strings.TrimSpace(r.URL.Query().Get("user"))
	if user == "" {
		http.Error(w, "missing ?user=reddit_username", http.StatusBadRequest)
		return
	}

	if !validRedditUser(user) {
		http.Error(w, "invalid reddit username", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	feedXML, err := a.getFeedXML(ctx, user)
	if err != nil {
		log.Printf("feed error for user=%s: %v", user, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/rss+xml; charset=utf-8")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(a.pollInterval.Seconds())))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(feedXML)
}

func (a *app) getFeedXML(ctx context.Context, user string) ([]byte, error) {
	now := time.Now()

	a.mu.Lock()
	if entry, ok := a.cache[user]; ok && now.Sub(entry.fetchedAt) < a.pollInterval {
		out := entry.xml
		a.mu.Unlock()
		return out, nil
	}
	a.mu.Unlock()

	items, err := a.fetchImageItems(ctx, user)
	if err != nil {
		return nil, err
	}

	feedXML, err := buildRSS(user, items)
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.cache[user] = cacheEntry{
		xml:       feedXML,
		fetchedAt: now,
	}
	a.mu.Unlock()

	return feedXML, nil
}

func (a *app) fetchImageItems(ctx context.Context, user string) ([]feedItem, error) {
	token, err := a.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf(
		"%s/user/%s/submitted?limit=%d&raw_json=1",
		redditOAuthBase,
		url.PathEscape(user),
		defaultFetchLimit,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", a.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("reddit returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var listing listingResponse
	if err := json.Unmarshal(body, &listing); err != nil {
		return nil, err
	}

	items := make([]feedItem, 0, len(listing.Data.Children))
	seenTitles := map[string]bool{}

	for _, child := range listing.Data.Children {
		p := child.Data

		if p.Title == "" {
			continue
		}

		titleKey := normalizeTitle(p.Title)
		if seenTitles[titleKey] {
			continue
		}

		img := bestImageURL(p)
		if img == "" {
			continue
		}

		seenTitles[titleKey] = true

		link := p.URL
		if p.Permalink != "" {
			link = "https://www.reddit.com" + p.Permalink
		}

		guid := p.Name
		if guid == "" {
			guid = p.ID
		}
		if guid == "" {
			guid = link
		}

		items = append(items, feedItem{
			Title:     p.Title,
			ImageURL:  img,
			Link:      link,
			GUID:      guid,
			CreatedAt: unixUTC(p.CreatedUTC),
		})
	}

	return items, nil
}

func (a *app) getAccessToken(ctx context.Context) (string, error) {
	now := time.Now()

	a.mu.Lock()
	if a.token != "" && now.Before(a.tokenExpiry.Add(-60*time.Second)) {
		token := a.token
		a.mu.Unlock()
		return token, nil
	}
	a.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, redditTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}

	req.SetBasicAuth(a.clientID, a.clientSecret)
	req.Header.Set("User-Agent", a.userAgent)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("reddit token request returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var tr redditTokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", err
	}

	if tr.AccessToken == "" {
		return "", errors.New("reddit token response did not include access_token")
	}

	expiresIn := tr.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}

	a.mu.Lock()
	a.token = tr.AccessToken
	a.tokenExpiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
	a.mu.Unlock()

	return tr.AccessToken, nil
}

func bestImageURL(p redditPost) string {
	// Reject obvious non-image/self/video posts.
	if p.IsSelf || p.IsVideo {
		return ""
	}

	// Reddit gallery: use the first valid image in gallery order.
	if p.GalleryData != nil && len(p.GalleryData.Items) > 0 && len(p.MediaMetadata) > 0 {
		for _, item := range p.GalleryData.Items {
			if item.IsDeleted || item.MediaID == "" {
				continue
			}

			meta, ok := p.MediaMetadata[item.MediaID]
			if !ok {
				continue
			}

			if strings.ToLower(meta.Status) != "valid" {
				continue
			}

			if !strings.HasPrefix(strings.ToLower(meta.M), "image/") && !strings.EqualFold(meta.E, "Image") {
				continue
			}

			if meta.S.URL != "" {
				return cleanURL(meta.S.URL)
			}
		}
	}

	// Direct i.redd.it image posts are the best case.
	direct := p.URLOverriddenByDest
	if direct == "" {
		direct = p.URL
	}

	if isImageLikeURL(direct) {
		return cleanURL(direct)
	}

	// Reddit sometimes gives full-size-ish preview source URLs.
	if p.PostHint == "image" && p.Preview != nil && len(p.Preview.Images) > 0 {
		src := p.Preview.Images[0].Source.URL
		if src != "" {
			return cleanURL(src)
		}
	}

	return ""
}

func isImageLikeURL(raw string) bool {
	if raw == "" {
		return false
	}

	u, err := url.Parse(html.UnescapeString(raw))
	if err != nil {
		return false
	}

	host := strings.ToLower(u.Hostname())
	if host == "i.redd.it" || host == "preview.redd.it" {
		return true
	}

	return imageExtRE.MatchString(u.Path)
}

func cleanURL(raw string) string {
	return html.UnescapeString(strings.TrimSpace(raw))
}

func buildRSS(user string, items []feedItem) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString(xml.Header)
	buf.WriteString(`<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">`)
	buf.WriteString("<channel>")

	writeElem(&buf, "title", "reddit images - u/"+user)
	writeElem(&buf, "link", "https://www.reddit.com/user/"+user+"/submitted/")
	writeElem(&buf, "description", "Image posts from u/"+user)
	writeElem(&buf, "ttl", "60")
	writeElem(&buf, "lastBuildDate", time.Now().UTC().Format(time.RFC1123Z))

	for _, item := range items {
		buf.WriteString("<item>")
		writeElem(&buf, "title", item.Title)
		writeElem(&buf, "link", item.Link)
		writeElem(&buf, "guid", item.GUID)
		writeElem(&buf, "pubDate", item.CreatedAt.UTC().Format(time.RFC1123Z))

		// Keep content minimal: just an image.
		content := fmt.Sprintf(`<img src="%s" alt="%s">`, xmlEscape(item.ImageURL), xmlEscape(item.Title))
		writeElem(&buf, "description", content)

		buf.WriteString("<content:encoded><![CDATA[")
		buf.WriteString(content)
		buf.WriteString("]]></content:encoded>")

		buf.WriteString("</item>")
	}

	buf.WriteString("</channel></rss>")

	return buf.Bytes(), nil
}

func writeElem(buf *bytes.Buffer, name string, value string) {
	buf.WriteByte('<')
	buf.WriteString(name)
	buf.WriteByte('>')
	buf.WriteString(xmlEscape(value))
	buf.WriteString("</")
	buf.WriteString(name)
	buf.WriteByte('>')
}

func xmlEscape(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

func normalizeTitle(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(s))), " ")
}

func unixUTC(v float64) time.Time {
	if v <= 0 {
		return time.Now().UTC()
	}
	return time.Unix(int64(v), 0).UTC()
}

func validRedditUser(user string) bool {
	if len(user) < 3 || len(user) > 20 {
		return false
	}

	for _, r := range user {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' || r == '-' {
			continue
		}
		return false
	}

	return true
}

func envInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid %s=%q, using %d", name, raw, fallback)
		return fallback
	}

	return n
}
