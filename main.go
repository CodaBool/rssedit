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
	defaultPort           = "9933"
	defaultMinRateSeconds = 86400
	redditTokenURL        = "https://www.reddit.com/api/v1/access_token"
	redditOAuthBase       = "https://oauth.reddit.com"
	defaultFetchLimit     = 100
	defaultCacheCapacity  = 256
)

var imageExtRE = regexp.MustCompile(`(?i)\.(jpg|jpeg|png|webp|gif)(\?|$)`)

type app struct {
	clientID     string
	clientSecret string
	minRate      time.Duration
	userAgent    string
	httpClient   *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
	cache       map[string]cacheEntry
	inFlight    map[string]chan struct{}
}

type cacheEntry struct {
	xml       []byte
	fetchedAt time.Time
}

type feedResult struct {
	xml        []byte
	cacheState string
	fetchedAt  time.Time
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
	Media               *redditMedia             `json:"media"`
	SecureMedia         *redditMedia             `json:"secure_media"`
}

type redditMedia struct {
	RedditVideo *redditVideo `json:"reddit_video"`
}

type redditVideo struct {
	FallbackURL       string `json:"fallback_url"`
	HLSURL            string `json:"hls_url"`
	DASHURL           string `json:"dash_url"`
	ScrubberMediaURL  string `json:"scrubber_media_url"`
	Width             int    `json:"width"`
	Height            int    `json:"height"`
	Duration          int    `json:"duration"`
	IsGIF             bool   `json:"is_gif"`
	TranscodingStatus string `json:"transcoding_status"`
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

type mediaKind string

const (
	mediaKindImage mediaKind = "image"
	mediaKindVideo mediaKind = "video"
)

type mediaItem struct {
	Kind        mediaKind
	URL         string
	PosterURL   string
	FallbackURL string
	Width       int
	Height      int
}

type feedItem struct {
	Title     string
	Media     mediaItem
	Permalink string
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

	minRateSeconds := envIntWithLegacy(
		"MIN_RATE_IN_SECONDS",
		"POLL_INTERVAL_IN_SECONDS",
		defaultMinRateSeconds,
	)

	if minRateSeconds < 60 {
		log.Printf("MIN_RATE_IN_SECONDS=%d is very aggressive; forcing minimum 60 seconds", minRateSeconds)
		minRateSeconds = 60
	}

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = defaultPort
	}

	a := &app{
		clientID:     clientID,
		clientSecret: clientSecret,
		minRate:      time.Duration(minRateSeconds) * time.Second,
		userAgent:    "linux:rssedit:v0.3 by /u/codabool",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache:    make(map[string]cacheEntry, defaultCacheCapacity),
		inFlight: make(map[string]chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleFeed)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	addr := ":" + port
	log.Printf("rssedit listening on %s", addr)
	log.Printf("minimum Reddit fetch interval per user: %s", a.minRate)

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

	result, err := a.getFeedXML(r.Context(), user)
	if err != nil {
		log.Printf("feed error for user=%s: %v", user, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/rss+xml; charset=utf-8")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(a.minRate.Seconds())))
	w.Header().Set("X-Rssedit-Cache", result.cacheState)
	w.Header().Set("X-Rssedit-Fetched-At", result.fetchedAt.UTC().Format(time.RFC3339))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result.xml)
}

func (a *app) getFeedXML(ctx context.Context, user string) (feedResult, error) {
	now := time.Now()

	a.mu.Lock()
	if entry, ok := a.cache[user]; ok && now.Sub(entry.fetchedAt) < a.minRate {
		a.mu.Unlock()
		return feedResult{
			xml:        entry.xml,
			cacheState: "HIT",
			fetchedAt:  entry.fetchedAt,
		}, nil
	}

	if ch, ok := a.inFlight[user]; ok {
		a.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return feedResult{}, ctx.Err()
		}

		a.mu.Lock()
		entry, ok := a.cache[user]
		a.mu.Unlock()

		if ok {
			return feedResult{
				xml:        entry.xml,
				cacheState: "HIT-AFTER-WAIT",
				fetchedAt:  entry.fetchedAt,
			}, nil
		}

		return feedResult{}, errors.New("refresh completed but no cache entry was created")
	}

	ch := make(chan struct{})
	a.inFlight[user] = ch

	staleEntry, hasStale := a.cache[user]

	a.mu.Unlock()

	items, err := a.fetchMediaItems(ctx, user)

	a.mu.Lock()
	delete(a.inFlight, user)
	close(ch)
	a.mu.Unlock()

	if err != nil {
		if hasStale {
			log.Printf("reddit fetch failed for user=%s; serving stale cache: %v", user, err)
			return feedResult{
				xml:        staleEntry.xml,
				cacheState: "STALE-ERROR",
				fetchedAt:  staleEntry.fetchedAt,
			}, nil
		}

		return feedResult{}, err
	}

	feedXML, err := buildRSS(user, items)
	if err != nil {
		if hasStale {
			log.Printf("rss build failed for user=%s; serving stale cache: %v", user, err)
			return feedResult{
				xml:        staleEntry.xml,
				cacheState: "STALE-BUILD-ERROR",
				fetchedAt:  staleEntry.fetchedAt,
			}, nil
		}

		return feedResult{}, err
	}

	fetchedAt := time.Now()

	a.mu.Lock()
	a.cache[user] = cacheEntry{
		xml:       feedXML,
		fetchedAt: fetchedAt,
	}
	a.mu.Unlock()

	return feedResult{
		xml:        feedXML,
		cacheState: "MISS",
		fetchedAt:  fetchedAt,
	}, nil
}

func (a *app) fetchMediaItems(ctx context.Context, user string) ([]feedItem, error) {
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

		media := bestMedia(p)
		if media.URL == "" {
			continue
		}

		seenTitles[titleKey] = true

		permalink := ""
		if p.Permalink != "" {
			permalink = "https://www.reddit.com" + p.Permalink
		} else if p.URL != "" {
			permalink = p.URL
		}

		guid := permalink
		if guid == "" {
			guid = p.Name
		}
		if guid == "" {
			guid = p.ID
		}

		items = append(items, feedItem{
			Title:     p.Title,
			Media:     media,
			Permalink: permalink,
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

func bestMedia(p redditPost) mediaItem {
	// Drop text posts.
	if p.IsSelf {
		return mediaItem{}
	}

	// Reddit-hosted video.
	if video := bestRedditVideo(p); video != nil {
		poster := bestPreviewPoster(p)

		videoURL := cleanURL(video.HLSURL)
		if videoURL == "" {
			videoURL = cleanURL(video.FallbackURL)
		}

		if videoURL == "" {
			return mediaItem{}
		}

		return mediaItem{
			Kind:        mediaKindVideo,
			URL:         videoURL,
			PosterURL:   poster,
			FallbackURL: cleanURL(video.FallbackURL),
			Width:       video.Width,
			Height:      video.Height,
		}
	}

	// Reddit gallery: use first valid image.
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
				return mediaItem{
					Kind:   mediaKindImage,
					URL:    cleanURL(meta.S.URL),
					Width:  meta.S.X,
					Height: meta.S.Y,
				}
			}
		}
	}

	// Direct image / gif.
	direct := p.URLOverriddenByDest
	if direct == "" {
		direct = p.URL
	}

	if isImageLikeURL(direct) {
		return mediaItem{
			Kind: mediaKindImage,
			URL:  cleanURL(direct),
		}
	}

	// Reddit preview fallback for image posts.
	if p.PostHint == "image" && p.Preview != nil && len(p.Preview.Images) > 0 {
		src := p.Preview.Images[0].Source.URL
		if src != "" {
			return mediaItem{
				Kind:   mediaKindImage,
				URL:    cleanURL(src),
				Width:  p.Preview.Images[0].Source.Width,
				Height: p.Preview.Images[0].Source.Height,
			}
		}
	}

	return mediaItem{}
}

func bestRedditVideo(p redditPost) *redditVideo {
	if p.SecureMedia != nil && p.SecureMedia.RedditVideo != nil {
		return p.SecureMedia.RedditVideo
	}

	if p.Media != nil && p.Media.RedditVideo != nil {
		return p.Media.RedditVideo
	}

	return nil
}

func bestPreviewPoster(p redditPost) string {
	if p.Preview != nil && len(p.Preview.Images) > 0 {
		src := p.Preview.Images[0].Source.URL
		if src != "" {
			return cleanURL(src)
		}
	}

	if p.URL != "" && isImageLikeURL(p.URL) {
		return cleanURL(p.URL)
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

	writeElem(&buf, "title", "reddit media - u/"+user)
	writeElem(&buf, "link", "https://www.reddit.com/user/"+user+"/submitted/")
	writeElem(&buf, "description", "Image and video posts from u/"+user)
	writeElem(&buf, "ttl", "1440")
	writeElem(&buf, "lastBuildDate", time.Now().UTC().Format(time.RFC1123Z))

	for _, item := range items {
		buf.WriteString("<item>")

		// Like your example: link points directly to media.
		writeElem(&buf, "link", item.MediaLink())

		writeElem(&buf, "title", item.DisplayTitle())

		buf.WriteString(`<guid isPermaLink="true">`)
		buf.WriteString(xmlEscape(item.GUID))
		buf.WriteString("</guid>")

		if item.Permalink != "" {
			writeElem(&buf, "comments", item.Permalink)
		}

		buf.WriteString("<description><![CDATA[")
		buf.WriteString(item.DescriptionHTML())
		buf.WriteString("]]></description>")

		buf.WriteString("<content:encoded><![CDATA[")
		buf.WriteString(item.DescriptionHTML())
		buf.WriteString("]]></content:encoded>")

		writeElem(&buf, "pubDate", item.CreatedAt.UTC().Format(time.RFC1123Z))

		buf.WriteString("</item>")
	}

	buf.WriteString("</channel></rss>")

	return buf.Bytes(), nil
}

func (i feedItem) MediaLink() string {
	if i.Media.Kind == mediaKindVideo && i.Media.FallbackURL != "" {
		// Keep <link> on v.redd.it-ish / MP4-ish media when available.
		return i.Media.FallbackURL
	}

	return i.Media.URL
}

func (i feedItem) DisplayTitle() string {
	switch i.Media.Kind {
	case mediaKindVideo:
		return i.Title + " (Video)"
	case mediaKindImage:
		return i.Title
	default:
		return i.Title
	}
}

func (i feedItem) DescriptionHTML() string {
	var buf strings.Builder

	if i.Permalink != "" {
		buf.WriteString("<section class='reading-time-and-permalink'>")
		buf.WriteString("<p><a href='")
		buf.WriteString(htmlAttr(i.Permalink))
		buf.WriteString("'>Post permalink</a></p>")
		buf.WriteString("</section>")
	}

	switch i.Media.Kind {
	case mediaKindVideo:
		buf.WriteString("<section class='embedded-media'>")
		buf.WriteString("<video controls preload='metadata' playsinline")

		if i.Media.PosterURL != "" {
			buf.WriteString(" poster='")
			buf.WriteString(htmlAttr(i.Media.PosterURL))
			buf.WriteString("'")
		}

		buf.WriteString(">")

		if i.Media.URL != "" {
			buf.WriteString("<source src='")
			buf.WriteString(htmlAttr(i.Media.URL))
			buf.WriteString("' type='")
			if strings.Contains(strings.ToLower(i.Media.URL), ".m3u8") {
				buf.WriteString("application/x-mpegURL")
			} else {
				buf.WriteString("video/mp4")
			}
			buf.WriteString("'")

			if i.Media.Width > 0 {
				buf.WriteString(" width='")
				buf.WriteString(strconv.Itoa(i.Media.Width))
				buf.WriteString("'")
			}

			if i.Media.Height > 0 {
				buf.WriteString(" height='")
				buf.WriteString(strconv.Itoa(i.Media.Height))
				buf.WriteString("'")
			}

			buf.WriteString(">")
		}

		if i.Media.FallbackURL != "" && i.Media.FallbackURL != i.Media.URL {
			buf.WriteString("<source src='")
			buf.WriteString(htmlAttr(i.Media.FallbackURL))
			buf.WriteString("' type='video/mp4'")

			if i.Media.Width > 0 {
				buf.WriteString(" width='")
				buf.WriteString(strconv.Itoa(i.Media.Width))
				buf.WriteString("'")
			}

			if i.Media.Height > 0 {
				buf.WriteString(" height='")
				buf.WriteString(strconv.Itoa(i.Media.Height))
				buf.WriteString("'")
			}

			buf.WriteString(">")
		}

		if i.Media.PosterURL != "" {
			buf.WriteString("<img src='")
			buf.WriteString(htmlAttr(i.Media.PosterURL))
			buf.WriteString("' alt='' />")
		}

		buf.WriteString("</video>")
		buf.WriteString("</section>")

	case mediaKindImage:
		buf.WriteString("<section class='preview-image'>")
		buf.WriteString("<img src='")
		buf.WriteString(htmlAttr(i.Media.URL))
		buf.WriteString("' />")
		buf.WriteString("</section>")
	}

	return buf.String()
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

func htmlAttr(s string) string {
	return html.EscapeString(strings.TrimSpace(s))
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

func envIntWithLegacy(name string, legacyName string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))

	if raw == "" && legacyName != "" {
		raw = strings.TrimSpace(os.Getenv(legacyName))
		if raw != "" {
			log.Printf("%s is deprecated; use %s instead", legacyName, name)
		}
	}

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
