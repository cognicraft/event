package event

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/cognicraft/hyper"
	"github.com/cognicraft/uri"
)

type StreamerOption func(*Streamer) error

func NewStreamer(url string, opts ...StreamerOption) (*Streamer, error) {
	s := &Streamer{
		url:            url,
		timeout:        30 * time.Second,
		follow:         false,
		currentVersion: 0,
		name:           "",
		stream:         make(chan Record),
		done:           make(chan struct{}, 0),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	s.reqCtx, s.reqCancel = context.WithCancel(context.Background())
	if s.client == nil {
		s.client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}
	go s.run()
	return s, nil
}

type Streamer struct {
	client           *http.Client
	url              string
	timeout          time.Duration
	follow           bool
	startWithCurrent bool
	currentVersion   uint64
	name             string
	stream           chan Record
	done             chan struct{}
	reqCtx           context.Context
	reqCancel        context.CancelFunc
}

func UseClient(client *http.Client) func(*Streamer) error {
	return func(s *Streamer) error {
		s.client = client
		return nil
	}
}

func Named(name string) func(*Streamer) error {
	return func(s *Streamer) error {
		s.name = name
		return nil
	}
}

func Follow() func(*Streamer) error {
	return func(s *Streamer) error {
		s.follow = true
		return nil
	}
}

func From(version uint64) func(*Streamer) error {
	return func(s *Streamer) error {
		s.currentVersion = version
		return nil
	}
}

func FromCurrent() func(*Streamer) error {
	return func(s *Streamer) error {
		s.startWithCurrent = true
		return nil
	}
}

func Timeout(d time.Duration) func(*Streamer) error {
	return func(s *Streamer) error {
		s.timeout = d
		return nil
	}
}

func (s *Streamer) Stream() RecordStream {
	return s.stream
}

func (s *Streamer) URL() string {
	return s.url
}

func (s *Streamer) Close() error {
	close(s.done)
	s.reqCancel()
	type connectionCloser interface {
		CloseIdleConnections()
	}
	if t, ok := s.client.Transport.(connectionCloser); ok {
		t.CloseIdleConnections()
	}
	return nil
}

func (s *Streamer) run() {
	defer close(s.stream)
	if s.startWithCurrent {
		s.currentVersion = s.findCurrentVersion()
	}
	currentVersion := s.currentVersion
	frontier := make(chan *entry, 1)
	first := s.findStart()
	if first != nil {
		frontier <- first
	}
	for {
		select {
		case <-s.done:
			return
		case e := <-frontier:
			var page hyper.Item
			var etag string
			var err error
			if e.etag == "" {
				page, etag, err = s.getPage(request(e.url))
				if err != nil {
					if s.reqCtx.Err() == context.Canceled {
						return
					}
					time.Sleep(time.Millisecond * 500)
				}
			} else {
				page, etag, err = s.getPage(longPollRequest(e.url, e.etag, s.timeout))
				if err != nil {
					if s.reqCtx.Err() == context.Canceled {
						return
					}
					time.Sleep(time.Millisecond * 500)
				}
				if etag == e.etag {
					time.Sleep(time.Millisecond * 100)
				}
			}

			for e := range pageToStream(page) {
				if e.StreamIndex < currentVersion {
					// skip
					continue
				}
				currentVersion = e.StreamIndex + 1
				select {
				case <-s.done:
					return
				case s.stream <- e:
				}
			}
			nextLink, exists := page.Links.FindByRel(hyper.RelNext)
			if exists {
				frontier <- &entry{url: nextLink.Href}
			} else if s.follow {
				frontier <- &entry{url: e.url, etag: etag}
			} else {
				close(frontier)
				return
			}
		}
	}
}

func (s *Streamer) findCurrentVersion() uint64 {
	page, _, err := s.getPage(request(s.url))
	if err != nil {
		return 0
	}
	if len(page.Items) == 0 {
		return 0
	}
	r := Record{}
	if err := page.Items[0].DecodeData(&r); err == nil {
		return r.StreamIndex + 1
	}
	return 0
}

func (s *Streamer) findStart() *entry {
	url := s.url
	for {
		page, _, err := s.getPage(request(url))
		if err != nil {
			return nil
		}
		if selfLink, existsSelf := page.Links.FindByRel(hyper.RelSelf); existsSelf {
			url = selfLink.Href
		}
		if s.currentVersion == 0 {
			if firstLink, exists := page.Links.FindByRel(hyper.RelFirst); exists {
				return &entry{url: firstLink.Href}
			}
		}
		if s.currentVersion > 0 {
			if searchLink, exists := page.Links.FindByRel(hyper.RelSearch); exists {
				exURL, _ := uri.Expand(searchLink.Template, map[string]interface{}{
					nSkip: fmt.Sprintf("%d", s.currentVersion-1),
				})
				return &entry{url: exURL}
			}
			if ContainsEventWithIndex(page, s.currentVersion-1) {
				return &entry{url: url}
			}
		}
		if previousLink, exists := page.Links.FindByRel(hyper.RelPrevious); exists {
			url = previousLink.Href
		} else {
			break
		}
	}
	return &entry{url: url}
}

func (s *Streamer) getPage(req *http.Request) (hyper.Item, string, error) {
	req = req.WithContext(s.reqCtx)
	resp, err := s.client.Do(req)
	if err != nil {
		return hyper.Item{}, "", fmt.Errorf("do: %s", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		page := hyper.Item{}
		err = json.NewDecoder(resp.Body).Decode(&page)
		return page, resp.Header.Get(HeaderEtag), err
	case http.StatusNotModified:
		io.Copy(ioutil.Discard, resp.Body)
		return hyper.Item{}, resp.Header.Get(HeaderEtag), nil
	default:
		io.Copy(ioutil.Discard, resp.Body)
		return hyper.Item{}, "", fmt.Errorf("bad status: %s", resp.Status)
	}
}

func pageToStream(page hyper.Item) RecordStream {
	out := make(chan Record)
	go func() {
		defer close(out)

		for i := len(page.Items) - 1; i >= 0; i-- {
			rItem := page.Items[i]
			if rItem.Type == "event-record" {
				r := Record{}
				if err := rItem.DecodeData(&r); err == nil {
					out <- r
				}
			}
		}

	}()
	return out
}

func request(url string) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add(hyper.HeaderAccept, hyper.ContentTypeHyperItem)
	return req
}

func longPollRequest(url string, etag string, timeout time.Duration) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add(hyper.HeaderAccept, hyper.ContentTypeHyperItem)
	req.Header.Add(hyper.HeaderIfNoneMatch, etag)
	req.Header.Add("Long-Poll", strconv.Itoa(int(timeout.Seconds())))
	return req
}

type entry struct {
	url  string
	etag string
}

func (e entry) String() string {
	return fmt.Sprintf("entry: [%s] [%s]", e.url, e.etag)
}

func ContainsEventWithIndex(page hyper.Item, i uint64) bool {
	for _, rItem := range page.Items {
		if rItem.Type == "event-record" {
			r := Record{}
			if err := rItem.DecodeData(&r); err == nil {
				if r.StreamIndex == i {
					return true
				}
			}
		}
	}
	return false
}

func SubscribeToStream(url string, auxOptions ...StreamerOption) (Subscription, error) {
	options := []StreamerOption{
		Follow(),
	}
	options = append(options, auxOptions...)
	s, err := NewStreamer(url, options...)
	if err != nil {
		return nil, err
	}
	return &urlSubscription{
		url:      url,
		from:     0,
		streamer: s,
	}, nil
}

func SubscribeToStreamFrom(url string, version uint64, auxOptions ...StreamerOption) (Subscription, error) {
	options := []StreamerOption{
		Follow(),
		From(version),
	}
	options = append(options, auxOptions...)
	s, err := NewStreamer(url, options...)
	if err != nil {
		return nil, err
	}
	return &urlSubscription{
		url:      url,
		from:     version,
		streamer: s,
	}, nil
}

type urlSubscription struct {
	url      string
	from     uint64
	streamer *Streamer
}

func (s *urlSubscription) Records() RecordStream {
	return s.streamer.Stream()
}

func (s *urlSubscription) On(callback func(r Record)) {
	records := s.Records()
	go func() {
		for e := range records {
			callback(e)
		}
	}()
}

func (s *urlSubscription) Cancel() error {
	return s.streamer.Close()
}

const (
	HeaderEtag        = "Etag"
	HeaderLongPoll    = "Long-Poll"
	HeaderIfNoneMatch = "If-None-Match"
)
