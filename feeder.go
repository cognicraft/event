package event

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cognicraft/hyper"
)

const (
	nSkip           = "skip"
	nLimit          = "limit"
	minPageSize     = 1
	defaultPageSize = 50
)

func NewFeeder(store Store, streamID string) *Feeder {
	return &Feeder{
		Store:    store,
		StreamID: streamID,
		PageSize: defaultPageSize,
	}
}

type Feeder struct {
	Store    Store
	StreamID string
	PageSize uint64
}

func (f *Feeder) Page(url *url.URL) hyper.Item {
	limit := f.PageSize
	version := f.Store.Version(f.StreamID)
	if qLimit := url.Query().Get(nLimit); qLimit != "" {
		limit, _ = strconv.ParseUint(qLimit, 10, 64)
		if limit < minPageSize {
			limit = minPageSize
		}
	}
	skip := uint64(0)
	if np := numberOfPages(version, limit); np > 0 {
		skip = (np - 1) * limit
	}
	if qSkip := url.Query().Get(nSkip); qSkip != "" {
		skip, _ = strconv.ParseUint(qSkip, 10, 64)
	}

	page := hyper.Item{
		Type: "event-records",
		Links: []hyper.Link{
			{
				Rel:  hyper.RelSelf,
				Href: pageURL(url, skip, limit),
			},
			{
				Rel:      hyper.RelSearch,
				Template: searchTemplate(url),
				Parameters: []hyper.Parameter{
					{
						Name: nSkip,
						Type: "int",
					},
				},
			},
			{
				Rel:  hyper.RelLast,
				Href: streamURL(url),
			},
			{
				Rel:  hyper.RelFirst,
				Href: pageURL(url, 0, limit),
			},
		},
	}

	s, err := f.Store.LoadSlice(f.StreamID, skip, limit)
	if err != nil {
		return page
	}

	for _, r := range s.Records {
		rItem := hyper.Item{
			Type: "event-record",
		}
		rItem.EncodeData(r)
		page.Items = append(hyper.Items{rItem}, page.Items...)
	}

	previous := skip > 0
	if previous {
		s := uint64(0)
		if skip > limit {
			s = skip - limit
		}
		page.Links = append(page.Links,
			hyper.Link{
				Rel:  hyper.RelPrevious,
				Href: pageURL(url, s, limit),
			})
	}

	if len(page.Items) > 0 {
		page.Links = append(page.Links,
			hyper.Link{
				Rel:  hyper.RelNext,
				Href: pageURL(url, s.Next, limit),
			})
	}
	return page
}

func streamURL(baseURL *url.URL) string {
	return fmt.Sprintf("%s://%s%s", baseURL.Scheme, baseURL.Host, baseURL.Path)
}

func pageURL(baseURL *url.URL, skip uint64, limit uint64) string {
	pageURL, _ := baseURL.Parse(fmt.Sprintf("?%s=%d&%s=%d", nSkip, skip, nLimit, limit))
	return pageURL.String()
}

func searchTemplate(baseURL *url.URL) string {
	pageURL, _ := baseURL.Parse("")
	pageURL.RawQuery = ""
	return pageURL.String() + fmt.Sprintf("{?%s}", nSkip)
}

func numberOfPages(version uint64, limit uint64) uint64 {
	p := uint64(version) / limit
	r := uint64(version) % limit
	if r > 0 {
		p++
	}
	return p
}

func HandleGETStream(store Store, stream string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		feeder := NewFeeder(store, stream)
		page := feeder.Page(hyper.ExternalURL(r))
		hyper.Write(w, http.StatusOK, page)
	}
}
