package event

import (
	"net/url"
	"strings"
)

type urn string

func (u urn) Path() string {
	raw := string(u)
	q := strings.Index(raw, "?")
	if q >= 0 {
		return raw[0:q]
	}
	return raw
}

func (u urn) Query() url.Values {
	raw := string(u)
	q := strings.Index(raw, "?")
	if q < 0 {
		return url.Values{}
	}
	vs := url.Values{}
	rest := raw[q+1:]
	ps := strings.Split(rest, "&")
	for _, p := range ps {
		nv := strings.Split(p, "=")
		if len(nv) != 2 {
			// illegal: we will ignore this parameter
			continue
		}
		n := nv[0]
		v := nv[1]

		cvs := vs[n]
		cvs = append(cvs, v)
		vs[n] = cvs
	}
	return vs
}
