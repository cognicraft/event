package event

import (
	"fmt"
	"log"
	"net/http"

	"github.com/cognicraft/hyper"
	"github.com/cognicraft/mux"
)

func NewServer(bind string, store *Store) (*Server, error) {
	s := &Server{
		bind:   bind,
		store:  store,
		router: mux.New(),
	}
	s.init()
	return s, nil
}

type Server struct {
	bind              string
	store             *Store
	storeSubscription Subscription
	router            *mux.Router
	signal            mux.SignalFunc
}

func (s *Server) Run() error {
	log.Printf("binding to '%s'", s.bind)
	return http.ListenAndServe(s.bind, s.router)
}

func (s *Server) Close() error {
	return s.storeSubscription.Cancel()
}

func (s *Server) init() {
	longPolling, signal := mux.LongPolling()
	s.signal = signal

	chain := mux.NewChain(
		mux.NoCache,
		mux.CORS(mux.AccessControlDefaults),
		mux.GZIP,
		longPolling,
	)
	s.router.Route("/").GET(chain.ThenFunc(s.handleGET))
	s.router.Route("/streams/").GET(chain.ThenFunc(s.handleGETStreams))
	s.router.Route("/streams/:id").GET(chain.ThenFunc(s.handleGETStream))
	s.router.Route("/streams/:id").POST(chain.ThenFunc(s.handlePOSTStream))

	s.storeSubscription = s.store.SubscribeToStreamFromCurrent(All)
	s.storeSubscription.On(s.onRecord)
}

func (s *Server) onRecord(r Record) {
	s.signal(fmt.Sprintf("/streams/%s", r.OriginStreamID))
}

func (s *Server) handleGET(w http.ResponseWriter, r *http.Request) {
	resolve := hyper.ExternalURLResolver(r)
	res := hyper.Item{
		Links: hyper.Links{
			{
				Rel:  hyper.RelSelf,
				Href: resolve("").String(),
			},
			{
				Rel:  "streams",
				Href: resolve("./streams/").String(),
			},
		},
	}
	hyper.Write(w, http.StatusOK, res)
}

func (s *Server) handleGETStreams(w http.ResponseWriter, r *http.Request) {
	resolve := hyper.ExternalURLResolver(r)
	res := hyper.Item{
		Links: hyper.Links{
			{
				Rel:  hyper.RelSelf,
				Href: resolve("").String(),
			},
			{
				Rel:  All,
				Href: resolve("./%s", All).String(),
			},
			{
				Rel:      "stream",
				Template: resolve("./").String() + "{id}",
				Parameters: hyper.Parameters{
					{
						Name: "id",
						Type: hyper.TypeText,
					},
				},
			},
		},
	}
	hyper.Write(w, http.StatusOK, res)
}

func (s *Server) handleGETStream(w http.ResponseWriter, r *http.Request) {
	resolve := hyper.ExternalURLResolver(r)
	streamID := r.Context().Value(":id").(string)
	feeder := NewFeeder(s.store, streamID)
	page := feeder.Page(resolve(""))
	if streamID != All {
		page.AddAction(hyper.Action{
			Rel:    "append",
			Href:   resolve("").String(),
			Method: hyper.MethodPOST,
			Parameters: hyper.Parameters{
				hyper.ActionParameter("append"),
				{
					Name:     "events",
					Type:     "application/vnd.event+json",
					Multiple: true,
				},
			},
		})
	}
	hyper.Write(w, http.StatusOK, page)
}

func (s *Server) handlePOSTStream(w http.ResponseWriter, r *http.Request) {
	streamID := r.Context().Value(":id").(string)
	cmd := hyper.ExtractCommand(r)
	switch cmd.Action {
	case "append":
		if streamID == All {
			hyper.Write(w, http.StatusBadRequest, Response(
				fmt.Sprintf("events can not be appended to %s", All),
				fmt.Errorf("events can not be appended to %s", All),
			))
			return
		}
		rs := Records{}
		err := cmd.Arguments.JSON("events", &rs)
		if err != nil {
			hyper.Write(w, http.StatusBadRequest, Response(
				fmt.Sprintf("could not deserialize events"),
				err,
			))
			return
		}
		v := s.store.Version(streamID)
		err = s.store.Append(streamID, v, rs)
		if err != nil {
			hyper.Write(w, http.StatusBadRequest, Response(
				fmt.Sprintf("could not append to %s", streamID),
				err,
			))
			return
		}
		hyper.Write(w, http.StatusOK, Response(
			fmt.Sprintf("appended %d events to %s", len(rs), streamID),
		))
		return
	default:
		hyper.Write(w, http.StatusBadRequest, Response(
			"unknown action",
			fmt.Errorf("unknown action: %v", cmd.Action),
		))
	}
}

func Response(msg string, errs ...error) hyper.Item {
	res := hyper.Item{
		Type: "response",
		Properties: hyper.Properties{
			{
				Name:  "message",
				Value: msg,
			},
		},
	}
	for _, err := range errs {
		e := hyper.Error{Message: err.Error()}
		if errC, ok := err.(errorCoder); ok {
			e.Code = errC.Code()
		}
		res.Errors = append(res.Errors, e)
	}
	return res
}

type errorCoder interface {
	Code() string
}
