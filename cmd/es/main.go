package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/cognicraft/event"
)

func main() {

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	serveBind := serveCommand.String("bind", "127.0.0.1:4711", "addresss")
	serveCommand.Usage = func() {
		fmt.Println("usage: es serve [<options>] <database-connection>")
		serveCommand.PrintDefaults()
	}

	streamCommand := flag.NewFlagSet("stream", flag.ExitOnError)
	streamFollow := streamCommand.Bool("follow", false, "follow")
	streamFrom := streamCommand.Uint64("from", 0, "from version")
	streamCommand.Usage = func() {
		fmt.Println("usage: es stream [<options>] <stream-url>")
		streamCommand.PrintDefaults()
	}

	replicateCommand := flag.NewFlagSet("replicate", flag.ExitOnError)
	replicateSource := replicateCommand.String("source", "", "URL of a stream.")
	replicateTarget := replicateCommand.String("target", "", "Database.")
	replicateFollow := replicateCommand.Bool("follow", false, "follow")
	replicateCommand.Usage = func() {
		fmt.Println("usage: es replicate [<options>]")
		replicateCommand.PrintDefaults()
	}

	if len(os.Args) == 1 {
		fmt.Println("usage: es <command> [<args>]")
		fmt.Println("The most commonly used es commands are: ")
		fmt.Println("  replicate Replicates a stream into a Database.")
		fmt.Println("  serve     Provides HTTP access to an event-store.")
		fmt.Println("  stream    Copies stream to out.")
		return
	}

	switch os.Args[1] {
	case "serve":
		serveCommand.Parse(os.Args[2:])
	case "stream":
		streamCommand.Parse(os.Args[2:])
	case "replicate":
		replicateCommand.Parse(os.Args[2:])
	default:
		fmt.Printf("%q is not a valid command.\n", os.Args[1])
		os.Exit(2)
	}

	switch {
	case serveCommand.Parsed():
		if len(serveCommand.Args()) == 0 {
			serveCommand.Usage()
			return
		}
		ds := serveCommand.Args()[0]
		serve(*serveBind, ds)
	case streamCommand.Parsed():
		if len(streamCommand.Args()) == 0 {
			streamCommand.Usage()
			return
		}
		stream(streamCommand.Args()[0], *streamFollow, *streamFrom)
	case replicateCommand.Parsed():
		target := *replicateTarget
		replicate(*replicateSource, target, *replicateFollow)
	}
}

func serve(bind string, dsn string) {
	store, err := event.NewBasicStore(dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	defer store.Close()

	server, err := event.NewServer(bind, store)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	server.Run()
}

func stream(stream string, follow bool, skip uint64) {
	opts := []event.StreamerOption{
		event.From(skip),
	}
	if follow {
		opts = append(opts, event.Follow())
	}
	streamer, err := event.NewStreamer(stream, opts...)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	for e := range streamer.Stream() {
		bs, err := json.Marshal(e)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] %s\n", err)
		}
		fmt.Printf("%s\n", string(bs))
	}
}

func replicate(source string, target string, follow bool) {
	store, err := event.NewBasicStore(target)
	defer store.Close()
	vAll := store.Version(event.All)

	opts := []event.StreamerOption{
		event.From(vAll),
	}
	if follow {
		opts = append(opts, event.Follow())
	}
	streamer, err := event.NewStreamer(source, opts...)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for record := range streamer.Stream() {
		err := store.Append(record.OriginStreamID, record.OriginStreamIndex, event.Records{record})
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}
}
