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

	sourceCommand := flag.NewFlagSet("source", flag.ExitOnError)
	sourceFollow := sourceCommand.Bool("f", false, "follow")
	sourceFrom := sourceCommand.Uint64("s", 0, "skip")
	sourceCommand.Usage = func() {
		fmt.Println("usage: es source [<options>] <stream>")
		sourceCommand.PrintDefaults()
	}

	replicateCommand := flag.NewFlagSet("replicate", flag.ExitOnError)
	replicateSource := replicateCommand.String("s", "", "URL of a stream.")
	replicateTarget := replicateCommand.String("t", "", "Database.")
	replicateFollow := replicateCommand.Bool("f", false, "follow")
	replicateCommand.Usage = func() {
		fmt.Println("usage: es replicate [<options>]")
		replicateCommand.PrintDefaults()
	}

	if len(os.Args) == 1 {
		fmt.Println("usage: es <command> [<args>]")
		fmt.Println("The most commonly used es commands are: ")
		fmt.Println("  replicate Replicates a stream into a Database.")
		fmt.Println("  serve     Provides HTTP access to an event-store.")
		fmt.Println("  source    Copies stream to out.")
		return
	}

	switch os.Args[1] {
	case "serve":
		serveCommand.Parse(os.Args[2:])
	case "source":
		sourceCommand.Parse(os.Args[2:])
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
	case sourceCommand.Parsed():
		if len(sourceCommand.Args()) == 0 {
			sourceCommand.Usage()
			return
		}
		source(sourceCommand.Args()[0], *sourceFollow, *sourceFrom)
	case replicateCommand.Parsed():
		target := *replicateTarget
		replicate(*replicateSource, target, *replicateFollow)
	}
}

func serve(bind string, dsn string) {
	store, err := event.NewStore(dsn)
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

func source(stream string, follow bool, skip uint64) {
	streamer, _ := event.NewStreamer(stream)
	if follow {
		streamer.SetOption(event.Follow())
	}
	if skip != 0 {
		streamer.SetOption(event.From(skip + 1))
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
	store, err := event.NewStore(target)
	defer store.Close()
	vAll := store.Version(event.All)
	streamer, err := event.NewStreamer(source, event.From(vAll))
	if err != nil {
		log.Fatalf("%+v", err)
	}
	opts := []func(*event.Streamer) error{}
	if follow {
		opts = append(opts, event.Follow())
	}
	err = streamer.SetOption(opts...)
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
