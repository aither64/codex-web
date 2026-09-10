package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aither64/codex-web/codex"
	"github.com/aither64/codex-web/conversation"
)

var page = template.Must(template.New("page").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Codex conversation</title>
  <link rel="stylesheet" href="/codex/assets/conversation.css">
</head>
<body>
  <main id="conversation"></main>
  <script type="module">
    import {mountConversation} from "/codex/assets/conversation.js";
    mountConversation(document.querySelector("#conversation"), {
	  id: "current",
    });
  </script>
</body>
</html>`))

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	socket := flag.String("socket", "", "Codex App Server Unix socket")
	thread := flag.String("thread", "", "trusted Codex thread ID")
	directory := flag.String("cwd", "", "trusted canonical thread working directory")
	origin := flag.String("origin", "http://127.0.0.1:8080", "exact browser origin")
	flag.Parse()
	if *socket == "" || *thread == "" || *directory == "" {
		flag.Usage()
		os.Exit(2)
	}
	if err := requireLoopbackAddress(*listen); err != nil {
		log.Fatal(err)
	}
	if err := requireLoopbackOrigin(*origin); err != nil {
		log.Fatal(err)
	}
	canonical, err := filepath.Abs(*directory)
	if err != nil {
		log.Fatal(err)
	}
	client := codex.New(*socket)
	defer client.Close()
	mutationLock := conversation.NewMutationLock()
	handler, err := conversation.NewHandler(conversation.Options{
		AllowedOrigins: []string{*origin},
		Resolver: conversation.ResolverFunc(func(_ context.Context, request conversation.ResolveRequest) (conversation.Target, error) {
			if request.ID != "current" {
				return conversation.Target{}, fmt.Errorf("unknown conversation")
			}
			return conversation.Target{
				Client: client, ThreadID: *thread, Directory: canonical,
				MutationLock: mutationLock,
				Capabilities: standaloneCapabilities(),
			}, nil
		}),
	})
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/codex/", handler)
	mux.HandleFunc("/", func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := page.Execute(response, nil); err != nil {
			log.Printf("render page: %v", err)
		}
	})
	server := &http.Server{
		Addr: *listen, Handler: securityHeaders(mux), ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("listening on http://%s", *listen)
	log.Fatal(server.ListenAndServe())
}

func requireLoopbackAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}
	if !loopbackHost(host) {
		return errors.New("the standalone example listens on loopback addresses only")
	}
	return nil
}

func requireLoopbackOrigin(raw string) error {
	origin, err := url.Parse(raw)
	if err != nil || origin.Scheme == "" || origin.Host == "" || origin.User != nil ||
		origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" ||
		!loopbackHost(origin.Hostname()) {
		return errors.New("the standalone example allows loopback browser origins only")
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(
			"Content-Security-Policy", "default-src 'self'; connect-src 'self'; frame-ancestors 'none'",
		)
		response.Header().Set("X-Frame-Options", "DENY")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(response, request)
	})
}

func standaloneCapabilities() conversation.Capabilities {
	return conversation.Capabilities{
		Read: true, Pending: true, QueueRead: true, Send: true,
		Queue: true, Interrupt: true, Settings: true, Respond: true,
		EventStream: true,
	}
}
