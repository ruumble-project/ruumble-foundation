// Command foundation runs one node of Ruumble Foundation.
//
//	foundation -listen :64738
//
// For now a node serves Mumble clients on its own: one channel, voice over the TCP tunnel. Clustering follows
// (ADR-0029).
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ruumble-project/ruumble-foundation/internal/certs"
	"github.com/ruumble-project/ruumble-foundation/internal/voice"
)

func main() {
	listen := flag.String("listen", ":64738", "address for Mumble clients")
	certFile := flag.String("cert", "", "TLS certificate in PEM; without -cert and -key a throwaway one is made")
	keyFile := flag.String("key", "", "TLS key in PEM")
	welcome := flag.String("welcome", "", "welcome text shown to clients")
	debug := flag.Bool("debug", false, "log more")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cert, err := certs.Load(*certFile, *keyFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "certificate:", err)
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := voice.New(voice.Config{
		TLS:     &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		Welcome: *welcome,
		Log:     log,
	})
	log.Info("node listening", "addr", ln.Addr().String())
	if err := srv.Serve(ctx, ln); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	log.Info("node stopped")
}
