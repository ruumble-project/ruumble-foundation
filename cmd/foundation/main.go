// Command foundation runs one node of Ruumble Foundation.
//
//	foundation -listen :64738                                  a node on its own
//	foundation -cluster cluster.json -node 1 -node-cert n.pem -node-key n.key
//	                                                           a node of a cluster (ADR-0032)
//	foundation keygen -cert n.pem -key n.key                   make a node key and print its pin
//	foundation pin n.pem                                       print the pin of a node certificate
//
// The cluster file lists every node with its interconnect address and the pin of its node key:
//
//	{"cluster": "lab", "nodes": [{"number": 1, "name": "fsn1", "addr": "10.0.0.1:64739", "pin": "..."}, ...]}
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/ruumble-project/ruumble-foundation/internal/certs"
	"github.com/ruumble-project/ruumble-foundation/internal/mesh"
	"github.com/ruumble-project/ruumble-foundation/internal/voice"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "keygen":
			exit(keygen(os.Args[2:]))
			return
		case "pin":
			exit(pin(os.Args[2:]))
			return
		}
	}
	exit(serve())
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "foundation:", err)
		os.Exit(1)
	}
}

type clusterFile struct {
	Cluster string      `json:"cluster"`
	Nodes   []mesh.Peer `json:"nodes"`
}

func serve() error {
	listen := flag.String("listen", ":64738", "address for Mumble clients")
	certFile := flag.String("cert", "", "TLS certificate for Mumble clients in PEM; without -cert and -key a throwaway one is made")
	keyFile := flag.String("key", "", "TLS key for Mumble clients in PEM")
	welcome := flag.String("welcome", "", "welcome text shown to clients")
	clusterPath := flag.String("cluster", "", "cluster file (JSON); without it the node runs on its own")
	nodeNumber := flag.Uint("node", 0, "this node's number in the cluster file")
	nodeCert := flag.String("node-cert", "", "certificate of this node's key for the interconnect, in PEM")
	nodeKey := flag.String("node-key", "", "this node's key for the interconnect, in PEM")
	meshListen := flag.String("mesh-listen", "", "UDP address for the interconnect; defaults to the port of this node's addr in the cluster file")
	debugLog := flag.Bool("debug", false, "log more")
	flag.Parse()

	level := slog.LevelInfo
	if *debugLog {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cert, err := certs.Load(*certFile, *keyFile)
	if err != nil {
		return fmt.Errorf("certificate: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := voice.Config{
		// Mumble clients present their certificate when asked; its hash identifies them.
		TLS:     &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, ClientAuth: tls.RequestClientCert},
		Welcome: *welcome,
		Log:     log,
	}
	var m *mesh.Mesh
	if *clusterPath != "" {
		m, err = startMesh(*clusterPath, uint32(*nodeNumber), *nodeCert, *nodeKey, *meshListen, log)
		if err != nil {
			return err
		}
		cfg.Node = uint32(*nodeNumber)
		cfg.Mesh = m
	}
	srv := voice.New(cfg)
	if m != nil {
		go func() {
			if err := m.Run(ctx, srv); err != nil {
				log.Error("interconnect", "err", err)
				stop()
			}
		}()
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	log.Info("node listening", "addr", ln.Addr().String(), "node", cfg.Node)
	if err := srv.Serve(ctx, ln); err != nil {
		return err
	}
	log.Info("node stopped")
	return nil
}

func startMesh(path string, self uint32, certFile, keyFile, listen string, log *slog.Logger) (*mesh.Mesh, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cf clusterFile
	if err := json.Unmarshal(b, &cf); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if self == 0 {
		return nil, errors.New("-cluster needs -node")
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("-cluster needs -node-cert and -node-key (make them with: foundation keygen)")
	}
	key, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("node key: %w", err)
	}
	if listen == "" {
		for _, n := range cf.Nodes {
			if n.Number == self {
				_, port, err := net.SplitHostPort(n.Addr)
				if err != nil {
					return nil, fmt.Errorf("node %d addr: %w", self, err)
				}
				listen = ":" + port
			}
		}
	}
	pc, err := net.ListenPacket("udp", listen)
	if err != nil {
		return nil, err
	}
	m, err := mesh.New(mesh.Config{Cluster: cf.Cluster, Self: self, Nodes: cf.Nodes, Cert: key, Build: build(), Log: log}, pc)
	if err != nil {
		pc.Close()
		return nil, err
	}
	log.Info("interconnect listening", "addr", pc.LocalAddr().String(), "cluster", cf.Cluster, "nodes", len(cf.Nodes))
	return m, nil
}

func build() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
		return bi.Main.Version
	}
	return ""
}

func keygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	certFile := fs.String("cert", "node.pem", "where to write the certificate")
	keyFile := fs.String("key", "node.key", "where to write the key")
	name := fs.String("name", "Ruumble Foundation node", "name in the certificate")
	fs.Parse(args)
	cert, err := certs.SelfSignedFor(*name, 100*365*24*time.Hour)
	if err != nil {
		return err
	}
	if err := certs.Write(cert, *certFile, *keyFile); err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	fmt.Println(mesh.PinOf(leaf))
	return nil
}

func pin(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: foundation pin node.pem")
	}
	b, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	block, _ := pem.Decode(b)
	if block == nil || block.Type != "CERTIFICATE" {
		return errors.New("no certificate in " + args[0])
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	fmt.Println(mesh.PinOf(leaf))
	return nil
}
