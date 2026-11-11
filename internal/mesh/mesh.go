// Package mesh is the interconnect between nodes (ADR-0032): one QUIC connection per pair of nodes, a control stream
// for live state and datagrams for voice and pings, all in the messages of ruumble.node.v1.
//
// The node with the lower number dials, the other accepts. Both sides prove themselves with their node key, which the
// cluster configuration lists by its SHA-256 pin. The mesh keeps a copy of the sessions homed on its own node, so a
// peer that connects gets them as its first Sync and every later change after it, in order.
package mesh

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/ruumble-project/ruumble-foundation/gen/ruumble/node/v1"
)

// ALPN names the major version of the interconnect.
const ALPN = "ruumble-node/1"

// Minor is the level within v1 this node speaks.
const Minor = 0

const (
	maxFrame         = 1 << 20
	controlQueue     = 1024
	handshakeTimeout = 2 * time.Second
	writeTimeout     = 5 * time.Second
	redialMin        = 100 * time.Millisecond
	redialMax        = 2 * time.Second
)

// QUIC application error codes when a link is closed.
const (
	codeDone     quic.ApplicationErrorCode = 0
	codeSilent   quic.ApplicationErrorCode = 1
	codeProtocol quic.ApplicationErrorCode = 2
	codeReplaced quic.ApplicationErrorCode = 3
)

// Peer is one node of the cluster as the configuration lists it.
type Peer struct {
	Number uint32 `json:"number"`
	Name   string `json:"name"`
	// Addr is where the node's interconnect is reached, host:port over UDP.
	Addr string `json:"addr"`
	// Pin is the SHA-256 of the node key's public key (SPKI), hex encoded.
	Pin string `json:"pin"`
}

// Config sets up a mesh.
type Config struct {
	Cluster string
	// Self is this node's number; Nodes lists every node, this one included.
	Self  uint32
	Nodes []Peer
	// Cert is this node's key and certificate; its pin must be the one listed for Self.
	Cert  tls.Certificate
	Build string
	Log   *slog.Logger
	// PingInterval and PeerTimeout default to 500 ms and 1.5 s (ADR-0032).
	PingInterval time.Duration
	PeerTimeout  time.Duration
}

// Handler receives what peers announce. Calls for one peer come in the order the peer sent them; the mesh holds no
// lock of its own while calling.
type Handler interface {
	// PeerSync replaces everything the handler holds for node: it is the first thing on every new connection.
	PeerSync(node uint32, sync *nodev1.Sync)
	PeerSession(node uint32, s *nodev1.Session)
	PeerSessionGone(node uint32, g *nodev1.SessionGone)
	PeerStatus(node uint32, st *nodev1.NodeStatus)
	PeerUserCommand(node uint32, c *nodev1.UserCommand)
	PeerDeliver(node uint32, d *nodev1.Deliver)
	PeerVoice(node uint32, v *nodev1.Voice)
	// PeerDown says the connection to node is gone. What it announced stays until the handler gives it up or the
	// next PeerSync replaces it.
	PeerDown(node uint32)
}

// Mesh is this node's side of the interconnect.
type Mesh struct {
	cfg         Config
	log         *slog.Logger
	self        Peer
	nodes       map[uint32]Peer
	pins        map[string]Peer
	incarnation []byte
	tr          *quic.Transport
	qconf       *quic.Config

	mu     sync.RWMutex
	local  map[uint32]*nodev1.Session
	status nodev1.NodeState
	links  map[uint32]*link

	tooLarge atomic.Uint64
}

// PinOf is the pin of a certificate: the SHA-256 of its public key, hex encoded.
func PinOf(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// New makes a mesh on pc, which carries all of its UDP traffic, listening and dialling. Run starts it.
func New(cfg Config, pc net.PacketConn) (*Mesh, error) {
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 500 * time.Millisecond
	}
	if cfg.PeerTimeout == 0 {
		cfg.PeerTimeout = 3 * cfg.PingInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	m := &Mesh{
		cfg:    cfg,
		log:    cfg.Log,
		nodes:  map[uint32]Peer{},
		pins:   map[string]Peer{},
		local:  map[uint32]*nodev1.Session{},
		status: nodev1.NodeState_NODE_STATE_SERVING,
		links:  map[uint32]*link{},
	}
	for _, p := range cfg.Nodes {
		if p.Number < 1 || p.Number > 255 {
			return nil, fmt.Errorf("node %q: number %d not in 1 to 255", p.Name, p.Number)
		}
		if _, dup := m.nodes[p.Number]; dup {
			return nil, fmt.Errorf("node number %d listed twice", p.Number)
		}
		m.nodes[p.Number] = p
		m.pins[p.Pin] = p
	}
	self, ok := m.nodes[cfg.Self]
	if !ok {
		return nil, fmt.Errorf("node %d is not in the cluster configuration", cfg.Self)
	}
	m.self = self
	if len(cfg.Cert.Certificate) == 0 {
		return nil, errors.New("no node certificate")
	}
	leaf, err := x509.ParseCertificate(cfg.Cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	if pin := PinOf(leaf); pin != self.Pin {
		return nil, fmt.Errorf("node key has pin %s, the configuration lists %s for node %d", pin, self.Pin, self.Number)
	}
	m.incarnation = make([]byte, 16)
	rand.Read(m.incarnation)
	m.tr = &quic.Transport{Conn: pc}
	m.qconf = &quic.Config{
		EnableDatagrams:      true,
		HandshakeIdleTimeout: handshakeTimeout,
		// Our pings find a silent peer much sooner; this only cleans up.
		MaxIdleTimeout: 2 * cfg.PeerTimeout,
	}
	return m, nil
}

// Run serves the mesh until ctx ends: it accepts peers with a higher number and dials those with a lower one.
func (m *Mesh) Run(ctx context.Context, h Handler) error {
	ln, err := m.tr.Listen(m.tlsConfig(nil), m.qconf)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, p := range m.nodes {
		if p.Number > m.self.Number {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.dialLoop(ctx, p, h)
			}()
		}
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			break
		}
		go m.accepted(ctx, conn, h)
	}
	m.mu.Lock()
	for _, l := range m.links {
		l.close(codeDone, "node stopping")
	}
	m.mu.Unlock()
	wg.Wait()
	return m.tr.Close()
}

// tlsConfig is mutual TLS 1.3 with pinned keys. With peer set (dialling), only that peer's key is accepted; without
// (accepting), any node of the cluster but this one.
func (m *Mesh) tlsConfig(peer *Peer) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{m.cfg.Cert},
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{ALPN},
		ClientAuth:         tls.RequireAnyClientCert,
		InsecureSkipVerify: true, // replaced by the pin check below
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return errors.New("no certificate")
			}
			cert, err := x509.ParseCertificate(raw[0])
			if err != nil {
				return err
			}
			pin := PinOf(cert)
			if peer != nil {
				if pin != peer.Pin {
					return fmt.Errorf("node %d presented an unknown key", peer.Number)
				}
				return nil
			}
			if p, ok := m.pins[pin]; !ok || p.Number == m.self.Number {
				return errors.New("key not in the cluster configuration")
			}
			return nil
		},
	}
}

func (m *Mesh) dialLoop(ctx context.Context, p Peer, h Handler) {
	wait := redialMin
	for ctx.Err() == nil {
		addr, err := net.ResolveUDPAddr("udp", p.Addr)
		var conn *quic.Conn
		if err == nil {
			dctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
			conn, err = m.tr.Dial(dctx, addr, m.tlsConfig(&p), m.qconf)
			cancel()
		}
		if err == nil {
			wait = redialMin
			m.run(ctx, conn, p, true, h)
		} else {
			m.log.Debug("dial failed", "peer", p.Name, "err", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
		wait = min(2*wait, redialMax)
	}
}

func (m *Mesh) accepted(ctx context.Context, conn *quic.Conn, h Handler) {
	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		conn.CloseWithError(codeProtocol, "no certificate")
		return
	}
	p, ok := m.pins[PinOf(certs[0])]
	if !ok || p.Number > m.self.Number {
		// The lower number dials, the higher accepts; refusing the other way keeps one connection per pair.
		m.log.Debug("refused a peer that should not dial", "peer", p.Name)
		conn.CloseWithError(codeProtocol, "the lower node number dials")
		return
	}
	m.run(ctx, conn, p, false, h)
}

// link is one connection to a peer.
type link struct {
	peer  Peer
	conn  *quic.Conn
	out   chan *nodev1.ControlFrame
	done  chan struct{}
	once  sync.Once
	heard atomic.Int64 // unix nanos of the last datagram
	rtt   atomic.Int64 // nanos, last ping
}

func (l *link) close(code quic.ApplicationErrorCode, why string) {
	l.once.Do(func() {
		close(l.done)
		l.conn.CloseWithError(code, why)
	})
}

// send queues a control frame. A peer that falls this far behind is out of step: the link is closed and the next
// connection starts over with a Sync.
func (l *link) send(f *nodev1.ControlFrame) {
	select {
	case l.out <- f:
	default:
		l.close(codeProtocol, "control queue full")
	}
}

// run holds one connection until it ends: hello both ways, then Sync, then whatever comes.
func (m *Mesh) run(ctx context.Context, conn *quic.Conn, p Peer, dialer bool, h Handler) {
	l := &link{peer: p, conn: conn, out: make(chan *nodev1.ControlFrame, controlQueue), done: make(chan struct{})}
	defer l.close(codeDone, "")
	hctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	var st *quic.Stream
	var err error
	if dialer {
		st, err = conn.OpenStreamSync(hctx)
		if err == nil {
			_, err = protodelim.MarshalTo(st, &nodev1.StreamHeader{Kind: nodev1.StreamKind_STREAM_KIND_CONTROL})
		}
	} else {
		st, err = conn.AcceptStream(hctx)
	}
	if err != nil {
		m.log.Debug("no control stream", "peer", p.Name, "err", err)
		return
	}
	r := bufio.NewReader(st)
	read := protodelim.UnmarshalOptions{MaxSize: maxFrame}
	if !dialer {
		var hdr nodev1.StreamHeader
		st.SetReadDeadline(time.Now().Add(handshakeTimeout))
		if err := read.UnmarshalFrom(r, &hdr); err != nil || hdr.Kind != nodev1.StreamKind_STREAM_KIND_CONTROL {
			l.close(codeProtocol, "expected a control stream")
			return
		}
	}
	hello := &nodev1.ControlFrame{Frame: &nodev1.ControlFrame_Hello{Hello: &nodev1.Hello{
		Cluster:     m.cfg.Cluster,
		Node:        m.self.Number,
		Name:        m.self.Name,
		Incarnation: m.incarnation,
		Minor:       Minor,
		Build:       m.cfg.Build,
	}}}
	st.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := protodelim.MarshalTo(st, hello); err != nil {
		m.log.Debug("hello not sent", "peer", p.Name, "err", err)
		return
	}
	var first nodev1.ControlFrame
	st.SetReadDeadline(time.Now().Add(handshakeTimeout))
	if err := read.UnmarshalFrom(r, &first); err != nil {
		m.log.Debug("no hello", "peer", p.Name, "err", err)
		return
	}
	theirs := first.GetHello()
	switch {
	case theirs == nil:
		l.close(codeProtocol, "expected hello")
		return
	case theirs.Cluster != m.cfg.Cluster:
		m.log.Warn("peer of another cluster", "peer", p.Name, "cluster", theirs.Cluster)
		l.close(codeProtocol, "wrong cluster")
		return
	case theirs.Node != p.Number:
		m.log.Warn("peer names another number than its key", "peer", p.Name, "says", theirs.Node)
		l.close(codeProtocol, "wrong node number")
		return
	}
	st.SetReadDeadline(time.Time{})
	l.heard.Store(time.Now().UnixNano())

	// From here on the link gets every change to the local sessions, after the Sync it starts from.
	m.mu.Lock()
	if old := m.links[p.Number]; old != nil {
		old.close(codeReplaced, "replaced by a new connection")
	}
	m.links[p.Number] = l
	snap := &nodev1.Sync{NodeStatus: &nodev1.NodeStatus{State: m.status}}
	for _, s := range m.local {
		snap.Sessions = append(snap.Sessions, s)
	}
	l.send(&nodev1.ControlFrame{Frame: &nodev1.ControlFrame_Sync{Sync: snap}})
	m.mu.Unlock()
	m.log.Info("peer connected", "peer", p.Name, "build", theirs.Build, "minor", theirs.Minor, "addr", conn.RemoteAddr())

	go m.write(l, st)
	go m.datagrams(ctx, l, h)
	go m.ping(l)
	m.control(l, r, read, h)

	l.close(codeDone, "")
	m.mu.Lock()
	current := m.links[p.Number] == l
	if current {
		delete(m.links, p.Number)
	}
	m.mu.Unlock()
	if current {
		m.log.Info("peer lost", "peer", p.Name)
		h.PeerDown(p.Number)
	}
}

func (m *Mesh) write(l *link, st *quic.Stream) {
	for {
		select {
		case <-l.done:
			return
		case f := <-l.out:
			st.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := protodelim.MarshalTo(st, f); err != nil {
				l.close(codeDone, "")
				return
			}
		}
	}
}

func (m *Mesh) control(l *link, r *bufio.Reader, read protodelim.UnmarshalOptions, h Handler) {
	n := l.peer.Number
	for {
		var f nodev1.ControlFrame
		if err := read.UnmarshalFrom(r, &f); err != nil {
			return
		}
		switch x := f.Frame.(type) {
		case *nodev1.ControlFrame_Sync:
			h.PeerSync(n, x.Sync)
		case *nodev1.ControlFrame_Session:
			h.PeerSession(n, x.Session)
		case *nodev1.ControlFrame_SessionGone:
			h.PeerSessionGone(n, x.SessionGone)
		case *nodev1.ControlFrame_NodeStatus:
			h.PeerStatus(n, x.NodeStatus)
		case *nodev1.ControlFrame_UserCommand:
			h.PeerUserCommand(n, x.UserCommand)
		case *nodev1.ControlFrame_Deliver:
			h.PeerDeliver(n, x.Deliver)
		}
		// Unknown frames come from a newer minor level and are skipped.
	}
}

func (m *Mesh) datagrams(ctx context.Context, l *link, h Handler) {
	for {
		b, err := l.conn.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		l.heard.Store(time.Now().UnixNano())
		var d nodev1.Datagram
		if proto.Unmarshal(b, &d) != nil {
			continue
		}
		switch x := d.Datagram.(type) {
		case *nodev1.Datagram_Voice:
			h.PeerVoice(l.peer.Number, x.Voice)
		case *nodev1.Datagram_Ping:
			m.sendDatagram(l, &nodev1.Datagram{Datagram: &nodev1.Datagram_Pong{Pong: &nodev1.Pong{Id: x.Ping.Id, Sent: x.Ping.Sent}}})
		case *nodev1.Datagram_Pong:
			l.rtt.Store(monotonic() - int64(x.Pong.Sent))
		}
	}
}

// ping sends a ping every interval and gives the peer up once nothing has come for PeerTimeout.
func (m *Mesh) ping(l *link) {
	t := time.NewTicker(m.cfg.PingInterval)
	defer t.Stop()
	var id uint64
	for {
		select {
		case <-l.done:
			return
		case now := <-t.C:
			if now.Sub(time.Unix(0, l.heard.Load())) > m.cfg.PeerTimeout {
				m.log.Info("peer silent", "peer", l.peer.Name)
				l.close(codeSilent, "silent")
				return
			}
			id++
			m.sendDatagram(l, &nodev1.Datagram{Datagram: &nodev1.Datagram_Ping{Ping: &nodev1.Ping{Id: id, Sent: uint64(monotonic())}}})
		}
	}
}

var start = time.Now()

func monotonic() int64 { return int64(time.Since(start)) }

func (m *Mesh) sendDatagram(l *link, d *nodev1.Datagram) {
	b, err := proto.Marshal(d)
	if err != nil {
		return
	}
	m.sendRaw(l, b)
}

func (m *Mesh) sendRaw(l *link, b []byte) {
	err := l.conn.SendDatagram(b)
	var tl *quic.DatagramTooLargeError
	if errors.As(err, &tl) {
		m.tooLarge.Add(1)
	}
}

// Announce tells every peer about a session homed on this node, now and on every later connection.
func (m *Mesh) Announce(s *nodev1.Session) {
	s = proto.Clone(s).(*nodev1.Session)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.local[s.Session] = s
	for _, l := range m.links {
		l.send(&nodev1.ControlFrame{Frame: &nodev1.ControlFrame_Session{Session: s}})
	}
}

// Gone tells every peer that a session is no longer homed on this node.
func (m *Mesh) Gone(session uint32, claim uint64, reason nodev1.SessionGone_Reason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.local, session)
	g := &nodev1.SessionGone{Session: session, Claim: claim, Reason: reason}
	for _, l := range m.links {
		l.send(&nodev1.ControlFrame{Frame: &nodev1.ControlFrame_SessionGone{SessionGone: g}})
	}
}

// SetStatus announces this node's state, serving or draining.
func (m *Mesh) SetStatus(state nodev1.NodeState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = state
	for _, l := range m.links {
		l.send(&nodev1.ControlFrame{Frame: &nodev1.ControlFrame_NodeStatus{NodeStatus: &nodev1.NodeStatus{State: state}}})
	}
}

// Forward sends a voice frame to every connected peer as a datagram. A frame too large for one is dropped and
// counted.
func (m *Mesh) Forward(v *nodev1.Voice) {
	b, err := proto.Marshal(&nodev1.Datagram{Datagram: &nodev1.Datagram_Voice{Voice: v}})
	if err != nil {
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, l := range m.links {
		m.sendRaw(l, b)
	}
}

// PeerInfo describes a connected peer.
type PeerInfo struct {
	Peer
	RTT time.Duration
}

// Peers lists the connected peers.
func (m *Mesh) Peers() []PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []PeerInfo
	for _, l := range m.links {
		out = append(out, PeerInfo{Peer: l.peer, RTT: time.Duration(l.rtt.Load())})
	}
	return out
}

// TooLarge is the number of voice frames dropped because they did not fit a datagram.
func (m *Mesh) TooLarge() uint64 { return m.tooLarge.Load() }
