package mesh

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nodev1 "github.com/ruumble-project/ruumble-foundation/gen/ruumble/node/v1"
	"github.com/ruumble-project/ruumble-foundation/internal/certs"
)

// cuttable drops everything in both directions while cut, as a route that goes away silently.
type cuttable struct {
	net.PacketConn
	cut atomic.Bool
}

func (c *cuttable) WriteTo(b []byte, addr net.Addr) (int, error) {
	if c.cut.Load() {
		return len(b), nil
	}
	return c.PacketConn.WriteTo(b, addr)
}

func (c *cuttable) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, addr, err := c.PacketConn.ReadFrom(b)
		if err != nil || !c.cut.Load() {
			return n, addr, err
		}
	}
}

// recorder is a Handler that keeps what it was told.
type recorder struct {
	mu     sync.Mutex
	syncs  int
	downs  int
	sess   map[uint32]*nodev1.Session
	voices int
}

func (r *recorder) PeerSync(node uint32, s *nodev1.Sync) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncs++
	r.sess = map[uint32]*nodev1.Session{}
	for _, x := range s.Sessions {
		r.sess[x.Session] = x
	}
}
func (r *recorder) PeerSession(node uint32, s *nodev1.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sess[s.Session] = s
}
func (r *recorder) PeerSessionGone(node uint32, g *nodev1.SessionGone) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sess, g.Session)
}
func (r *recorder) PeerStatus(uint32, *nodev1.NodeStatus)       {}
func (r *recorder) PeerUserCommand(uint32, *nodev1.UserCommand) {}
func (r *recorder) PeerDeliver(uint32, *nodev1.Deliver)         {}
func (r *recorder) PeerVoice(uint32, *nodev1.Voice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.voices++
}
func (r *recorder) PeerDown(uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.downs++
}

func (r *recorder) get() (syncs, downs, sessions, voices int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.syncs, r.downs, len(r.sess), r.voices
}

type testNode struct {
	m    *Mesh
	pc   *cuttable
	rec  *recorder
	stop context.CancelFunc
}

func pair(t *testing.T) (*testNode, *testNode) {
	t.Helper()
	var peers []Peer
	var keys []tls.Certificate
	var pcs []*cuttable
	for i := 1; i <= 2; i++ {
		cert, err := certs.SelfSigned(fmt.Sprintf("n%d", i))
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := x509.ParseCertificate(cert.Certificate[0])
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, cert)
		pcs = append(pcs, &cuttable{PacketConn: pc})
		peers = append(peers, Peer{Number: uint32(i), Name: fmt.Sprintf("n%d", i), Addr: pc.LocalAddr().String(), Pin: PinOf(leaf)})
	}
	var out []*testNode
	for i := range 2 {
		m, err := New(Config{
			Cluster:      "test",
			Self:         uint32(i + 1),
			Nodes:        peers,
			Cert:         keys[i],
			Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
			PingInterval: 50 * time.Millisecond,
		}, pcs[i])
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		n := &testNode{m: m, pc: pcs[i], rec: &recorder{sess: map[uint32]*nodev1.Session{}}, stop: cancel}
		go m.Run(ctx, n.rec)
		out = append(out, n)
	}
	return out[0], out[1]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSyncThenChangesThenVoice(t *testing.T) {
	a, b := pair(t)
	// Announced before the peers meet: arrives with the Sync.
	a.m.Announce(&nodev1.Session{Session: 1<<24 | 5, Name: "alice"})
	waitFor(t, "sync", func() bool { s, _, n, _ := b.rec.get(); return s == 1 && n == 1 })
	a.m.Announce(&nodev1.Session{Session: 1<<24 | 6, Name: "bob"})
	a.m.Gone(1<<24|5, 0, nodev1.SessionGone_REASON_LEFT)
	waitFor(t, "changes", func() bool {
		b.rec.mu.Lock()
		defer b.rec.mu.Unlock()
		return len(b.rec.sess) == 1 && b.rec.sess[1<<24|6] != nil
	})
	a.m.Forward(&nodev1.Voice{Audio: []byte{1, 2, 3}, Route: &nodev1.Voice_ChannelId{ChannelId: 0}})
	waitFor(t, "voice", func() bool { _, _, _, v := b.rec.get(); return v == 1 })
	if len(a.m.Peers()) != 1 || len(b.m.Peers()) != 1 {
		t.Errorf("peers %d and %d, want one each", len(a.m.Peers()), len(b.m.Peers()))
	}
}

func TestSilentCutFoundAndHealed(t *testing.T) {
	a, b := pair(t)
	a.m.Announce(&nodev1.Session{Session: 1<<24 | 5, Name: "alice"})
	waitFor(t, "first sync", func() bool { s, _, _, _ := b.rec.get(); return s == 1 })

	cut := time.Now()
	a.pc.cut.Store(true)
	waitFor(t, "both sides to give up", func() bool {
		_, da, _, _ := a.rec.get()
		_, db, _, _ := b.rec.get()
		return da == 1 && db == 1
	})
	// Three ping intervals of 50 ms, plus a tick.
	if d := time.Since(cut); d > time.Second {
		t.Errorf("cut found after %v", d)
	}
	a.m.Announce(&nodev1.Session{Session: 1<<24 | 6, Name: "bob"})
	a.pc.cut.Store(false)
	waitFor(t, "a new connection with a fresh sync", func() bool { s, _, n, _ := b.rec.get(); return s == 2 && n == 2 })
}

func TestUnknownKeyRefused(t *testing.T) {
	_, b := pair(t)
	// A stranger that calls itself node 1 and dials node 2 with a key node 2 does not list.
	stranger, _ := certs.SelfSigned("stranger")
	leaf, _ := x509.ParseCertificate(stranger.Certificate[0])
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
	nodes := []Peer{{Number: 1, Name: "stranger", Addr: pc.LocalAddr().String(), Pin: PinOf(leaf)}, b.m.self}
	m, err := New(Config{Cluster: "test", Self: 1, Nodes: nodes, Cert: stranger, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, pc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx, &recorder{sess: map[uint32]*nodev1.Session{}})
	time.Sleep(500 * time.Millisecond)
	if len(m.Peers()) != 0 {
		t.Error("node 2 accepted a key it does not list")
	}
	if s, _, _, _ := b.rec.get(); s != 1 {
		t.Errorf("node 2 synced %d times, want once (with the real node 1)", s)
	}
}

func TestWrongPinRejectedAtStart(t *testing.T) {
	cert, _ := certs.SelfSigned("n1")
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
	defer pc.Close()
	_, err := New(Config{Cluster: "test", Self: 1, Nodes: []Peer{{Number: 1, Name: "n1", Addr: "x", Pin: "00"}}, Cert: cert}, pc)
	if err == nil {
		t.Fatal("a node key that does not match its pin was accepted")
	}
}
