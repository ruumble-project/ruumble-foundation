package voice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleproto"
	"github.com/ruumble-project/ruumble-foundation/internal/certs"
	"github.com/ruumble-project/ruumble-foundation/internal/mesh"
	"github.com/ruumble-project/ruumble-foundation/internal/wire"
)

// node is one node of a test cluster.
type node struct {
	srv  *Server
	addr string // for Mumble clients
	stop context.CancelFunc
}

// cluster starts n nodes connected by the interconnect on localhost.
func cluster(t *testing.T, n int) []*node {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	var peers []mesh.Peer
	var keys []tls.Certificate
	var pcs []net.PacketConn
	for i := 1; i <= n; i++ {
		cert, err := certs.SelfSigned(fmt.Sprintf("node%d", i))
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := x509.ParseCertificate(cert.Certificate[0])
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, cert)
		pcs = append(pcs, pc)
		peers = append(peers, mesh.Peer{Number: uint32(i), Name: fmt.Sprintf("node%d", i), Addr: pc.LocalAddr().String(), Pin: mesh.PinOf(leaf)})
	}
	var nodes []*node
	for i := range n {
		m, err := mesh.New(mesh.Config{
			Cluster:      "test",
			Self:         uint32(i + 1),
			Nodes:        peers,
			Cert:         keys[i],
			Log:          quiet,
			PingInterval: 50 * time.Millisecond,
		}, pcs[i])
		if err != nil {
			t.Fatal(err)
		}
		cert, _ := certs.SelfSigned("mumble")
		srv := New(Config{
			TLS:   &tls.Config{Certificates: []tls.Certificate{cert}},
			Log:   quiet,
			Node:  uint32(i + 1),
			Mesh:  m,
			Grace: 300 * time.Millisecond,
		})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() {
			if err := m.Run(ctx, srv); err != nil {
				t.Log("mesh:", err)
			}
		}()
		go srv.Serve(ctx, ln)
		nodes = append(nodes, &node{srv: srv, addr: ln.Addr().String(), stop: cancel})
	}
	return nodes
}

// userState waits for a UserState about session.
func (c *client) userState(session uint32) *mumbleproto.UserState {
	c.t.Helper()
	for {
		var st mumbleproto.UserState
		if err := proto.Unmarshal(c.until(wire.TypeUserState), &st); err != nil {
			c.t.Fatal(err)
		}
		if st.GetSession() == session {
			return &st
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSessionIdsCarryTheNodeNumber(t *testing.T) {
	nodes := cluster(t, 2)
	_, s1 := join(t, nodes[0].addr, "alice")
	_, s2 := join(t, nodes[1].addr, "bob")
	if s1>>24 != 1 || s2>>24 != 2 {
		t.Errorf("sessions %#x and %#x, want node numbers 1 and 2 in the top byte", s1, s2)
	}
}

func TestUsersOfOtherNodesAreShown(t *testing.T) {
	nodes := cluster(t, 3)
	a, _ := join(t, nodes[0].addr, "alice")
	_, sb := join(t, nodes[2].addr, "bob")
	if st := a.userState(sb); st.GetName() != "bob" {
		t.Errorf("alice sees %q for bob's session", st.GetName())
	}
	// A client joining later gets the remote users with the server's state.
	eventually(t, "node 2 to know bob", func() bool { return nodes[1].srv.Remote() == 2 })
	join(t, nodes[1].addr, "carol")
	// Names are unique in the whole cluster.
	d := dial(t, nodes[1].addr)
	d.hello(1<<48|6<<32, "bob")
	var r mumbleproto.Reject
	proto.Unmarshal(d.until(wire.TypeReject), &r)
	if r.GetType() != mumbleproto.Reject_UsernameInUse {
		t.Errorf("reject type %v, want UsernameInUse", r.GetType())
	}
}

func TestVoiceCrossesNodes(t *testing.T) {
	nodes := cluster(t, 3)
	a, sa := join(t, nodes[0].addr, "alice")
	b, _ := join(t, nodes[1].addr, "bob")
	c, _ := join(t, nodes[2].addr, "carol")
	// bob and carol joined after alice and got her with the server's state; wait until every node knows everyone.
	for _, n := range nodes {
		eventually(t, "nodes to know each other's users", func() bool { return n.srv.Remote() == 2 })
	}

	a.sendRaw(wire.TypeUDPTunnel, audio(42, targetNormal))
	for _, l := range []*client{b, c} {
		got := readAudio(t, l)
		if got.SenderSession != sa || got.FrameNumber != 42 || got.GetContext() != contextNormal {
			t.Errorf("got sender %#x frame %d context %d, want sender %#x frame 42 context 0",
				got.SenderSession, got.FrameNumber, got.GetContext(), sa)
		}
	}
}

func TestFrameDeliveredOnce(t *testing.T) {
	nodes := cluster(t, 2)
	a, _ := join(t, nodes[0].addr, "alice")
	b, _ := join(t, nodes[1].addr, "bob")
	eventually(t, "node 1 to know bob", func() bool { return nodes[0].srv.Remote() == 1 })
	// The same frame twice, as from a speaker sending to two nodes during a drain.
	a.sendRaw(wire.TypeUDPTunnel, audio(7, targetNormal))
	a.sendRaw(wire.TypeUDPTunnel, audio(7, targetNormal))
	a.sendRaw(wire.TypeUDPTunnel, audio(8, targetNormal))
	if got := readAudio(t, b); got.FrameNumber != 7 {
		t.Fatalf("first frame %d", got.FrameNumber)
	}
	if got := readAudio(t, b); got.FrameNumber != 8 {
		t.Fatalf("frame 7 arrived twice, then %d", got.FrameNumber)
	}
}

func TestLeaveCrossesNodes(t *testing.T) {
	nodes := cluster(t, 2)
	a, _ := join(t, nodes[0].addr, "alice")
	b, sb := join(t, nodes[1].addr, "bob")
	a.userState(sb)
	b.conn.Close()
	var rm mumbleproto.UserRemove
	proto.Unmarshal(a.until(wire.TypeUserRemove), &rm)
	if rm.GetSession() != sb {
		t.Errorf("removed %#x, want %#x", rm.GetSession(), sb)
	}
}

func TestSelfMuteCrossesNodes(t *testing.T) {
	nodes := cluster(t, 2)
	a, _ := join(t, nodes[0].addr, "alice")
	b, sb := join(t, nodes[1].addr, "bob")
	a.userState(sb)
	b.send(&mumbleproto.UserState{SelfMute: proto.Bool(true)})
	if st := a.userState(sb); !st.GetSelfMute() {
		t.Error("alice did not see bob mute himself")
	}
}

func TestLostPeerUsersRemovedAfterGrace(t *testing.T) {
	nodes := cluster(t, 2)
	a, _ := join(t, nodes[0].addr, "alice")
	_, sb := join(t, nodes[1].addr, "bob")
	a.userState(sb)
	lost := time.Now()
	nodes[1].stop()
	var rm mumbleproto.UserRemove
	proto.Unmarshal(a.until(wire.TypeUserRemove), &rm)
	if rm.GetSession() != sb {
		t.Errorf("removed %#x, want %#x", rm.GetSession(), sb)
	}
	if d := time.Since(lost); d < 250*time.Millisecond {
		t.Errorf("bob removed after %v, before the grace period", d)
	}
}

func TestSeenDropsRepeatsOnly(t *testing.T) {
	var s seen
	if !s.first(1, 10) || !s.first(1, 11) || !s.first(2, 10) {
		t.Fatal("new frames dropped")
	}
	if s.first(1, 10) {
		t.Fatal("repeated frame passed")
	}
	s.forget(1)
	if !s.first(1, 10) {
		t.Fatal("frame of a forgotten sender dropped")
	}
}
