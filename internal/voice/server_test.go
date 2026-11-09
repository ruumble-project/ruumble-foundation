package voice

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleproto"
	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleudp"
	"github.com/ruumble-project/ruumble-foundation/internal/certs"
	"github.com/ruumble-project/ruumble-foundation/internal/wire"
)

func start(t *testing.T) (*Server, string) {
	t.Helper()
	cert, err := certs.SelfSigned("test")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(Config{
		TLS: &tls.Config{Certificates: []tls.Certificate{cert}},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Serve(ctx, ln)
	return s, ln.Addr().String()
}

type client struct {
	t    *testing.T
	conn *tls.Conn
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn}
}

func (c *client) send(m proto.Message) {
	c.t.Helper()
	b, err := wire.Encode(m)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) sendRaw(typ wire.Type, body []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(wire.Frame(typ, body)); err != nil {
		c.t.Fatal(err)
	}
}

// until reads messages until one of type want arrives and returns its body.
func (c *client) until(want wire.Type) []byte {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		typ, body, err := wire.Read(c.conn)
		if err != nil {
			c.t.Fatalf("waiting for type %d: %v", want, err)
		}
		if typ == want {
			return body
		}
	}
}

func (c *client) hello(v2 uint64, name string) {
	c.send(&mumbleproto.Version{VersionV2: proto.Uint64(v2), Release: proto.String("test")})
	c.send(&mumbleproto.Authenticate{Username: proto.String(name), Opus: proto.Bool(true)})
}

// join connects as name and returns the session from ServerSync.
func join(t *testing.T, addr, name string) (*client, uint32) {
	t.Helper()
	c := dial(t, addr)
	c.hello(1<<48|6<<32, name)
	var sync mumbleproto.ServerSync
	if err := proto.Unmarshal(c.until(wire.TypeServerSync), &sync); err != nil {
		t.Fatal(err)
	}
	return c, sync.GetSession()
}

func audio(frame uint64, target uint32) []byte {
	b, _ := proto.Marshal(&mumbleudp.Audio{
		Header:      &mumbleudp.Audio_Target{Target: target},
		FrameNumber: frame,
		OpusData:    []byte{0xf8, 0xff, 0xfe},
	})
	return append([]byte{udpAudio}, b...)
}

func readAudio(t *testing.T, c *client) *mumbleudp.Audio {
	t.Helper()
	body := c.until(wire.TypeUDPTunnel)
	if len(body) == 0 || body[0] != udpAudio {
		t.Fatalf("not a voice packet: %x", body)
	}
	var a mumbleudp.Audio
	if err := proto.Unmarshal(body[1:], &a); err != nil {
		t.Fatal(err)
	}
	return &a
}

func TestVoiceReachesOthersWithSenderSession(t *testing.T) {
	_, addr := start(t)
	a, sa := join(t, addr, "alice")
	b, _ := join(t, addr, "bob")
	// bob's arrival tells alice that the server has registered him.
	a.until(wire.TypeUserState)

	a.sendRaw(wire.TypeUDPTunnel, audio(42, targetNormal))
	got := readAudio(t, b)
	if got.SenderSession != sa || got.FrameNumber != 42 || got.GetContext() != 0 {
		t.Errorf("got sender %d frame %d context %d, want sender %d frame 42 context 0",
			got.SenderSession, got.FrameNumber, got.GetContext(), sa)
	}
}

func TestLoopbackReturnsToSender(t *testing.T) {
	_, addr := start(t)
	a, sa := join(t, addr, "alice")
	a.sendRaw(wire.TypeUDPTunnel, audio(7, targetLoopback))
	if got := readAudio(t, a); got.FrameNumber != 7 || got.SenderSession != sa {
		t.Errorf("got %+v", got)
	}
}

func TestDeafUserHearsNothing(t *testing.T) {
	_, addr := start(t)
	a, _ := join(t, addr, "alice")
	b, sb := join(t, addr, "bob")
	a.until(wire.TypeUserState)
	b.send(&mumbleproto.UserState{SelfDeaf: proto.Bool(true)})
	// Wait until alice has seen bob go deaf, then talk.
	for {
		var st mumbleproto.UserState
		proto.Unmarshal(a.until(wire.TypeUserState), &st)
		if st.GetSession() == sb && st.GetSelfDeaf() {
			break
		}
	}
	a.sendRaw(wire.TypeUDPTunnel, audio(1, targetNormal))
	b.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	for {
		typ, _, err := wire.Read(b.conn)
		if err != nil {
			return // nothing came: right
		}
		if typ == wire.TypeUDPTunnel {
			t.Fatal("a deaf user received voice")
		}
	}
}

func TestOldClientRejected(t *testing.T) {
	_, addr := start(t)
	c := dial(t, addr)
	c.hello(1<<48|4<<32, "old")
	var r mumbleproto.Reject
	if err := proto.Unmarshal(c.until(wire.TypeReject), &r); err != nil {
		t.Fatal(err)
	}
	if r.GetType() != mumbleproto.Reject_WrongVersion {
		t.Errorf("reject type %v", r.GetType())
	}
}

func TestOldClientWithOnlyV1Rejected(t *testing.T) {
	_, addr := start(t)
	c := dial(t, addr)
	c.send(&mumbleproto.Version{VersionV1: proto.Uint32(1<<16 | 4<<8)})
	c.send(&mumbleproto.Authenticate{Username: proto.String("old")})
	var r mumbleproto.Reject
	proto.Unmarshal(c.until(wire.TypeReject), &r)
	if r.GetType() != mumbleproto.Reject_WrongVersion {
		t.Errorf("reject type %v", r.GetType())
	}
}

func TestNameInUseRejected(t *testing.T) {
	_, addr := start(t)
	join(t, addr, "alice")
	c := dial(t, addr)
	c.hello(1<<48|6<<32, "alice")
	var r mumbleproto.Reject
	proto.Unmarshal(c.until(wire.TypeReject), &r)
	if r.GetType() != mumbleproto.Reject_UsernameInUse {
		t.Errorf("reject type %v", r.GetType())
	}
}

func TestPingAnswered(t *testing.T) {
	_, addr := start(t)
	a, _ := join(t, addr, "alice")
	a.send(&mumbleproto.Ping{Timestamp: proto.Uint64(4711)})
	var p mumbleproto.Ping
	proto.Unmarshal(a.until(wire.TypePing), &p)
	if p.GetTimestamp() != 4711 {
		t.Errorf("timestamp %d", p.GetTimestamp())
	}
}

func TestLeaveIsAnnounced(t *testing.T) {
	s, addr := start(t)
	a, _ := join(t, addr, "alice")
	b, sb := join(t, addr, "bob")
	a.until(wire.TypeUserState)
	b.conn.Close()
	var rm mumbleproto.UserRemove
	proto.Unmarshal(a.until(wire.TypeUserRemove), &rm)
	if rm.GetSession() != sb {
		t.Errorf("removed %d, want %d", rm.GetSession(), sb)
	}
	if n := s.Users(); n != 1 {
		t.Errorf("%d users left", n)
	}
}
