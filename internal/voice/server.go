// Package voice is the side of a node that Mumble clients talk to: it accepts their TLS connections, runs the
// handshake, keeps the list of users in step and forwards voice between them.
//
// This first cut has one channel (the root), no stored state, and voice tunnelled over TCP only; UDP with OCB2
// follows. Clients older than Mumble 1.5 are turned away (ADR-0030), so voice is always in the Protobuf format of
// MumbleUDP.proto.
package voice

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleproto"
	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleudp"
	"github.com/ruumble-project/ruumble-foundation/internal/wire"
)

// Version 1.6.0 in the v2 encoding (major << 48 | minor << 32 | patch << 16), which is what the node announces.
const announced = 1<<48 | 6<<32

// Oldest accepted client: Mumble 1.5.0 (ADR-0030).
const oldest = 1<<48 | 5<<32

const (
	// A client that sends nothing for this long is gone. Mumble clients ping every few seconds.
	idleTimeout = 30 * time.Second
	// Time a client has to finish the handshake.
	handshakeTimeout = 10 * time.Second
	writeTimeout     = 10 * time.Second
	// Messages queued for one client; a client this far behind loses voice, then the connection.
	queueLen = 512
)

// Permissions granted in the root channel for now: traverse, enter, speak, whisper, text message.
const permissions = 0x2 | 0x4 | 0x8 | 0x100 | 0x200

// First byte of a voice packet in the Protobuf format.
const udpAudio = 0

// Audio targets a client may send to.
const (
	targetNormal   = 0
	targetLoopback = 31
)

// Config sets up a server.
type Config struct {
	TLS     *tls.Config
	Welcome string
	// MaxBandwidth announced to clients, in bits per second.
	MaxBandwidth uint32
	Log          *slog.Logger
}

// Server holds the users connected to this node.
type Server struct {
	cfg Config
	log *slog.Logger

	mu    sync.Mutex
	next  uint32
	users map[uint32]*user
}

// New makes a server; Serve starts it.
func New(cfg Config) *Server {
	if cfg.MaxBandwidth == 0 {
		cfg.MaxBandwidth = 558000
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, log: log, users: map[uint32]*user{}}
}

// Serve accepts clients on ln until ctx ends.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handle(ctx, tls.Server(c, s.cfg.TLS))
	}
}

// Users is the number of connected users.
func (s *Server) Users() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.users)
}

type user struct {
	session uint32
	name    string
	channel uint32
	mute    bool
	deaf    bool

	out  chan []byte
	done chan struct{}
	once sync.Once
	conn net.Conn
}

func (u *user) close() {
	u.once.Do(func() {
		close(u.done)
		u.conn.Close()
	})
}

// queue hands a framed message to the writer. Voice is dropped when the client is behind; anything else that does
// not fit ends the connection, since a client that misses state is out of step.
func (u *user) queue(b []byte, voice bool) {
	select {
	case u.out <- b:
	default:
		if !voice {
			u.close()
		}
	}
}

func (u *user) send(m proto.Message) {
	b, err := wire.Encode(m)
	if err != nil {
		panic(err) // only messages of Mumble.proto are sent
	}
	u.queue(b, false)
}

func (u *user) write() {
	for {
		select {
		case <-u.done:
			return
		case b := <-u.out:
			u.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := u.conn.Write(b); err != nil {
				u.close()
				return
			}
		}
	}
}

func (u *user) state() *mumbleproto.UserState {
	return &mumbleproto.UserState{
		Session:   proto.Uint32(u.session),
		Name:      proto.String(u.name),
		ChannelId: proto.Uint32(u.channel),
		SelfMute:  proto.Bool(u.mute),
		SelfDeaf:  proto.Bool(u.deaf),
	}
}

var errRejected = errors.New("rejected")

func (s *Server) handle(ctx context.Context, conn *tls.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(handshakeTimeout))
	if err := conn.HandshakeContext(ctx); err != nil {
		return
	}
	u := &user{out: make(chan []byte, queueLen), done: make(chan struct{}), conn: conn}
	go u.write()
	defer u.close()

	u.send(&mumbleproto.Version{
		VersionV1: proto.Uint32(1<<16 | 6<<8),
		VersionV2: proto.Uint64(announced),
		Release:   proto.String("Ruumble Foundation"),
	})
	name, err := s.handshake(conn, u)
	if err != nil {
		if !errors.Is(err, errRejected) {
			s.log.Debug("handshake failed", "remote", conn.RemoteAddr(), "err", err)
		}
		// Let the writer deliver the reject before the connection closes.
		time.Sleep(100 * time.Millisecond)
		return
	}
	u.name = name
	if !s.join(u) {
		time.Sleep(100 * time.Millisecond)
		return
	}
	defer s.leave(u)
	s.log.Info("user joined", "session", u.session, "name", u.name, "remote", conn.RemoteAddr())

	for {
		conn.SetReadDeadline(time.Now().Add(idleTimeout))
		t, body, err := wire.Read(conn)
		if err != nil {
			return
		}
		s.dispatch(u, t, body)
	}
}

// handshake reads Version and Authenticate and checks them. It returns the user name.
func (s *Server) handshake(conn net.Conn, u *user) (string, error) {
	var version uint64
	for {
		t, body, err := wire.Read(conn)
		if err != nil {
			return "", err
		}
		switch t {
		case wire.TypeVersion:
			var v mumbleproto.Version
			if err := proto.Unmarshal(body, &v); err != nil {
				return "", err
			}
			version = v.GetVersionV2()
			if version == 0 {
				v1 := uint64(v.GetVersionV1())
				version = (v1>>16)<<48 | (v1>>8&0xff)<<32 | (v1&0xff)<<16
			}
		case wire.TypeAuthenticate:
			var a mumbleproto.Authenticate
			if err := proto.Unmarshal(body, &a); err != nil {
				return "", err
			}
			if version < oldest {
				return "", s.reject(u, mumbleproto.Reject_WrongVersion, "Ruumble needs Mumble 1.5 or later.")
			}
			name := a.GetUsername()
			if name == "" || len(name) > 128 {
				return "", s.reject(u, mumbleproto.Reject_InvalidUsername, "Please choose a user name.")
			}
			return name, nil
		}
		// Anything else before authentication is ignored, as pings may come early.
	}
}

func (s *Server) reject(u *user, t mumbleproto.Reject_RejectType, reason string) error {
	u.send(&mumbleproto.Reject{Type: t.Enum(), Reason: proto.String(reason)})
	return fmt.Errorf("%w: %s", errRejected, reason)
}

// join registers the user and sends it the server's state. Both happen under the lock, so the user's queue sees
// every later change after the state it starts from.
func (s *Server) join(u *user) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.users {
		if o.name == u.name {
			s.reject(u, mumbleproto.Reject_UsernameInUse, "That user name is already in use.")
			return false
		}
	}
	s.next++
	u.session = s.next
	s.users[u.session] = u

	u.send(&mumbleproto.CodecVersion{Alpha: proto.Int32(-2147483637), Beta: proto.Int32(0), PreferAlpha: proto.Bool(true), Opus: proto.Bool(true)})
	u.send(&mumbleproto.ChannelState{ChannelId: proto.Uint32(0), Name: proto.String("Root"), Position: proto.Int32(0)})
	for _, o := range s.users {
		u.send(o.state())
	}
	u.send(&mumbleproto.ServerSync{
		Session:      proto.Uint32(u.session),
		MaxBandwidth: proto.Uint32(s.cfg.MaxBandwidth),
		WelcomeText:  proto.String(s.cfg.Welcome),
		Permissions:  proto.Uint64(permissions),
	})
	u.send(&mumbleproto.ServerConfig{
		MaxBandwidth:       proto.Uint32(s.cfg.MaxBandwidth),
		AllowHtml:          proto.Bool(true),
		MessageLength:      proto.Uint32(5000),
		ImageMessageLength: proto.Uint32(131072),
	})
	st := u.state()
	for _, o := range s.users {
		if o != u {
			o.send(st)
		}
	}
	return true
}

func (s *Server) leave(u *user) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, u.session)
	rm := &mumbleproto.UserRemove{Session: proto.Uint32(u.session)}
	for _, o := range s.users {
		o.send(rm)
	}
	s.log.Info("user left", "session", u.session, "name", u.name)
}

func (s *Server) dispatch(u *user, t wire.Type, body []byte) {
	switch t {
	case wire.TypePing:
		var p mumbleproto.Ping
		if proto.Unmarshal(body, &p) == nil {
			u.send(&mumbleproto.Ping{Timestamp: proto.Uint64(p.GetTimestamp())})
		}
	case wire.TypeUDPTunnel:
		s.voice(u, body)
	case wire.TypeUserState:
		var st mumbleproto.UserState
		if proto.Unmarshal(body, &st) == nil {
			s.userState(u, &st)
		}
	case wire.TypePermissionQuery:
		var q mumbleproto.PermissionQuery
		if proto.Unmarshal(body, &q) == nil {
			u.send(&mumbleproto.PermissionQuery{ChannelId: proto.Uint32(q.GetChannelId()), Permissions: proto.Uint32(permissions)})
		}
	}
	// Other messages are not supported yet and ignored.
}

// userState applies what a client may change about itself: self mute and self deaf. Moving between channels comes
// with channels.
func (s *Server) userState(u *user, st *mumbleproto.UserState) {
	if st.Session != nil && st.GetSession() != u.session {
		u.send(&mumbleproto.PermissionDenied{Type: mumbleproto.PermissionDenied_Permission.Enum(), Session: proto.Uint32(st.GetSession())})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &mumbleproto.UserState{Session: proto.Uint32(u.session), Actor: proto.Uint32(u.session)}
	changed := false
	if st.SelfDeaf != nil {
		u.deaf = st.GetSelfDeaf()
		if u.deaf {
			u.mute = true
		}
		out.SelfDeaf = proto.Bool(u.deaf)
		out.SelfMute = proto.Bool(u.mute)
		changed = true
	}
	if st.SelfMute != nil {
		u.mute = st.GetSelfMute()
		if !u.mute {
			u.deaf = false
			out.SelfDeaf = proto.Bool(false)
		}
		out.SelfMute = proto.Bool(u.mute)
		changed = true
	}
	if !changed {
		return
	}
	for _, o := range s.users {
		o.send(out)
	}
}

// voice forwards one voice packet from u. Normal talking reaches everyone else in u's channel who is not deaf;
// server loopback goes back to u.
func (s *Server) voice(u *user, packet []byte) {
	if len(packet) == 0 || packet[0] != udpAudio {
		return // pings over the tunnel and the legacy format are not forwarded
	}
	var a mumbleudp.Audio
	if proto.Unmarshal(packet[1:], &a) != nil {
		return
	}
	target := a.GetTarget()
	if target != targetNormal && target != targetLoopback {
		return // whisper and shout come with voice targets
	}
	a.Header = &mumbleudp.Audio_Context{Context: 0}
	a.SenderSession = u.session
	body, err := proto.Marshal(&a)
	if err != nil {
		return
	}
	frame := wire.Frame(wire.TypeUDPTunnel, append([]byte{udpAudio}, body...))

	if target == targetLoopback {
		u.queue(frame, true)
		return
	}
	s.mu.Lock()
	mute := u.mute
	var to []*user
	if !mute {
		for _, o := range s.users {
			if o != u && o.channel == u.channel && !o.deaf {
				to = append(to, o)
			}
		}
	}
	s.mu.Unlock()
	for _, o := range to {
		o.queue(frame, true)
	}
}
