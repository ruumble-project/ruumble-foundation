package voice

import (
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleproto"
	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleudp"
	nodev1 "github.com/ruumble-project/ruumble-foundation/gen/ruumble/node/v1"
	"github.com/ruumble-project/ruumble-foundation/internal/wire"
)

// Interconnect is what the server needs from the mesh (ADR-0032). Announce and Gone are called with the server's
// lock held, so peers see changes in the order local clients do.
type Interconnect interface {
	Announce(s *nodev1.Session)
	Gone(session uint32, claim uint64, reason nodev1.SessionGone_Reason)
	Forward(v *nodev1.Voice)
}

// Audio contexts a node sets on voice it delivers.
const (
	contextNormal  = 0
	contextShout   = 1
	contextWhisper = 2
)

// remote is a session homed on another node.
type remote struct {
	node uint32
	s    *nodev1.Session
}

func (r *remote) state() *mumbleproto.UserState {
	s := r.s
	st := &mumbleproto.UserState{
		Session:         proto.Uint32(s.Session),
		Name:            proto.String(s.Name),
		ChannelId:       proto.Uint32(s.ChannelId),
		Mute:            proto.Bool(s.Mute),
		Deaf:            proto.Bool(s.Deaf),
		Suppress:        proto.Bool(s.Suppress),
		SelfMute:        proto.Bool(s.SelfMute),
		SelfDeaf:        proto.Bool(s.SelfDeaf),
		PrioritySpeaker: proto.Bool(s.PrioritySpeaker),
		Recording:       proto.Bool(s.Recording),
	}
	if s.UserId != nil {
		st.UserId = proto.Uint32(s.GetUserId())
	}
	if s.CertHash != "" {
		st.Hash = proto.String(s.CertHash)
	}
	return st
}

// wins says whether a claim (aClaim from node aNode) beats another: the higher claim, with equal claims the lower
// node number (ADR-0032).
func wins(aClaim uint64, aNode uint32, bClaim uint64, bNode uint32) bool {
	return aClaim > bClaim || (aClaim == bClaim && aNode < bNode)
}

// allocate hands out the next free session id: the node number in the top 8 bits, a counter below that started at a
// random value. Ids in use here or anywhere in the cluster are skipped. Called with s.mu held.
func (s *Server) allocate() uint32 {
	for range 1 << 24 {
		s.counter = (s.counter + 1) & 0xffffff
		if s.counter == 0 {
			continue
		}
		id := s.cfg.Node<<24 | s.counter
		if s.users[id] == nil && s.remote[id] == nil {
			return id
		}
	}
	panic("no free session id")
}

// announce passes u's state to the mesh. Called with s.mu held.
func (s *Server) announce(u *user) {
	if s.cfg.Mesh == nil {
		return
	}
	s.cfg.Mesh.Announce(&nodev1.Session{
		Session:       u.session,
		Claim:         u.claim,
		CertHash:      u.certHash,
		Name:          u.name,
		ChannelId:     u.channel,
		SelfMute:      u.mute,
		SelfDeaf:      u.deaf,
		ClientVersion: u.version,
	})
}

// broadcast sends m to every local user. Called with s.mu held.
func (s *Server) broadcast(m proto.Message) {
	for _, o := range s.users {
		o.send(m)
	}
}

// PeerSync replaces everything known about node's sessions.
func (s *Server) PeerSync(node uint32, sync *nodev1.Sync) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.grace[node]; t != nil {
		t.Stop()
		delete(s.grace, node)
	}
	keep := map[uint32]bool{}
	for _, ses := range sync.Sessions {
		keep[ses.Session] = true
		s.applyRemote(node, ses)
	}
	for id, r := range s.remote {
		if r.node == node && !keep[id] {
			s.dropRemote(id)
		}
	}
	s.log.Info("peer synced", "node", node, "sessions", len(sync.Sessions))
}

// PeerSession takes a new or changed session homed on node.
func (s *Server) PeerSession(node uint32, ses *nodev1.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyRemote(node, ses)
}

// applyRemote records a session homed on node unless a better claim to it is known. Called with s.mu held.
func (s *Server) applyRemote(node uint32, ses *nodev1.Session) {
	if u := s.users[ses.Session]; u != nil {
		if !wins(ses.Claim, node, u.claim, s.cfg.Node) {
			return
		}
		// Another node holds a better claim to a session served here: stop serving it (ADR-0032).
		s.log.Info("session claimed by another node", "session", ses.Session, "node", node)
		u.replaced = true
		u.close()
	}
	if r := s.remote[ses.Session]; r != nil && r.node != node && !wins(ses.Claim, node, r.s.Claim, r.node) {
		return
	}
	r := &remote{node: node, s: ses}
	s.remote[ses.Session] = r
	s.broadcast(r.state())
}

// dropRemote forgets a remote session and tells local clients. Called with s.mu held.
func (s *Server) dropRemote(id uint32) {
	delete(s.remote, id)
	s.seen.forget(id)
	s.broadcast(&mumbleproto.UserRemove{Session: proto.Uint32(id)})
}

// PeerSessionGone removes a session node no longer serves. A session that moved stays visible for the grace period,
// since its new node may announce it a moment later.
func (s *Server) PeerSessionGone(node uint32, g *nodev1.SessionGone) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.remote[g.Session]
	if r == nil || r.node != node || r.s.Claim > g.Claim {
		return
	}
	if g.Reason == nodev1.SessionGone_REASON_MOVED {
		time.AfterFunc(s.cfg.Grace, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.remote[g.Session] == r {
				s.dropRemote(g.Session)
			}
		})
		return
	}
	s.dropRemote(g.Session)
}

// PeerStatus notes a peer's state; nothing depends on it yet.
func (s *Server) PeerStatus(node uint32, st *nodev1.NodeStatus) {
	s.log.Info("peer status", "node", node, "state", st.State)
}

// PeerUserCommand would apply a change made elsewhere to a local user. Moving and muting others come with channels
// and permissions; until then there is nothing a peer may send.
func (s *Server) PeerUserCommand(node uint32, c *nodev1.UserCommand) {
	s.log.Debug("user command ignored", "node", node, "session", c.Session)
}

// PeerDeliver would hand text and relays to local clients; text messages are not supported yet.
func (s *Server) PeerDeliver(node uint32, d *nodev1.Deliver) {
	s.log.Debug("delivery ignored", "node", node, "from", d.FromSession)
}

// PeerDown keeps node's sessions for the grace period: Ruumble Clients reappear elsewhere with their ids, plain ones
// reconnect. Whatever is still attributed to node afterwards is removed.
func (s *Server) PeerDown(node uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grace[node] != nil {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(s.cfg.Grace, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.grace[node] != t {
			return
		}
		delete(s.grace, node)
		n := 0
		for id, r := range s.remote {
			if r.node == node {
				s.dropRemote(id)
				n++
			}
		}
		s.log.Info("peer given up", "node", node, "sessions removed", n)
	})
	s.grace[node] = t
}

// PeerVoice delivers a frame forwarded by node to the local clients its route reaches.
func (s *Server) PeerVoice(node uint32, v *nodev1.Voice) {
	var a mumbleudp.Audio
	if proto.Unmarshal(v.Audio, &a) != nil || a.SenderSession == 0 {
		return
	}
	if !s.seen.first(a.SenderSession, a.FrameNumber) {
		return
	}
	type target struct {
		u   *user
		ctx uint32
	}
	var to []target
	s.mu.Lock()
	switch r := v.Route.(type) {
	case *nodev1.Voice_ChannelId:
		for _, o := range s.users {
			if o.session != a.SenderSession && o.channel == r.ChannelId && !o.deaf {
				to = append(to, target{o, contextNormal})
			}
		}
	case *nodev1.Voice_Whisper:
		picked := map[*user]bool{}
		for _, id := range r.Whisper.Sessions {
			if o := s.users[id]; o != nil && !o.deaf && !picked[o] {
				picked[o] = true
				to = append(to, target{o, contextWhisper})
			}
		}
		for _, ch := range r.Whisper.ChannelIds {
			for _, o := range s.users {
				if o.channel == ch && !o.deaf && !picked[o] && o.session != a.SenderSession {
					picked[o] = true
					to = append(to, target{o, contextShout})
				}
			}
		}
	}
	s.mu.Unlock()

	frames := map[uint32][]byte{}
	for _, t := range to {
		f, ok := frames[t.ctx]
		if !ok {
			a.Header = &mumbleudp.Audio_Context{Context: t.ctx}
			body, err := proto.Marshal(&a)
			if err != nil {
				return
			}
			f = wire.Frame(wire.TypeUDPTunnel, append([]byte{udpAudio}, body...))
			frames[t.ctx] = f
		}
		t.u.queue(f, true)
	}
}

// seen remembers the last frames of every sender, so a frame that arrives twice (from the speaker and forwarded by
// another node, or from two nodes during a drain) is delivered once (ADR-0032).
type seen struct {
	mu      sync.Mutex
	senders map[uint32]*recent
}

const (
	recentFrames = 64
	recentFor    = 2 * time.Second
)

type recent struct {
	frame [recentFrames]uint64
	at    [recentFrames]time.Time
	next  int
}

// first says whether frame of sender is new, and records it.
func (s *seen) first(sender uint32, frame uint64) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.senders == nil {
		s.senders = map[uint32]*recent{}
	}
	r := s.senders[sender]
	if r == nil {
		r = &recent{}
		s.senders[sender] = r
	}
	for i := range recentFrames {
		if r.frame[i] == frame && !r.at[i].IsZero() && now.Sub(r.at[i]) < recentFor {
			return false
		}
	}
	r.frame[r.next] = frame
	r.at[r.next] = now
	r.next = (r.next + 1) % recentFrames
	return true
}

func (s *seen) forget(sender uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.senders, sender)
}

// randomCounter is where a node's session counter starts.
func randomCounter() uint32 { return rand.Uint32N(1 << 24) }
