// Package wire frames Mumble's control channel: each message is a 2-byte big-endian type, a 4-byte big-endian length
// and a Protobuf body from Mumble.proto. The type is the position of the message in Mumble.proto.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/ruumble-project/ruumble-foundation/gen/mumble/mumbleproto"
)

// Type is a message type on the control channel.
type Type uint16

// Message types, in the order of the messages in Mumble.proto.
const (
	TypeVersion Type = iota
	TypeUDPTunnel
	TypeAuthenticate
	TypePing
	TypeReject
	TypeServerSync
	TypeChannelRemove
	TypeChannelState
	TypeUserRemove
	TypeUserState
	TypeBanList
	TypeTextMessage
	TypePermissionDenied
	TypeACL
	TypeQueryUsers
	TypeCryptSetup
	TypeContextActionModify
	TypeContextAction
	TypeUserList
	TypeVoiceTarget
	TypePermissionQuery
	TypeCodecVersion
	TypeUserStats
	TypeRequestBlob
	TypeServerConfig
	TypeSuggestConfig
	TypePluginDataTransmission
)

// MaxBody is the largest body accepted; Mumble's own limit for a control message is 8 MiB.
const MaxBody = 8 << 20

// Read reads one message. The body of a UDPTunnel message is a raw voice packet, not Protobuf.
func Read(r io.Reader) (Type, []byte, error) {
	var head [6]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	t := Type(binary.BigEndian.Uint16(head[:2]))
	n := binary.BigEndian.Uint32(head[2:])
	if n > MaxBody {
		return 0, nil, fmt.Errorf("wire: message type %d of %d bytes", t, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return t, body, nil
}

// Frame puts the header in front of a body.
func Frame(t Type, body []byte) []byte {
	out := make([]byte, 6+len(body))
	binary.BigEndian.PutUint16(out, uint16(t))
	binary.BigEndian.PutUint32(out[2:], uint32(len(body)))
	copy(out[6:], body)
	return out
}

// Encode frames a Protobuf message under its type.
func Encode(m proto.Message) ([]byte, error) {
	t, ok := TypeOf(m)
	if !ok {
		return nil, fmt.Errorf("wire: no type for %T", m)
	}
	body, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return Frame(t, body), nil
}

// TypeOf is the control channel type of a message from Mumble.proto.
func TypeOf(m proto.Message) (Type, bool) {
	switch m.(type) {
	case *mumbleproto.Version:
		return TypeVersion, true
	case *mumbleproto.Authenticate:
		return TypeAuthenticate, true
	case *mumbleproto.Ping:
		return TypePing, true
	case *mumbleproto.Reject:
		return TypeReject, true
	case *mumbleproto.ServerSync:
		return TypeServerSync, true
	case *mumbleproto.ChannelState:
		return TypeChannelState, true
	case *mumbleproto.UserRemove:
		return TypeUserRemove, true
	case *mumbleproto.UserState:
		return TypeUserState, true
	case *mumbleproto.TextMessage:
		return TypeTextMessage, true
	case *mumbleproto.PermissionDenied:
		return TypePermissionDenied, true
	case *mumbleproto.CryptSetup:
		return TypeCryptSetup, true
	case *mumbleproto.PermissionQuery:
		return TypePermissionQuery, true
	case *mumbleproto.CodecVersion:
		return TypeCodecVersion, true
	case *mumbleproto.ServerConfig:
		return TypeServerConfig, true
	}
	return 0, false
}
