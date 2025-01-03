package queue

import (
	"bytes"
	"errors"
	"slices"
	"strconv"
	"strings"

	zmq "github.com/go-zeromq/zmq4"
	"github.com/rs/zerolog/log"
	"orctom.com/rmq/internal/utils"
)

const (
	MAGIC = "<RMQ>"
	SEP   = "\r\n"

	GET_OP  = byte(1)
	PUT_OP  = byte(2)
	ACK_OP  = byte(3)
	STAT_OP = byte(4)
)

var (
	ERR_MAGIC   = errors.New("invalid magic")
	ERR_OP      = errors.New("invalid op")
	ERR_SUCCESS = errors.New("invalid success")
)

const ()

func Int2Bytes(i int) []byte {
	bytes := make([]byte, 4)
	utils.ORDER.PutUint32(bytes, uint32(i))
	return bytes
}

type Request struct {
	OP   byte
	Head string
	Data []byte
}

type Msg struct {
	Magic   string
	OP      byte
	Success bool
	Payload []byte
}

func NewMsg(op byte, payload []byte) *Msg {
	return &Msg{
		Magic:   MAGIC,
		OP:      op,
		Success: true,
		Payload: payload,
	}
}

func NewMsgErr(op byte, err error) *Msg {
	return &Msg{
		Magic:   MAGIC,
		OP:      op,
		Success: false,
		Payload: []byte(err.Error()),
	}
}

func (msg *Msg) Encode() []byte {
	var buf bytes.Buffer
	buf.WriteString(MAGIC) // 5 bytes [:5]
	buf.WriteByte(msg.OP)  // 1 byte  [5]
	if msg.Success {       // 1 byte  [6]
		buf.Write([]byte{1})
	} else {
		buf.Write([]byte{0})
	}
	buf.Write(msg.Payload)
	return buf.Bytes()
}

func DecodeMsg(bytes []byte) (*Msg, error) {
	var magic = string(bytes[:5])
	if magic != MAGIC {
		return nil, ERR_MAGIC
	}
	var op = bytes[5]
	if int(op) > 4 {
		return nil, ERR_OP
	}
	var success = int(bytes[6])
	if success != 0 && success != 1 {
		return nil, ERR_SUCCESS
	}
	msg := NewMsg(op, bytes[7:])
	if success == 0 {
		msg.Success = false
	}
	return msg, nil
}

// ================ get ================

func GetRequest(queue string) []byte {
	return NewMsg(GET_OP, []byte(queue)).Encode()
}

func GetResponse(msg *Message) []byte {
	head := []byte(msg.Priority.S() + SEP + msg.ID.S())
	payload := slices.Concat(
		Int2Bytes(len(head)),
		head,
		msg.Data,
	)
	return NewMsg(GET_OP, payload).Encode()
}

func handleGet(payload []byte) zmq.Msg {
	queue := strings.TrimSpace(string(payload))
	msg := RMQ().GetQueue(queue).Get()
	return zmq.NewMsg(GetResponse(msg))
}

// =============== put ================

func PutRequest(queue string, priority Priority, data []byte) []byte {
	head := []byte(queue + SEP + priority.S())
	payload := slices.Concat(
		Int2Bytes(len(head)),
		head,
		data,
	)
	return NewMsg(PUT_OP, payload).Encode()
}

func PutResponse(e error) []byte {
	if e == nil {
		return NewMsg(PUT_OP, Int2Bytes(0)).Encode()
	} else {
		return NewMsgErr(PUT_OP, e).Encode()
	}
}

func handlePut(payload []byte) zmq.Msg {
	headLen := utils.ORDER.Uint32(payload[:4])
	items := strings.Split(string(payload[4:4+headLen]), SEP)
	queue := items[0]
	priority := ParsePriority(items[1])
	data := payload[4+headLen:]

	err := RMQ().GetQueue(queue).Put(data, priority)
	return zmq.NewMsg(PutResponse(err))
}

// =============== ack ================

func AckRequest(queue string, priority Priority, id ID) []byte {
	payload := []byte(queue + SEP + priority.S() + SEP + id.S())
	return NewMsg(ACK_OP, payload).Encode()
}

func AckResponse(e error) []byte {
	if e == nil {
		return NewMsg(ACK_OP, []byte{}).Encode()
	} else {
		return NewMsgErr(ACK_OP, e).Encode()
	}
}

func handleAck(payload []byte) zmq.Msg {
	items := strings.Split(string(payload), SEP)
	queue := items[0]
	priority := ParsePriority(items[1])
	id, err := strconv.Atoi(items[2])
	if err != nil {
		return zmq.NewMsg(AckResponse(err))
	} else {
		RMQ().GetQueue(queue).Ack(priority, ID(id))
		return zmq.NewMsg(AckResponse(err))
	}
}

// =============== stat ================

func StatRequest(queue string) []byte {
	return NewMsg(STAT_OP, []byte(queue)).Encode()
}

func StatResponse(queue string) []byte {
	var stats map[string]*Stats
	if queue == "" {
		stats = RMQ().Stats()
	} else {
		stats = map[string]*Stats{
			queue: RMQ().GetQueue(queue).Stats(),
		}
	}

	var buf bytes.Buffer
	buf.Write(Int2Bytes(len(stats)))

	for q, stat := range stats {
		buf.Write(Int2Bytes(len(q)))
		buf.WriteString(q)
		statBytes := stat.Bytes()
		buf.Write((Int2Bytes(len(statBytes))))
		buf.Write(statBytes)
	}
	payload := buf.Bytes()
	return NewMsg(PUT_OP, payload).Encode()
}

func handleStat(payload []byte) zmq.Msg {
	queue := strings.TrimSpace(string(payload))
	return zmq.NewMsg(StatResponse(queue))
}

// ================ handler ================

func HandleRequest(bytes []byte) zmq.Msg {
	if string(bytes[:5]) != MAGIC {
		log.Info().Msgf("%v vs %v", bytes[:5], MAGIC)
		log.Info().Msgf("invalid request: %v", bytes)
		return zmq.NewMsgString("invalid request") // invalid request, so do not obey the protocol
	}

	op := bytes[5]
	payload := bytes[7:]
	switch op {
	case GET_OP:
		return handleGet(payload)
	case PUT_OP:
		return handlePut(payload)
	case ACK_OP:
		return handleAck(payload)
	case STAT_OP:
		return handleStat(payload)
	default:
		log.Info().Msgf("unknown request op %v", op)
		return zmq.NewMsg(NewMsgErr(op, ERR_OP).Encode())
	}
}
