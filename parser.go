// Copyright 2012-2014 Apcera Inc. All rights reserved.

package nats

import (
	"fmt"
)

type msgArg struct {
	subject []byte
	reply   []byte
	sid     int64
	size    int
}

const MAX_CONTROL_LINE_SIZE = 1024

type parseState struct {
	state   int
	as      int
	drop    int
	ma      msgArg
	argBuf  []byte
	msgBuf  []byte
	scratch [MAX_CONTROL_LINE_SIZE]byte
}

const (
	OP_START = iota
	OP_PLUS
	OP_PLUS_O
	OP_PLUS_OK
	OP_MINUS
	OP_MINUS_E
	OP_MINUS_ER
	OP_MINUS_ERR
	OP_MINUS_ERR_SPC
	MINUS_ERR_ARG
	OP_C
	OP_CO
	OP_CON
	OP_CONN
	OP_CONNE
	OP_CONNEC
	OP_CONNECT
	CONNECT_ARG
	OP_M
	OP_MS
	OP_MSG
	OP_MSG_SPC
	MSG_ARG
	MSG_PAYLOAD
	MSG_END
	OP_P
	OP_PI
	OP_PIN
	OP_PING
	OP_PO
	OP_PON
	OP_PONG
)

// parse is the fast protocol parser engine.
func (nc *Conn) parse(buf []byte) error {
	var i int
	var b byte

	for i, b = range buf {
		switch nc.ps.state {
		case OP_START:
			switch b {
			case 'M', 'm':
				nc.ps.state = OP_M
			case 'C', 'c':
				nc.ps.state = OP_C
			case 'P', 'p':
				nc.ps.state = OP_P
			case '+':
				nc.ps.state = OP_PLUS
			case '-':
				nc.ps.state = OP_MINUS
			default:
				goto parseErr
			}
		case OP_M:
			switch b {
			case 'S', 's':
				nc.ps.state = OP_MS
			default:
				goto parseErr
			}
		case OP_MS:
			switch b {
			case 'G', 'g':
				nc.ps.state = OP_MSG
			default:
				goto parseErr
			}
		case OP_MSG:
			switch b {
			case ' ', '\t':
				nc.ps.state = OP_MSG_SPC
			default:
				goto parseErr
			}
		case OP_MSG_SPC:
			switch b {
			case ' ', '\t':
				continue
			default:
				nc.ps.state = MSG_ARG
				nc.ps.as = i
			}
		case MSG_ARG:
			switch b {
			case '\r':
				nc.ps.drop = 1
			case '\n':
				var arg []byte
				if nc.ps.argBuf != nil {
					arg = nc.ps.argBuf
				} else {
					arg = buf[nc.ps.as : i-nc.ps.drop]
				}
				if err := nc.processMsgArgs(arg); err != nil {
					return err
				}
				nc.ps.drop, nc.ps.as, nc.ps.state = 0, i+1, MSG_PAYLOAD
			default:
				if nc.ps.argBuf != nil {
					nc.ps.argBuf = append(nc.ps.argBuf, b)
				}
			}
		case MSG_PAYLOAD:
			if nc.ps.msgBuf != nil {
				if len(nc.ps.msgBuf) >= nc.ps.ma.size {
					nc.processMsg(nc.ps.msgBuf)
					nc.ps.argBuf, nc.ps.msgBuf, nc.ps.state = nil, nil, MSG_END
				} else {
					nc.ps.msgBuf = append(nc.ps.msgBuf, b)
				}
			} else if i-nc.ps.as >= nc.ps.ma.size {
				nc.processMsg(buf[nc.ps.as:i])
				nc.ps.argBuf, nc.ps.msgBuf, nc.ps.state = nil, nil, MSG_END
			}
		case MSG_END:
			switch b {
			case '\n':
				nc.ps.drop, nc.ps.as, nc.ps.state = 0, i+1, OP_START
			default:
				continue
			}
		case OP_PLUS:
			switch b {
			case 'O', 'o':
				nc.ps.state = OP_PLUS_O
			default:
				goto parseErr
			}
		case OP_PLUS_O:
			switch b {
			case 'K', 'k':
				nc.ps.state = OP_PLUS_OK
			default:
				goto parseErr
			}
		case OP_PLUS_OK:
			switch b {
			case '\n':
				nc.processOK()
				nc.ps.drop, nc.ps.state = 0, OP_START
			}
		case OP_MINUS:
			switch b {
			case 'E', 'e':
				nc.ps.state = OP_MINUS_E
			default:
				goto parseErr
			}
		case OP_MINUS_E:
			switch b {
			case 'R', 'r':
				nc.ps.state = OP_MINUS_ER
			default:
				goto parseErr
			}
		case OP_MINUS_ER:
			switch b {
			case 'R', 'r':
				nc.ps.state = OP_MINUS_ERR
			default:
				goto parseErr
			}
		case OP_MINUS_ERR:
			switch b {
			case ' ', '\t':
				nc.ps.state = OP_MINUS_ERR_SPC
			default:
				goto parseErr
			}
		case OP_MINUS_ERR_SPC:
			switch b {
			case ' ', '\t':
				continue
			default:
				nc.ps.state = MINUS_ERR_ARG
				nc.ps.as = i
			}
		case MINUS_ERR_ARG:
			switch b {
			case '\r':
				nc.ps.drop = 1
			case '\n':
				var arg []byte
				if nc.ps.argBuf != nil {
					arg = nc.ps.argBuf
					nc.ps.argBuf = nil
				} else {
					arg = buf[nc.ps.as : i-nc.ps.drop]
				}
				nc.processErr(string(arg))
				nc.ps.drop, nc.ps.as, nc.ps.state = 0, i+1, OP_START
			default:
				if nc.ps.argBuf != nil {
					nc.ps.argBuf = append(nc.ps.argBuf, b)
				}
			}
		case OP_P:
			switch b {
			case 'I', 'i':
				nc.ps.state = OP_PI
			case 'O', 'o':
				nc.ps.state = OP_PO
			default:
				goto parseErr
			}
		case OP_PO:
			switch b {
			case 'N', 'n':
				nc.ps.state = OP_PON
			default:
				goto parseErr
			}
		case OP_PON:
			switch b {
			case 'G', 'g':
				nc.ps.state = OP_PONG
			default:
				goto parseErr
			}
		case OP_PONG:
			switch b {
			case '\n':
				nc.processPong()
				nc.ps.drop, nc.ps.state = 0, OP_START
			}
		case OP_PI:
			switch b {
			case 'N', 'n':
				nc.ps.state = OP_PIN
			default:
				goto parseErr
			}
		case OP_PIN:
			switch b {
			case 'G', 'g':
				nc.ps.state = OP_PING
			default:
				goto parseErr
			}
		case OP_PING:
			switch b {
			case '\n':
				nc.processPing()
				nc.ps.drop, nc.ps.state = 0, OP_START
			}
		default:
			goto parseErr
		}
	}
	// Check for split buffer scenarios
	if (nc.ps.state == MSG_ARG || nc.ps.state == MINUS_ERR_ARG) && nc.ps.argBuf == nil {
		nc.ps.argBuf = nc.ps.scratch[:0]
		nc.ps.argBuf = append(nc.ps.argBuf, buf[nc.ps.as:(i+1)-nc.ps.drop]...)
		// FIXME, check max len
	}
	// Check for split msg
	if nc.ps.state == MSG_PAYLOAD && nc.ps.msgBuf == nil {
		// We need to clone the msgArg if it is still referencing the
		// read buffer and we are not able to process the msg.
		if nc.ps.argBuf == nil {
			nc.cloneMsgArg()
		}
		// FIXME: copy better here? Make whole buf if large?
		nc.ps.msgBuf = nc.ps.scratch[len(nc.ps.argBuf):len(nc.ps.argBuf)]
		nc.ps.msgBuf = append(nc.ps.msgBuf, (buf[nc.ps.as:])...)
	}

	return nil

parseErr:
	return fmt.Errorf("nats: Parse Error [%d]: '%s'", nc.ps.state, buf[i:])
}

// cloneMsgArg is used when the split buffer scenario has the pubArg in the existing read buffer, but
// we need to hold onto it into the next read.
func (nc *Conn) cloneMsgArg() {
	nc.ps.argBuf = nc.ps.scratch[:0]
	nc.ps.argBuf = append(nc.ps.argBuf, nc.ps.ma.subject...)
	nc.ps.argBuf = append(nc.ps.argBuf, nc.ps.ma.reply...)
	nc.ps.ma.subject = nc.ps.argBuf[:len(nc.ps.ma.subject)]
	if nc.ps.ma.reply != nil {
		nc.ps.ma.reply = nc.ps.argBuf[len(nc.ps.ma.subject):]
	}
}

const argsLenMax = 4

func (nc *Conn) processMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [argsLenMax][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	switch len(args) {
	case 3:
		nc.ps.ma.subject = args[0]
		nc.ps.ma.sid = parseInt64(args[1])
		nc.ps.ma.reply = nil
		nc.ps.ma.size = int(parseInt64(args[2]))
	case 4:
		nc.ps.ma.subject = args[0]
		nc.ps.ma.sid = parseInt64(args[1])
		nc.ps.ma.reply = args[2]
		nc.ps.ma.size = int(parseInt64(args[3]))
	default:
		return fmt.Errorf("nats: processMsgArgs Parse Error: '%s'", arg)
	}
	if nc.ps.ma.size < 0 {
		return fmt.Errorf("nats: processMsgArgs Bad or Missing Size: '%s'", arg)
	}
	return nil
}

// Ascii numbers 0-9
const (
	ascii_0 = 48
	ascii_9 = 57
)

// parseInt64 expects decimal positive numbers. We
// return -1 to signal error
func parseInt64(d []byte) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < ascii_0 || dec > ascii_9 {
			return -1
		}
		n = n*10 + (int64(dec) - ascii_0)
	}
	return n
}
