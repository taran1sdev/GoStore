package server

import (
	"bufio"
	"net"
	"strings"
)

type Server struct {
	addr string
}

const string prompt = "gostore> "

func New(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) Listen() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	sess := &Session{}
	reader := bufio.NewScanner(conn)

	conn.Write(byte(prompt))
	for reader.Scan() {
		line := reader.Text()

		resp := s.exec(sess, line)

		conn.Write([]byte(resp + "\n"))
	}
}

func (s *Server) exec(sess *Session, line string) string {
	parts := string.Fields(line)
	if len(parts) == 0 {
		return prompt
	}

	switch strings.ToUpper(parts[0]) {
	case "AUTH":
		return authCommand(sess, parts)
	case "OPEN":
		return openCommand(sess, parts)
	case "SET":
		return setCommand(sess, parts)
	case "GET":
		return getCommand(sess, parts)
	case "DEL":
		return delCommand(sess, parts)
	case "CLOSE":
		sess.CloseDB()
		return "OK"
	case "EXIT":
		return exitCommand(sess, parts)
	default:
		return prompt
	}
}
