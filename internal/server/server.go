package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.store/internal/auth"
	"go.store/internal/config"
)

type Server struct {
	cfg      *config.Config
	auth     *auth.Authenticator
	ln       net.Listener
	shutdown chan struct{}
}

func New(cfg *config.Config) (*Server, error) {
	store, err := auth.NewFileStore(cfg.UserFile)
	if err != nil {
		return nil, err
	}
	a := auth.NewAuthenticator(store)

	return &Server{
		cfg:      cfg,
		auth:     a,
		shutdown: make(chan struct{}),
	}, nil
}

func (s *Server) Listen() error {
	var l net.Listener
	var err error

	if s.cfg.EnableTLS {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			return fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		l, err = tls.Listen("tcp", s.cfg.Addr, tlsCfg)
		if err != nil {
			return fmt.Errorf("failed to start TLS listener: %w", err)
		}

		fmt.Println("TLS enabled")
	} else {
		l, err = net.Listen("tcp", s.cfg.Addr)
		if err != nil {
			return fmt.Errorf("failed to start TCP listener: %w", err)
		}
	}

	s.ln = l

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

		<-sigCh
		fmt.Println("\nServer shutting down...")

		close(s.shutdown)

		s.ln.Close()
	}()

	for {
		conn, err := l.Accept()

		select {
		case <-s.shutdown:
			return nil
		default:
		}

		if err != nil {
			select {
			case <-s.shutdown:
				return nil
			default:
				continue
			}
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	sess := &Session{}
	reader := bufio.NewScanner(conn)

	conn.Write([]byte(Prompt))

	for reader.Scan() {
		select {
		case <-s.shutdown:
			conn.Write([]byte("\nServer shutting down...\n"))
			conn.Close()
			return
		default:
		}

		line := reader.Text()
		resp := s.exec(sess, line)

		conn.Write([]byte(resp.Msg + "\n"))

		if resp.Close {
			conn.Close()
			return
		}

		conn.Write([]byte(Prompt))
	}
}

func (s *Server) exec(sess *Session, line string) Response {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return Response{Msg: Msg(""), Close: false}
	}

	switch strings.ToUpper(parts[0]) {
	case "AUTH":
		return s.authCommand(sess, parts)
	case "OPEN":
		return s.openDBCommand(sess, parts)
	case "SET":
		return setCommand(sess, parts)
	case "GET":
		return getCommand(sess, parts)
	case "DEL":
		return delCommand(sess, parts)
	case "CLOSE":
		sess.CloseDB()
		return Respond(OK)
	case "EXIT":
		return exitCommand(sess, parts)
	default:
		return Respond(Prompt)
	}
}
