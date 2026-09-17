package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

var Debug bool

const usageText = `acp-multiplex - run one ACP agent behind several frontends

usage:
  acp-multiplex [--tcp host:port] <agent-command> [args...]
        Start the agent and proxy it. The primary frontend speaks ACP on
        stdin/stdout; further frontends attach over a unix socket, and over
        TCP as well when --tcp is given.

  acp-multiplex attach [--tcp host:port | <socket-path>]
        Connect stdin/stdout to an already-running proxy as a secondary
        frontend, replaying the session so far.

options:
  --tcp host:port   listen on (proxy) or dial (attach) a TCP address
  -h, --help        print this help

examples:
  acp-multiplex claude-agent-acp
  acp-multiplex --tcp 100.64.0.10:19994 pi-acp
  acp-multiplex attach --tcp 100.64.0.10:19994
`

func usage(w io.Writer) { fmt.Fprint(w, usageText) }

// fatalf reports a startup failure on stderr as well as in the log file.
// runProxy points the logger at a per-pid file before anything that can
// fail, so log.Fatalf there is invisible: the process exits 1 with silent
// output and the ACP client just sees an agent that never started.
func fatalf(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	fmt.Fprintf(os.Stderr, "acp-multiplex: %s\n", msg)
	log.Printf("fatal: %s", msg)
	os.Exit(1)
}

func main() {
	tcpAddr := ""
	args := os.Args[1:]

	// Extract --tcp HOST:PORT from args before mode routing.
	// Works in both proxy and attach modes.
	for i := 0; i < len(args); i++ {
		if args[i] == "--tcp" {
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "acp-multiplex: --tcp needs a host:port value\n\n")
				usage(os.Stderr)
				os.Exit(1)
			}
			tcpAddr = args[i+1]
			args = append(args[:i], args[i+2:]...)
			break
		}
	}

	if len(args) < 1 {
		usage(os.Stderr)
		os.Exit(1)
	}

	// Only args[0] is ours to interpret; everything after the agent command
	// belongs to the agent, so `acp-multiplex some-agent -h` still reaches
	// some-agent.
	if args[0] == "-h" || args[0] == "--help" {
		usage(os.Stdout)
		os.Exit(0)
	}

	// A leftover dash here is a mistyped option, not an agent. Without this
	// it goes to exec.Command as the binary to run and fails where nobody
	// is looking.
	if strings.HasPrefix(args[0], "-") {
		fmt.Fprintf(os.Stderr, "acp-multiplex: unknown option %q\n\n", args[0])
		usage(os.Stderr)
		os.Exit(1)
	}

	switch args[0] {
	case "attach":
		runAttach(tcpAddr, args[1:])
	default:
		runProxy(tcpAddr, args)
	}
}

// runProxy starts the agent subprocess and multiplexing proxy.
// tcpAddr, if set, makes the proxy also listen on a TCP address
// (e.g. "100.x.x.x:9999") for secondary frontends over the network.
// agentArgs is the agent command to spawn (everything after flags).
func runProxy(tcpAddr string, agentArgs []string) {
	// Set up debug logging to file
	Debug = false
	logDir := filepath.Join(socketDir(), "logs")
	os.MkdirAll(logDir, 0700)
	logPath := filepath.Join(logDir, fmt.Sprintf("%d.log", os.Getpid()))
	logFile, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log file: %v\n", err)
	} else {
		log.SetOutput(logFile)
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	}

	cleanStaleSockets()

	cmd := exec.Command(agentArgs[0], agentArgs[1:]...)
	agentIn, err := cmd.StdinPipe()
	if err != nil {
		fatalf("agent stdin pipe: %v", err)
	}
	agentOut, err := cmd.StdoutPipe()
	if err != nil {
		fatalf("agent stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		fatalf("start agent %q: %v", agentArgs[0], err)
	}

	cache := NewCache()

	// If a session name is provided, cache it as metadata for secondary frontends.
	if name := os.Getenv("ACP_MULTIPLEX_NAME"); name != "" {
		meta, _ := json.Marshal(map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "acp-multiplex/meta",
			"params":  map[string]string{"name": name},
		})
		cache.SetMeta(meta)
	}

	proxy := NewProxy(agentIn, agentOut, cache)
	proxy.sockPath = socketPath()

	// Primary frontend on stdin/stdout
	primary := NewStdioFrontend(0)
	proxy.AddFrontend(primary)

	// Unix socket for additional frontends
	sockPath := socketPath()
	ln, err := listenUnix(sockPath)
	if err != nil {
		fatalf("listen unix %s: %v", sockPath, err)
	}
	fmt.Fprintf(os.Stderr, "acp-multiplex: socket %s, log %s/logs/%d.log\n", sockPath, socketDir(), os.Getpid())

	// TCP listener for remote secondary frontends (optional)
	var tcpLn net.Listener
	if tcpAddr != "" {
		tcpLn, err = net.Listen("tcp", tcpAddr)
		if err != nil {
			fatalf("listen tcp %s: %v", tcpAddr, err)
		}
		fmt.Fprintf(os.Stderr, "acp-multiplex: tcp %s\n", tcpAddr)
	}

	nextID := 1
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept: %v", err)
				return
			}
			nextID++
			f := NewSocketFrontend(nextID, conn)
			proxy.AddFrontend(f)
		}
	}()

	if tcpLn != nil {
		go func() {
			for {
				conn, err := tcpLn.Accept()
				if err != nil {
					log.Printf("tcp accept: %v", err)
					return
				}
				nextID++
				f := NewSocketFrontend(nextID, conn)
				proxy.AddFrontend(f)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Remove(sockPath)
		if tcpLn != nil {
			tcpLn.Close()
		}
		cmd.Process.Kill()
		os.Exit(0)
	}()

	go proxy.Run()
	err = cmd.Wait()
	if err != nil {
		log.Printf("agent process exited: %v", err)
	} else {
		log.Printf("agent process exited: status 0")
	}
	if cmd.ProcessState != nil {
		log.Printf("agent exit details: pid=%d, exitcode=%d, sys=%v",
			cmd.ProcessState.Pid(), cmd.ProcessState.ExitCode(), cmd.ProcessState.Sys())
	}
	ln.Close()
	if tcpLn != nil {
		tcpLn.Close()
	}
	os.Remove(sockPath)
	os.Exit(0)
}

// runAttach bridges stdin/stdout to an existing proxy.
// With tcpAddr set, connects over TCP (e.g. "100.x.x.x:9999").
// Otherwise expects a Unix socket path as the first positional arg.
// This lets stdio-only ACP clients (Toad, acp-ui, agent-shell) connect
// as secondary frontends to remote or local sessions.
func runAttach(tcpAddr string, args []string) {
	var conn net.Conn
	var err error

	if tcpAddr != "" {
		conn, err = net.Dial("tcp", tcpAddr)
		if err != nil {
			fatalf("connect to tcp %s: %v", tcpAddr, err)
		}
	} else {
		if len(args) < 1 {
			fmt.Fprintf(os.Stderr, "acp-multiplex: attach needs --tcp host:port or a socket path\n\n")
			usage(os.Stderr)
			os.Exit(1)
		}
		conn, err = net.Dial("unix", args[0])
		if err != nil {
			fatalf("connect to %s: %v", args[0], err)
		}
	}
	defer conn.Close()

	// Bidirectional pipe: stdin -> conn, conn -> stdout
	done := make(chan struct{}, 2)
	go func() { io.Copy(conn, os.Stdin); done <- struct{}{} }()
	go func() { io.Copy(os.Stdout, conn); done <- struct{}{} }()
	<-done
}

// socketDir returns the directory for acp-multiplex sockets, creating it if needed.
func socketDir() string {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	dir = filepath.Join(dir, "acp-multiplex")
	os.MkdirAll(dir, 0700)
	return dir
}

func socketPath() string {
	return filepath.Join(socketDir(), fmt.Sprintf("%d.sock", os.Getpid()))
}

// cleanStaleSockets removes sockets whose owning process is dead.
func cleanStaleSockets() {
	dir := socketDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sock") {
			continue
		}
		pidStr := strings.TrimSuffix(name, ".sock")
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			continue
		}
		if err := syscall.Kill(pid, 0); err != nil {
			os.Remove(filepath.Join(dir, name))
		}
	}
}

func listenUnix(path string) (net.Listener, error) {
	os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	// Owner-only so other users on the machine can't connect.
	os.Chmod(path, 0600)
	return ln, nil
}
