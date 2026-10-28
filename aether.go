// Package aether provides a generic, thread-safe WebSocket networking hub
// for multiplayer game servers.
package aether

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ErrNilHub      = errors.New("aether: hub is nil")
	ErrNilWriter   = errors.New("aether: response writer is nil")
	ErrNilRequest  = errors.New("aether: request is nil")
	ErrNilConn     = errors.New("aether: websocket connection is nil")
	ErrInvalidOpts = errors.New("aether: PingPeriod must be shorter than PongWait")
)

// WebSocketOptions configures connection upgrades and per-client pumps.
type WebSocketOptions struct {
	ReadBufferSize   int
	WriteBufferSize  int
	HandshakeTimeout time.Duration
	// WriteWait is the time allowed to write a message to the peer.
	WriteWait time.Duration
	// PongWait is the time allowed to read the next pong message from the peer.
	PongWait time.Duration
	// PingPeriod must be less than PongWait.
	PingPeriod time.Duration
	// MaxMessageSize is the maximum size in bytes of an inbound message.
	MaxMessageSize int64
	// SendBufferSize is the capacity of each client's outbound queue.
	SendBufferSize int
	// CheckOrigin overrides the origin check; nil uses gorilla's same-origin default.
	CheckOrigin func(r *http.Request) bool
}

// DefaultWebSocketOptions returns sensible defaults.
func DefaultWebSocketOptions() WebSocketOptions {
	return WebSocketOptions{
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
		HandshakeTimeout: 10 * time.Second,
		WriteWait:        10 * time.Second,
		PongWait:         60 * time.Second,
		PingPeriod:       54 * time.Second,
		MaxMessageSize:   64 * 1024,
		SendBufferSize:   256,
	}
}

func (o WebSocketOptions) withDefaults() WebSocketOptions {
	d := DefaultWebSocketOptions()
	if o.ReadBufferSize <= 0 {
		o.ReadBufferSize = d.ReadBufferSize
	}
	if o.WriteBufferSize <= 0 {
		o.WriteBufferSize = d.WriteBufferSize
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = d.HandshakeTimeout
	}
	if o.WriteWait <= 0 {
		o.WriteWait = d.WriteWait
	}
	if o.PongWait <= 0 {
		o.PongWait = d.PongWait
	}
	if o.PingPeriod <= 0 {
		o.PingPeriod = (o.PongWait * 9) / 10
	}
	if o.MaxMessageSize <= 0 {
		o.MaxMessageSize = d.MaxMessageSize
	}
	if o.SendBufferSize <= 0 {
		o.SendBufferSize = d.SendBufferSize
	}
	return o
}

// Client is a connected peer that can receive messages of type M.
type Client[M any] interface {
	ID() uint64
	Send(msg M)
	Close()
}

// MessageHandler is invoked for every inbound message decoded from a client.
type MessageHandler[M any] func(c Client[M], msg M)

// NetworkHub tracks connected clients and routes messages to them.
type NetworkHub[M any] struct {
	mu      sync.RWMutex
	clients map[uint64]Client[M]
	nextID  atomic.Uint64
	handler atomic.Pointer[MessageHandler[M]]
}

// NewNetworkHub creates an empty hub.
func NewNetworkHub[M any]() *NetworkHub[M] {
	return &NetworkHub[M]{
		clients: make(map[uint64]Client[M]),
	}
}

// SetMessageHandler sets the callback for inbound client messages. Pass nil to clear it.
func (h *NetworkHub[M]) SetMessageHandler(fn MessageHandler[M]) {
	if h == nil {
		return
	}
	if fn == nil {
		h.handler.Store(nil)
		return
	}
	h.handler.Store(&fn)
}

func (h *NetworkHub[M]) dispatch(c Client[M], msg M) {
	if h == nil || c == nil {
		return
	}
	fn := h.handler.Load()
	if fn == nil || *fn == nil {
		return
	}
	(*fn)(c, msg)
}

// newID returns a strictly unique, incrementing ID (starting at 1).
func (h *NetworkHub[M]) newID() uint64 {
	return h.nextID.Add(1)
}

// Register adds a client to the hub, replacing any client with the same ID.
func (h *NetworkHub[M]) Register(c Client[M]) {
	if h == nil || c == nil {
		return
	}
	h.mu.Lock()
	if h.clients == nil {
		h.clients = make(map[uint64]Client[M])
	}
	old, exists := h.clients[c.ID()]
	h.clients[c.ID()] = c
	h.mu.Unlock()

	if exists && old != nil && old != c {
		old.Close()
	}
}

// Unregister removes a client from the hub and closes it.
func (h *NetworkHub[M]) Unregister(c Client[M]) {
	if h == nil || c == nil {
		return
	}
	h.mu.Lock()
	if existing, ok := h.clients[c.ID()]; ok && existing == c {
		delete(h.clients, c.ID())
	}
	h.mu.Unlock()

	c.Close()
}

// Broadcast sends msg to every registered client.
func (h *NetworkHub[M]) Broadcast(msg M) {
	if h == nil {
		return
	}
	h.mu.RLock()
	targets := make([]Client[M], 0, len(h.clients))
	for _, c := range h.clients {
		if c != nil {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()

	// Send outside the lock so Close/Unregister callbacks cannot deadlock.
	for _, c := range targets {
		c.Send(msg)
	}
}

// SendTo sends msg to the client with the given ID. It reports whether the client was found.
func (h *NetworkHub[M]) SendTo(id uint64, msg M) bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	c, ok := h.clients[id]
	h.mu.RUnlock()

	if !ok || c == nil {
		return false
	}
	c.Send(msg)
	return true
}

// Len returns the number of registered clients.
func (h *NetworkHub[M]) Len() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// webSocketClient is a Client backed by a gorilla websocket connection.
type webSocketClient[M any] struct {
	id        uint64
	hub       *NetworkHub[M]
	conn      *websocket.Conn
	send      chan M
	done      chan struct{}
	closeOnce sync.Once
	opts      WebSocketOptions
}

var _ Client[struct{}] = (*webSocketClient[struct{}])(nil)

func newWebSocketClient[M any](hub *NetworkHub[M], conn *websocket.Conn, opts WebSocketOptions) *webSocketClient[M] {
	return &webSocketClient[M]{
		id:   hub.newID(),
		hub:  hub,
		conn: conn,
		send: make(chan M, opts.SendBufferSize),
		done: make(chan struct{}),
		opts: opts,
	}
}

func (c *webSocketClient[M]) ID() uint64 {
	if c == nil {
		return 0
	}
	return c.id
}

// Send queues msg for delivery. Slow clients whose buffer is full are disconnected.
func (c *webSocketClient[M]) Send(msg M) {
	if c == nil {
		return
	}
	select {
	case <-c.done:
		return
	default:
	}

	select {
	case c.send <- msg:
	case <-c.done:
	default:
		// Buffer full: drop the client rather than block the caller.
		go c.hub.Unregister(c)
	}
}

// Close signals both pumps to stop. Safe to call multiple times.
func (c *webSocketClient[M]) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		close(c.done)
	})
}

func (c *webSocketClient[M]) readPump() {
	if c == nil || c.conn == nil {
		return
	}
	defer func() {
		c.hub.Unregister(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(c.opts.MaxMessageSize)
	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.PongWait)); err != nil {
		return
	}
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.opts.PongWait))
	})

	for {
		var msg M
		if err := c.conn.ReadJSON(&msg); err != nil {
			return
		}
		c.hub.dispatch(c, msg)
	}
}

func (c *webSocketClient[M]) writePump() {
	if c == nil || c.conn == nil {
		return
	}
	ticker := time.NewTicker(c.opts.PingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg := <-c.send:
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteWait)); err != nil {
				c.Close()
				return
			}
			if err := c.conn.WriteJSON(msg); err != nil {
				c.Close()
				return
			}
		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteWait)); err != nil {
				c.Close()
				return
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.Close()
				return
			}
		case <-c.done:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteWait))
			_ = c.conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			return
		}
	}
}

// UpgradeHTTP upgrades the request to a WebSocket, registers the client with
// hub, and starts its read and write pumps.
func UpgradeHTTP[M any](hub *NetworkHub[M], w http.ResponseWriter, r *http.Request, opts WebSocketOptions) (Client[M], error) {
	if hub == nil {
		return nil, ErrNilHub
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	if r == nil {
		return nil, ErrNilRequest
	}

	opts = opts.withDefaults()
	if opts.PingPeriod >= opts.PongWait {
		http.Error(w, "server misconfigured", http.StatusInternalServerError)
		return nil, ErrInvalidOpts
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:   opts.ReadBufferSize,
		WriteBufferSize:  opts.WriteBufferSize,
		HandshakeTimeout: opts.HandshakeTimeout,
		CheckOrigin:      opts.CheckOrigin,
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written an HTTP error response.
		return nil, err
	}
	if conn == nil {
		return nil, ErrNilConn
	}

	client := newWebSocketClient(hub, conn, opts)
	hub.Register(client)

	go client.writePump()
	go client.readPump()

	return client, nil
}
