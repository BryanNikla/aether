# aether

`aether` is a generic, concurrency-safe WebSocket hub for Go servers. It upgrades HTTP requests, tracks connected clients, decodes inbound JSON messages, and queues outbound JSON messages for delivery.

## Requirements

- Go 1.21 or later
- WebSocket messages encoded as JSON

## Install

```sh
go get github.com/BryanNikla/aether
```

## Quick start: echo server

Define the message type your application expects. The same type is used for messages read from and sent to each client:

```go
package main

import (
	"log"
	"net/http"

	"github.com/BryanNikla/aether"
)

type Message struct {
	Text string `json:"text"`
}

func main() {
	hub := aether.NewNetworkHub[Message]()
	hub.SetMessageHandler(func(client aether.Client[Message], message Message) {
		// Echo the decoded message back to the sender.
		client.Send(message)
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if _, err := aether.UpgradeHTTP(hub, w, r, aether.WebSocketOptions{}); err != nil {
			log.Printf("websocket upgrade: %v", err)
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Connect with any WebSocket client and send a JSON message such as `{"text":"hello"}`. The server replies with the same JSON object. `UpgradeHTTP` applies defaults for zero-valued options, registers the connection, and starts its read and write pumps.

## Routing messages

The handler runs when a client sends a valid JSON message. Use the `Client` passed to the handler to reply to that client, or the hub to route to other clients:

```go
hub.SetMessageHandler(func(sender aether.Client[Message], message Message) {
	// Send to every connected client.
	hub.Broadcast(message)

	// Send only to a client when its ID is known.
	if hub.SendTo(sender.ID(), Message{Text: "received"}) {
		log.Printf("replied to client %d", sender.ID())
	}
})
```

`NetworkHub` also provides `Register`, `Unregister`, and `Len`. `Unregister` closes the client. Registering a client whose ID is already present replaces and closes the previous client. `SetMessageHandler(nil)` clears the handler.

## WebSocket options

Pass a `WebSocketOptions` value to `UpgradeHTTP` to tune connection limits and keepalives. Unset or non-positive numeric and duration fields use defaults; `PingPeriod` must be shorter than `PongWait`.

```go
func handleWebSocket(hub *aether.NetworkHub[Message], w http.ResponseWriter, r *http.Request) {
	opts := aether.DefaultWebSocketOptions()
	opts.MaxMessageSize = 128 * 1024
	opts.SendBufferSize = 64
	opts.PongWait = 30 * time.Second
	opts.PingPeriod = 27 * time.Second

	if _, err := aether.UpgradeHTTP(hub, w, r, opts); err != nil {
		log.Printf("websocket upgrade: %v", err)
	}
}
```

The default settings use 1 KiB read and write buffers, a 10-second handshake timeout and write deadline, a 60-second pong wait, a 54-second ping interval, a 64 KiB maximum inbound message, and an outbound queue of 256 messages per client.

`CheckOrigin` can override Gorilla WebSocket's default same-origin check. If you set it, validate allowed origins explicitly; accepting every origin can expose an authenticated WebSocket endpoint to cross-site use.

## Delivery and lifecycle

- `Send` queues a message; it does not wait for the network write to finish. A slow client with a full queue is disconnected.
- Inbound messages are decoded with `encoding/json` via Gorilla WebSocket's `ReadJSON`; outbound messages are encoded with `WriteJSON`. Use message types supported by JSON.
- The hub invokes the current message handler from the connection's read pump. Keep handlers quick and avoid blocking work so that a client can continue reading messages.
- A WebSocket client is removed and closed when its connection ends. `Client.Close` is safe to call more than once.
- `Client.ID()` returns the connection's hub-assigned ID. IDs start at 1 and increase for each upgraded connection.

## Errors

`UpgradeHTTP` can return `ErrNilHub`, `ErrNilWriter`, `ErrNilRequest`, or `ErrInvalidOpts` for invalid arguments/configuration. It also returns errors from the WebSocket upgrade; in that case, the upgrader has already written the HTTP error response.