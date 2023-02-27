package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Src     string  `json:src,omitempty`
	Type    string  `json:type`
	Message float64 `json:message`
}

type SrcMessage struct {
	Src     string
	Message BroadcastMessage
}

func main() {
	n := maelstrom.NewNode()

	ctx := context.Background()
	messageQueue := make(chan SrcMessage, 3)
	topology := n.NodeIDs()
	messages := map[float64]bool{}
	var mu sync.RWMutex

	for i := 0; i < 3; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case newMsg := <-messageQueue:
					// Propagate received broadcast message to neighbouring nodes
					for _, dest := range topology {
						if dest == newMsg.Src {
							continue
						}
						n.Send(dest, newMsg.Message)
					}
				}
			}
		}()
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Check if the message has already been seen
		mu.RLock()
		if ok := messages[body.Message]; ok {
			mu.RUnlock()
			return nil
		}
		mu.RUnlock()

		mu.Lock()
		messages[body.Message] = true
		mu.Unlock()

		messageQueue <- SrcMessage{msg.Src, body}

		// Echo the original message back with the updated message type.
		if strings.HasPrefix(msg.Src, "n") {
			return nil
		}
		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"
		keys := []float64{}
		mu.RLock()
		for k := range messages {
			keys = append(keys, k)
		}
		mu.RUnlock()
		body["messages"] = keys

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		topology = []string{}
		for _, neighbour := range body["topology"].(map[string]interface{})[n.ID()].([]interface{}) {
			topology = append(topology, neighbour.(string))
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	ctx.Done()
}
