package main

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type    string  `json:type`
	Message float64 `json:message`
}

func main() {
	n := maelstrom.NewNode()

	topology := n.NodeIDs()
	messages := map[float64]bool{}
	var mu sync.Mutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Check if the message has already been seen
		mu.Lock()
		if ok := messages[body.Message]; ok {
			mu.Unlock()
			return nil
		}
		messages[body.Message] = true
		mu.Unlock()

		// Propagate received broadcast message to neighbouring nodes
		for _, dest := range topology {
			if dest == msg.Src {
				continue
			}
			n.Send(dest, body)
		}

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
		mu.Lock()
		for k := range messages {
			keys = append(keys, k)
		}
		mu.Unlock()
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
}
