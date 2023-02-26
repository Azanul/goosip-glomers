package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	topology := n.NodeIDs()
	messages := map[float64]bool{}
	var mu sync.Mutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Check if the message has already been seen
		if ok := messages[body["message"].(float64)]; ok {
			return nil
		}

		// Propagate received broadcast message to neighbouring nodes
		for _, dest := range topology {
			n.Send(dest, body)
		}

		// Update the message type to return back.
		body["type"] = "broadcast_ok"
		mu.Lock()
		messages[body["message"].(float64)] = true
		mu.Unlock()
		delete(body, "message")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
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
		for k := range messages {
			keys = append(keys, k)
		}
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

		delete(body, "topology")
		body["type"] = "topology_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
