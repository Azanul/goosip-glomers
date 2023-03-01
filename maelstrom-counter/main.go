package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessage struct {
	Type  string `json:type`
	Delta int    `json:delta`
}

var CounterKey string = "counter"

func main() {
	n := maelstrom.NewNode()

	ctx := context.Background()
	kv := maelstrom.NewSeqKV(n)
	ticker := time.NewTicker(5 * time.Second)

	getInt := func(nodeId string) int {
		v, err := kv.ReadInt(ctx, nodeId)
		if err != nil {
			v = 0
		}
		return v
	}

	go func() {
		for {
			<-ticker.C
			counters := map[string]int{}
			for _, node := range n.NodeIDs() {
				counters[node] = getInt(node)
			}
			replicateBody := map[string]interface{}{"type": "replicate", "counters": counters}
			for _, node := range n.NodeIDs() {
				n.Send(node, replicateBody)
			}
		}
	}()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		kv.Write(ctx, n.ID(), getInt(n.ID())+body.Delta)

		return n.Reply(msg, map[string]string{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		sum := 0
		for _, node := range n.NodeIDs() {
			sum += getInt(node)
		}

		return n.Reply(msg, map[string]any{"type": "read_ok", "value": sum})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, node := range n.NodeIDs() {
			count := int(body["counters"].(map[string]interface{})[node].(float64))
			if getInt(node) < count {
				kv.Write(ctx, node, count)
			}
		}

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
