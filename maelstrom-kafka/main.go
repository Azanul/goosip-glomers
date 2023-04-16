package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendMessage struct {
	Type string  `json:"type"`
	Key  string  `json:"key"`
	Msg  float64 `json:"msg"`
}

type OffsetsMessage struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func main() {
	n := maelstrom.NewNode()

	ctx := context.Background()
	kv := maelstrom.NewLinKV(n)
	logs := AppendOnlyLog{kv: kv, commit_mu: sync.RWMutex{}, commited_offsets: map[string]int{}}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		tail, err := logs.Append(ctx, body.Key, body.Msg)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]interface{}{"type": "send_ok", "offset": tail})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := map[string][][]int{}
		for key, offset := range body.Offsets {
			msgs[key] = [][]int{}
			for i, polled := range logs.Poll(ctx, key, offset) {
				msgs[key] = append(msgs[key], []int{offset + i, int(polled)})
			}
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body OffsetsMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			logs.CommitOffset(key, offset)
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		commited_offsets := map[string]int{}
		for _, key := range body["keys"].([]interface{}) {
			keyString := key.(string)
			commited_offsets[keyString] = logs.GetCommitedOffset(keyString)
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": commited_offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type AppendOnlyLog struct {
	mu               sync.RWMutex
	kv               *maelstrom.KV
	commit_mu        sync.RWMutex
	commited_offsets map[string]int
}

func (aol *AppendOnlyLog) Append(ctx context.Context, key string, val float64) (int, error) {
	aol.mu.Lock()
	defer aol.mu.Unlock()
	arr, _ := aol.kv.Read(ctx, key)
	if arr == nil {
		arr = []interface{}{}
	}
	values := sliceInterfaceToSliceInt(arr.([]interface{}))
	values = append(values, val)

	return len(values) - 1, aol.kv.Write(ctx, key, values)
}

func (aol *AppendOnlyLog) Poll(ctx context.Context, key string, offset int) []float64 {
	aol.mu.Lock()
	defer aol.mu.Unlock()
	arr, err := aol.kv.Read(ctx, key)
	if err != nil {
		return []float64{}
	}

	values := sliceInterfaceToSliceInt(arr.([]interface{}))
	return values[min(offset, len(values)):min(offset+3, len(values))]
}

func (aol *AppendOnlyLog) CommitOffset(key string, offset int) {
	aol.commit_mu.Lock()
	defer aol.commit_mu.Unlock()
	aol.commited_offsets[key] = offset
}

func (aol *AppendOnlyLog) GetCommitedOffset(key string) int {
	aol.commit_mu.RLock()
	defer aol.commit_mu.RUnlock()
	return aol.commited_offsets[key]
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func sliceInterfaceToSliceInt(ip []interface{}) []float64 {
	op := []float64{}
	for _, v := range ip {
		op = append(op, v.(float64))
	}
	return op
}
