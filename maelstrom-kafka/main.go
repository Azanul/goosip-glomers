package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

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

func tailKey(key string) string {
	return fmt.Sprintf("tail_%s", key)
}

func entryKey(key string, offset int) string {
	return fmt.Sprintf("entry_%s_%d", key, offset)
}

func commitKey(key string) string {
	return fmt.Sprintf("commit_%s", key)
}

func main() {
	n := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(n)
	logs := AppendOnlyLog{ctx: context.TODO(), kv: kv}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		tail, err := logs.Append(body.Key, body.Msg)
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

		msgs := map[string][][2]int{}
		for key, startingOffset := range body.Offsets {
			for offset := startingOffset; offset < startingOffset+3; offset++ {
				message, exists, err := logs.Poll(key, offset)
				if err != nil {
					return err
				}
				if !exists {
					break
				}
				msgs[key] = append(msgs[key], [2]int{offset, message})
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
			if err := logs.CommitOffset(key, offset); err != nil {
				return err
			}
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
	ctx context.Context
	kv  *maelstrom.KV
}

func (aol *AppendOnlyLog) Append(key string, val float64) (int, error) {
	keyTail := tailKey(key)
	offset, err := aol.kv.ReadInt(aol.ctx, keyTail)
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			offset = 0
		} else {
			return -1, err
		}
	} else {
		offset++
	}

	for ; ; offset++ {
		if err := aol.kv.CompareAndSwap(aol.ctx,
			keyTail, offset-1, offset, true); err != nil {
			fmt.Fprintf(os.Stderr, "cas retry: %v\n", err)
			continue
		}
		break
	}

	return offset, aol.kv.Write(aol.ctx, entryKey(key, offset), val)
}

func (aol *AppendOnlyLog) Poll(key string, offset int) (int, bool, error) {
	val, err := aol.kv.ReadInt(aol.ctx, entryKey(key, offset))
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}
	return val, true, nil
}

func (aol *AppendOnlyLog) CommitOffset(key string, offset int) error {
	return aol.kv.Write(aol.ctx, commitKey(key), offset)
}

func (aol *AppendOnlyLog) GetCommitedOffset(key string) int {
	v, err := aol.kv.ReadInt(aol.ctx, commitKey(key))
	if err != nil {
		return 0
	}

	return v
}
