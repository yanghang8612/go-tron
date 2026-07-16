package jsonrpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// wsSub is a single active eth_subscribe subscription for one WebSocket connection.
type wsSub struct {
	id        string
	kind      string     // "newHeads" or "logs"
	logFilter *LogFilter // non-nil for "logs"
	outCh     chan<- []byte
}

// SubscriptionManager tracks active WebSocket subscriptions.
type SubscriptionManager struct {
	mu   sync.Mutex
	subs map[string]*wsSub
}

func newSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{subs: make(map[string]*wsSub)}
}

// notify is called by FilterManager.fanOut (after releasing fm.mu) for each new block.
// It copies the relevant sub set under sm.mu, then sends to per-connection channels
// without holding any lock.
func (sm *SubscriptionManager) notify(block *types.Block, logs []*RPCLog) {
	type work struct {
		sub    *wsSub
		frames [][]byte
	}

	sm.mu.Lock()
	pending := make([]work, 0, len(sm.subs))
	for _, sub := range sm.subs {
		switch sub.kind {
		case "newHeads":
			frame := marshalPush(sub.id, blockToRPC(block, false))
			pending = append(pending, work{sub: sub, frames: [][]byte{frame}})
		case "logs":
			var frames [][]byte
			for _, l := range logs {
				if matchesLogFilter(l, sub.logFilter) {
					frames = append(frames, marshalPush(sub.id, l))
				}
			}
			if len(frames) > 0 {
				pending = append(pending, work{sub: sub, frames: frames})
			}
		}
	}
	sm.mu.Unlock()

	for _, p := range pending {
		for _, frame := range p.frames {
			select {
			case p.sub.outCh <- frame:
			default: // slow subscriber — drop
			}
		}
	}
}

// ServeWS upgrades the HTTP connection to WebSocket and runs the subscription loop.
func (sm *SubscriptionManager) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	outCh := make(chan []byte, 128)
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		writerLoop(conn, outCh, quit)
	}()

	var connSubIDs []string
	defer func() {
		close(quit)
		wg.Wait()
		sm.mu.Lock()
		for _, id := range connSubIDs {
			delete(sm.subs, id)
		}
		sm.mu.Unlock()
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req rpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			sendWS(outCh, errResp(nil, codeParseError, "parse error"))
			continue
		}
		switch req.Method {
		case "eth_subscribe":
			id, err := sm.handleSubscribe(req, outCh)
			if err != nil {
				sendWS(outCh, errResp(req.ID, codeInvalidParams, err.Error()))
				continue
			}
			connSubIDs = append(connSubIDs, id)
			sendWS(outCh, rpcResponse{JSONRPC: "2.0", Result: id, ID: req.ID})
		case "eth_unsubscribe":
			var p []string
			if err := json.Unmarshal(req.Params, &p); err != nil || len(p) == 0 {
				sendWS(outCh, errResp(req.ID, codeInvalidParams, "invalid params"))
				continue
			}
			removed := sm.removeByID(p[0])
			sendWS(outCh, rpcResponse{JSONRPC: "2.0", Result: removed, ID: req.ID})
		default:
			sendWS(outCh, errResp(req.ID, codeMethodNotFound, "method not found"))
		}
	}
}

func (sm *SubscriptionManager) handleSubscribe(req rpcRequest, outCh chan<- []byte) (string, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(req.Params, &p); err != nil || len(p) == 0 {
		return "", fmt.Errorf("invalid params")
	}
	var kind string
	if err := json.Unmarshal(p[0], &kind); err != nil {
		return "", fmt.Errorf("invalid subscription type")
	}

	id, err := generateFilterID()
	if err != nil {
		return "", err
	}

	sub := &wsSub{id: id, kind: kind, outCh: outCh}
	switch kind {
	case "newHeads":
		// no extra params
	case "logs":
		lf := LogFilter{}
		if len(p) > 1 {
			lf, err = parseLogFilterObject(p[1])
			if err != nil {
				return "", err
			}
		}
		sub.logFilter = &lf
	default:
		return "", fmt.Errorf("unsupported subscription type %q", kind)
	}

	sm.mu.Lock()
	sm.subs[id] = sub
	sm.mu.Unlock()
	return id, nil
}

func (sm *SubscriptionManager) removeByID(id string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.subs[id]
	if ok {
		delete(sm.subs, id)
	}
	return ok
}

// writerLoop is the single writer goroutine for a WebSocket connection.
func writerLoop(conn *websocket.Conn, ch <-chan []byte, quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}
}

// sendWS routes a JSON-RPC response through the connection's outCh so the
// single writerLoop goroutine handles the write.
func sendWS(outCh chan<- []byte, resp rpcResponse) {
	data, _ := json.Marshal(resp)
	select {
	case outCh <- data:
	default:
	}
}

// marshalPush builds an eth_subscription push frame.
func marshalPush(subID string, result interface{}) []byte {
	data, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscription",
		"params": map[string]interface{}{
			"subscription": subID,
			"result":       result,
		},
	})
	return data
}

// parseLogFilterObject parses an eth filter object (address + topics + block range).
func parseLogFilterObject(raw json.RawMessage) (LogFilter, error) {
	var obj struct {
		FromBlock string          `json:"fromBlock"`
		ToBlock   string          `json:"toBlock"`
		BlockHash string          `json:"blockHash"`
		Address   json.RawMessage `json:"address"`
		Topics    json.RawMessage `json:"topics"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return LogFilter{}, fmt.Errorf("invalid filter: %w", err)
	}
	lf := LogFilter{}
	if obj.BlockHash != "" {
		var h common.Hash
		copy(h[:], common.FromHex(obj.BlockHash))
		lf.BlockHash = &h
	} else {
		if obj.FromBlock != "" {
			n, err := parseBlockParam(obj.FromBlock)
			if err != nil {
				return LogFilter{}, err
			}
			lf.FromBlock = &n
		}
		if obj.ToBlock != "" {
			n, err := parseBlockParam(obj.ToBlock)
			if err != nil {
				return LogFilter{}, err
			}
			lf.ToBlock = &n
		}
	}
	if len(obj.Address) > 0 && string(obj.Address) != "null" {
		addresses, err := parseFilterAddresses(obj.Address)
		if err != nil {
			return LogFilter{}, err
		}
		lf.Addresses = addresses
	}
	if len(obj.Topics) > 0 && string(obj.Topics) != "null" {
		var rawTopics []json.RawMessage
		if err := json.Unmarshal(obj.Topics, &rawTopics); err == nil {
			lf.Topics = make([][]common.Hash, len(rawTopics))
			for i, rt := range rawTopics {
				if string(rt) == "null" {
					continue
				}
				var single string
				var multi []string
				if json.Unmarshal(rt, &single) == nil {
					var h common.Hash
					copy(h[:], common.FromHex(single))
					lf.Topics[i] = []common.Hash{h}
				} else if json.Unmarshal(rt, &multi) == nil {
					for _, s := range multi {
						var h common.Hash
						copy(h[:], common.FromHex(s))
						lf.Topics[i] = append(lf.Topics[i], h)
					}
				}
			}
		}
	}
	return lf, nil
}
