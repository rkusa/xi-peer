// This code is partially based on Go's RPC implementation
// https://golang.org/src/net/rpc/

package peer

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

type Handler func(params interface{})

type Call struct {
	ID     uint64      `json:"id,omitempty"`
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	Reply  interface{} `json:"-"`
	Done   chan *Call  `json:"-"`
	Error  error       `json:"-"`
}

type incoming struct {
	ID     uint64           `json:"id"`
	Method string           `json:"method"`
	Params interface{}      `json:"params"`
	Result *json.RawMessage `json:"result"`
	// errors are currently not to be expected from Xi
	// Error  interface{}      `json:"error"`
}

type Peer struct {
	in  io.Reader
	out *json.Encoder

	reqMutex sync.Mutex

	handlerMutex sync.Mutex // protects following
	handler      map[string]Handler

	mutex   sync.Mutex // protects following
	seq     uint64
	pending map[uint64]*Call
}

func New() *Peer {
	peer := &Peer{
		in:      os.Stdin,
		out:     json.NewEncoder(os.Stdout),
		handler: make(map[string]Handler),
		pending: make(map[uint64]*Call),
	}
	go peer.run()
	return peer
}

func (p *Peer) run() {
	sc := bufio.NewScanner(p.in)
	for sc.Scan() {
		inc := new(incoming)
		if err := json.Unmarshal(sc.Bytes(), inc); err != nil {
			log.Fatal(err)
		}

		if inc.Result != nil { // received a response
			p.mutex.Lock()
			call := p.pending[inc.ID]
			delete(p.pending, inc.ID)
			p.mutex.Unlock()

			if call == nil {
				log.Println("rpc: dropping response that does not have a corresponding pending request")
				continue
			}

			// parse result
			if err := json.Unmarshal(*inc.Result, call.Reply); err != nil {
				log.Fatal(err)
			}

			call.done()
		} else { // received a notification
			p.handlerMutex.Lock()
			handler, ok := p.handler[inc.Method]
			p.handlerMutex.Unlock()

			if !ok {
				log.Printf("rpc: dropping notfication because there is no handler for %s registered", inc.Method)
				continue
			}

			// TODO: allow handlers to return errors (using an error channel)?
			go handler(inc.Params)
		}
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
}

func (p *Peer) send(call *Call) {
	p.reqMutex.Lock()
	defer p.reqMutex.Unlock()

	// add call to pending
	p.mutex.Lock()
	// start at 1, because IDs with value of 0 will be omitted to allow
	// rpc notifications (instead of requests)
	p.seq++
	call.ID = p.seq
	p.pending[call.ID] = call
	p.mutex.Unlock()

	// encode and send
	err := p.out.Encode(call)
	if err != nil {
		p.mutex.Lock()
		delete(p.pending, call.ID)
		p.mutex.Unlock()

		call.Error = err
		call.done()
	}
}

func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		log.Println("rpc: discarding Call reply due to insufficient Done chan capacity")
	}
}

func (p *Peer) Handle(method string, handler Handler) {
	p.handlerMutex.Lock()
	p.handler[method] = handler
	p.handlerMutex.Unlock()
}

func (p *Peer) CallSync(method string, params interface{}, reply interface{}) error {
	call := <-p.Call(method, params, reply, make(chan *Call, 1)).Done
	return call.Error
}

func (p *Peer) Call(method string, params interface{}, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("peer: done channel is unbuffed")
	}

	call := &Call{
		Method: method,
		Params: params,
		Reply:  reply,
		Done:   done,
	}

	p.send(call)
	return call
}
