package pipeline

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// Pipeline implements publish/subscribe messaging paradigm for a list of modules
type Pipeline interface {
	// Publish publishes arguments to the given list subscribers
	Publish(p *pipeline, args ...interface{})

	// Process processes the pipeline result
	Process(p *pipeline, fn interface{}, timeout int, background context.Context)
}

type funcResult struct {
	channel chan []reflect.Value
	created bool
}

type pipeline struct {
	modules []*subscriber
	argChan chan []reflect.Value
	runmtx  sync.RWMutex
	running bool
	retChan chan []reflect.Value
}

type subscriber struct {
	callback reflect.Value
	queue    chan []reflect.Value
}

func New(num int) *pipeline {
	return &pipeline{
		running: false,
		argChan: make(chan []reflect.Value, num),
		retChan: make(chan []reflect.Value, num),
	}
}

func (p *pipeline) withModule(fun interface{}) *pipeline {
	p.modules = append(p.modules, newSubscriber(fun, 100))
	return p
}

func (b *pipeline) Publish(p *pipeline, args ...interface{}) {
	rArgs := buildHandlerArgs(args)

	p.argChan <- rArgs
}

func (b *pipeline) Process(p *pipeline, fn interface{}, timeout int, ctx context.Context) {
	for {
		select {
		case msg := <-p.retChan:
			reflect.ValueOf(fn).Call(msg)
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			return
		default:

		}
	}
}

func (p *pipeline) Start() {
	if p.running {
		return
	}
	p.runmtx.Lock()
	if !p.running {
		p.running = true
		go func() {
			for {
				select {
				case qArg := <-p.argChan:
					retCall := p.modules[0].callback.Call(qArg)
					for i := 1; i < len(p.modules); i++ {
						retCall = p.modules[i].callback.Call(retCall)
					}
					p.retChan <- retCall
				default:
				}
			}
		}()
	} // end of if
	p.runmtx.Unlock()
}

func (b *pipeline) newTopicRet() *funcResult {
	return &funcResult{
		channel: make(chan []reflect.Value, 100),
		created: true,
	}
}

func newSubscriber(fn interface{}, qsize int) *subscriber {
	return &subscriber{
		callback: reflect.ValueOf(fn),
		queue:    make(chan []reflect.Value, qsize),
	}

}

func buildHandlerArgs(args []interface{}) []reflect.Value {
	reflectedArgs := make([]reflect.Value, 0)

	for _, arg := range args {
		reflectedArgs = append(reflectedArgs, reflect.ValueOf(arg))
	}

	return reflectedArgs
}
