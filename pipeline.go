package pipeline

import (
	"sync"
)

type Pipeline struct {
	Err           error
	setErrOnce    sync.Once
	wg            sync.WaitGroup
	done          chan struct{}
	closeDoneOnce sync.Once
	closed        bool
}

type Pipe struct {
	c    chan func()
	done chan struct{}
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		done: make(chan struct{}),
	}
}

func (p *Pipeline) Error(err error) {
	p.setErrOnce.Do(func() {
		p.Err = err
	})
	p.Close()
}

func (p *Pipeline) Add() {
	p.wg.Add(1)
}

func (p *Pipeline) Done() {
	p.wg.Done()
}

func (p *Pipeline) Wait() {
	p.wg.Wait()
}

func (p *Pipeline) NewPipe(bufferSize int) *Pipe {
	if p.closed {
		panic("pipeline closed")
	}
	var c chan func()
	if bufferSize > 0 {
		c = make(chan func(), bufferSize)
	} else {
		c = make(chan func())
	}
	return &Pipe{
		c:    c,
		done: p.done,
	}
}

func (p *Pipeline) Close() {
	p.closeDoneOnce.Do(func() {
		close(p.done)
		p.closed = true
	})
}

func (p *Pipe) Do(f func()) bool {
	select {
	case p.c <- f:
	case <-p.done:
		return false
	}
	return true
}

func (p *Pipe) ProcessFunc(cb func()) {
	for {
		select {
		case fn := <-p.c:
			fn()
			cb()
		case <-p.done:
			return
		}
	}
}

func (p *Pipe) Process() {
	p.ProcessFunc(func() {})
}

func (p *Pipe) ParallelProcess(n int) {
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case fn := <-p.c:
					fn()
				case <-p.done:
					return
				}
			}
		}()
	}
	wg.Wait()
}

func (p *Pipe) Len() int {
	return len(p.c)
}
