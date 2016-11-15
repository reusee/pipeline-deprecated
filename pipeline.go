package pipeline

import (
	"sync"
	"sync/atomic"
)

type Pipeline struct {
	Err               error
	setErrOnce        sync.Once
	wg                sync.WaitGroup
	done              chan struct{}
	closeDoneOnce     sync.Once
	closed            bool
	count             int64
	jobWaitGroups     map[string]*sync.WaitGroup
	jobWaitGroupsLock sync.Mutex
}

type Pipe struct {
	c    chan func()
	done chan struct{}
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		done:          make(chan struct{}),
		jobWaitGroups: make(map[string]*sync.WaitGroup),
	}
}

func (p *Pipeline) Error(err error) {
	p.setErrOnce.Do(func() {
		p.Err = err
	})
	p.Close()
}

func (p *Pipeline) Add() {
	atomic.AddInt64(&p.count, 1)
	p.wg.Add(1)
}

func (p *Pipeline) Done() {
	atomic.AddInt64(&p.count, -1)
	p.wg.Done()
}

func (p *Pipeline) Count() int64 {
	return atomic.LoadInt64(&p.count)
}

func (p *Pipeline) Wait() {
	p.wg.Wait()
}

func (p *Pipeline) AddJob(job string) {
	p.jobWaitGroupsLock.Lock()
	wg, ok := p.jobWaitGroups[job]
	if !ok {
		wg = new(sync.WaitGroup)
		p.jobWaitGroups[job] = wg
	}
	p.jobWaitGroupsLock.Unlock()
	wg.Add(1)
}

func (p *Pipeline) DoneJob(job string) {
	p.jobWaitGroupsLock.Lock()
	wg := p.jobWaitGroups[job]
	p.jobWaitGroupsLock.Unlock()
	wg.Done()
}

func (p *Pipeline) WaitJob(job string) {
	p.jobWaitGroupsLock.Lock()
	wg := p.jobWaitGroups[job]
	p.jobWaitGroupsLock.Unlock()
	wg.Wait()
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
