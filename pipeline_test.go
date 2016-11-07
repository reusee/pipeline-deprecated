package pipeline

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestPipeline(t *testing.T) {
	pipeline := NewPipeline()
	p1 := pipeline.NewPipe(128)
	p2 := pipeline.NewPipe(128)
	p3 := pipeline.NewPipe(0)

	var n int64
	var num int64 = 1024

	go func() {
		for i := int64(0); i < num; i++ {
			pipeline.Add()

			if !p1.Do(func() {

				if !p2.Do(func() {

					if !p3.Do(func() {
						_ = p3.Len()
						atomic.AddInt64(&n, 1)
						pipeline.Done()
					}) {
						return
					}

				}) {
					return
				}

			}) {
				return
			}

		}
		pipeline.Wait()
		pipeline.Close()
	}()

	go p2.ParallelProcess(16)
	go p3.ParallelProcess(16)

	p1.Process()

	if n != num {
		t.Fatal("wrong n")
	}
}

func TestError(t *testing.T) {
	pipeline := NewPipeline()
	p1 := pipeline.NewPipe(1)
	pipeline.Add()
	p1.Do(func() {
		pipeline.Error(fmt.Errorf("error"))
		pipeline.Done()
	})
	go p1.Process()
	pipeline.Wait()
	if pipeline.Err == nil || pipeline.Err.Error() != "error" {
		t.Fail()
	}
}

func TestReuseClosedPipeline(t *testing.T) {
	pipeline := NewPipeline()
	pipeline.Close()
	func() {
		defer func() {
			p := recover()
			if p == nil {
				t.Fail()
			}
		}()
		pipeline.NewPipe(0)
	}()
}

func TestDone(t *testing.T) {
	pipeline := NewPipeline()
	p1 := pipeline.NewPipe(0)
	pipeline.Close()
	b := false
	p1.Do(func() {
		// should not run
		b = true
	})
	p1.ParallelProcess(4)
	if b {
		t.Fail()
	}
}
