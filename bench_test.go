package pipeline

import (
	"testing"
)

func BenchmarkSinglePipe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pipeline := NewPipeline()
		p1 := pipeline.NewPipe(0)
		num := 1024
		n := 0
		go func() {
			for i := 0; i < num; i++ {
				pipeline.Add()
				if !p1.Do(func() {
					n++
					pipeline.Done()
				}) {
					return
				}
			}
			pipeline.Wait()
			pipeline.Close()
		}()
		p1.Process()
		if n != num {
			b.Fail()
		}
	}
}
