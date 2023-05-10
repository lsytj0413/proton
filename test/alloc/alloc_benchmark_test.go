package alloc

import (
	"testing"

	"github.com/google/uuid"
)

func Benchmark_AllocSrv_Alloc(b *testing.B) {
	s := &AllocSrv{
		MaxSequence: 10,
		initialSeq:  0,
		ch:          make(chan *Req, 1000),
	}
	go s.run()

	b.RunParallel(func(p *testing.PB) {
		key := uuid.New().String()

		for p.Next() {
			s.Next(key)
		}
	})
}
