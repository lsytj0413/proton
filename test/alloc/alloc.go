package alloc

import (
	"sync"
	"sync/atomic"
)

// nolint
type SegmentAllocer struct {
	cur  *Segment
	next *Segment

	Max     uint64
	nexting bool

	l sync.Mutex

	GetNextSegment func(lastMax uint64)
}

// nolint
func (s *SegmentAllocer) AddSegment(ss *Segment) {
	s.l.Lock()
	defer s.l.Unlock()

	if s.next != nil {
		panic("22")
	}
	s.next = ss
	s.Max = ss.Max

	s.nexting = false
}

// nolint
func (s *SegmentAllocer) Next() uint64 {
	ss := func() *Segment {
		s.l.Lock()
		defer s.l.Unlock()

		if s.cur != nil {
			return s.cur
		}

		s.cur = s.next
		s.next = nil
		return s.cur
	}()

	if ss == nil {
		// If this happened
		panic("nil segment")
	}

	v := atomic.AddUint64(&(ss.Cur), 1)
	if v <= ss.Min+(ss.Max-ss.Min)/2 {
		return v
	}

	// 加载下一个 segment，最好是能排除一些情况
	s.accquireNext(ss.Max)
	return v
}

func (s *SegmentAllocer) accquireNext(lastMax uint64) {
	s.l.Lock()
	defer s.l.Unlock()

	if s.next != nil {
		return
	}

	// 这个比较也不行，还是会有重复
	// if s.Max == lastMax {

	// }
	if !s.nexting {
		s.nexting = true
		s.GetNextSegment(lastMax)
	}
}

// nolint
type Segment struct {
	Min uint64
	Max uint64

	Cur uint64
}

// nolint
// AllocSrv ...
type AllocSrv struct {
	MaxSequence uint64

	l          sync.Mutex
	kk         map[string]uint64
	initialSeq uint64

	ks map[string]*SegmentAllocer

	ch chan *Req
}

// nolint
type Req struct {
	key string
	ch  chan uint64
}

func (s *AllocSrv) run() {
	for req := range s.ch {
		if s.kk == nil {
			s.kk = map[string]uint64{}
		}

		v := s.kk[req.key]
		if v < s.MaxSequence {
			v++
			s.kk[req.key] = v
			req.ch <- v
		} else {
			s.MaxSequence += 10
			v++
			s.kk[req.key] = v
			req.ch <- v
		}
	}
}

// nolint
func (s *AllocSrv) next2(key string) uint64 {
	req := &Req{
		key: key,
		ch:  make(chan uint64),
	}
	s.ch <- req
	return <-req.ch
}

// nolint
func (s *AllocSrv) next1(key string) uint64 {
	s.l.Lock()
	defer s.l.Unlock()

	if s.kk == nil {
		s.kk = map[string]uint64{}
	}

	v := s.kk[key]
	if v < s.MaxSequence {
		v++
		s.kk[key] = v
		return v
	}

	s.MaxSequence += 10
	v++
	s.kk[key] = v
	return v
}

// nolint
func (s *AllocSrv) next3(key string) uint64 {
	sa := func() *SegmentAllocer {
		s.l.Lock()
		defer s.l.Unlock()

		if s.ks == nil {
			s.ks = map[string]*SegmentAllocer{}
		}

		sa, ok := s.ks[key]
		if !ok {
			sa = &SegmentAllocer{
				Max: s.MaxSequence,
				cur: &Segment{
					Min: s.initialSeq,
					Max: s.MaxSequence,
					Cur: s.initialSeq,
				},
			}
			sa.GetNextSegment = func(lastMax uint64) {
				go func() {
					s.l.Lock()
					defer s.l.Unlock()

					if lastMax < s.MaxSequence {
						// 这里不好处理了，因为有可能有其他 key 已经来更新过 max
						// lastMax 有可能是有小于 s.Max 的正常情况的，这种情况下需要填充一个 Segment（lastMax - s.Max）
						// 但是，这里没有办法确定到底是属于哪种情况（重复的 GetNextSegment 请求？）
						// 看起来接收端没法处理这个问题了，需要在发送端处理？

						// 如果发送端确保只请求一次，那么此处直接 Add 即可
						sa.AddSegment(&Segment{
							Min: lastMax + 1,
							Max: s.MaxSequence,
							Cur: lastMax + 1,
						})
						return
					}

					if lastMax > s.MaxSequence {
						panic("wrong lastMax")
					}

					// 需要去重
					s.MaxSequence += 10
					sa.AddSegment(&Segment{
						Min: s.MaxSequence - 10,
						Max: s.MaxSequence,
						Cur: s.MaxSequence - 10,
					})
				}()
			}
			s.ks[key] = sa
		}

		return sa
	}()

	return sa.Next()
}

// Next ...
func (s *AllocSrv) Next(key string) uint64 {
	return s.next2(key)
}

// 双 Buffer 实现：
// 1. 一个 key 对应一个 SegmentAllocer
// 2. 一个 SegmenAllocer 包含一个 segment 组成的链
// 3. 一个 Segment 包含最小值、最大值、当前分配值
// 4. 通过 atomic 去获取，如果值不多了则进行下个 buffer 加载；

// 经过测试，这个使用双 buffer 并没有太大的优势，反倒变得非常复杂
