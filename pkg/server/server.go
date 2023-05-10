// Package server provide the HelloServer implement
package server

import (
	"context"
	"hash/crc32"
	"sync"

	"github.com/lsytj0413/proton/pb"
)

// nolint
type HelloServer struct {
	pb.UnsafeAllocerServiceServer

	m metadataStore
}

var (
	_ pb.AllocerServiceServer = (*HelloServer)(nil)
)

// nolint
func NewHelloServer() *HelloServer {
	return &HelloServer{
		m: &mockMetadataStore{
			b: &Bundle{
				MaxSequence: 10,
				initialSeq:  0,
			},
		},
	}
}

// nolint
type Bundle struct {
	ID       uint64
	Rivision uint64

	Min uint32
	Max uint32

	MaxSequence uint64

	initialSeq uint64
	kk         map[string]uint64
	l          sync.Mutex
}

func (b *Bundle) Alloc(k string) (uint64, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.kk == nil {
		b.kk = map[string]uint64{}
	}

	if _, ok := b.kk[k]; !ok {
		b.kk[k] = b.initialSeq
	}

	v := b.kk[k]
	if v < b.MaxSequence {
		v++
		b.kk[k] = v
		return v, nil
	}

	b.MaxSequence += 10
	v++
	b.kk[k] = v
	return v, nil
}

type metadataStore interface {
	FindBundle(k uint32) (*Bundle, error)
}

type mockMetadataStore struct {
	b *Bundle
}

func (m *mockMetadataStore) FindBundle(k uint32) (*Bundle, error) {
	return m.b, nil
}

// Alloc ...
func (s *HelloServer) Alloc(ctx context.Context, in *pb.AllocRequest) (*pb.AllocResponse, error) {
	// Split Key's structure
	k := crc32.ChecksumIEEE([]byte(in.Key))
	_ = k

	// Validate is current key's bundle belong to this node
	b, err := s.m.FindBundle(k)
	if err != nil {
		return nil, err
	}

	vv, err := b.Alloc(in.Key)
	if err != nil {
		return nil, err
	}

	return &pb.AllocResponse{
		Key: in.Key,
		ID:  vv,
	}, nil
}
