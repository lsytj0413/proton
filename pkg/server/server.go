// Package server provide the HelloServer implement
package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lsytj0413/golang-project-template/pb"
	"github.com/lsytj0413/golang-project-template/pkg/utils"
)

// nolint
type HelloServer struct {
	pb.UnimplementedHelloServiceServer
}

// nolint
func NewHelloServer() *HelloServer {
	return &HelloServer{}
}

// nolint
func (s *HelloServer) Hello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		//nolint
		return nil, fmt.Errorf("MUST specify incoming metadata")
	}

	requestID := func() string {
		ids := md.Get("Request-Id")
		if len(ids) > 0 {
			return ids[0]
		}

		return ""
	}()

	if err := grpc.SendHeader(ctx, metadata.New(map[string]string{
		"Request-Id": "r_" + requestID,
	})); err != nil {
		return nil, fmt.Errorf("cannot send header for response, %w", err)
	}

	return &pb.HelloResponse{
		Message:     utils.GenerateResponseMessage(in.Name),
		CurrentTime: timestamppb.Now(),
	}, nil
}
