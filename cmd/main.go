// Package main is the entrance of project
// nolint
package main

import (
	"context"
	"fmt"
	"net"
	"os"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/lsytj0413/proton/pb"
	"github.com/lsytj0413/proton/pkg/server"
	"github.com/lsytj0413/proton/pkg/utils/version"
)

func main() {
	fmt.Printf("%s\n", version.Get().Pretty())

	lis, err := net.Listen("tcp", os.Getenv("GRPC_ENDPOINT"))
	if err != nil {
		panic(err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterAllocerServiceServer(grpcServer, server.NewHelloServer())

	dialOptions := []grpc.DialOption{
		grpc.WithBlock(),
	}
	cfg := clientv3.Config{
		DialOptions: dialOptions,
		TLS:         nil,
		Endpoints:   []string{"http://127.0.0.1:2379"},
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}
	nodeId := os.Getenv("NODE_ID")
	if nodeId == "" {
		panic(fmt.Errorf("Must specify node id"))
	}
	leaseId, err := client.Grant(context.Background(), 10)
	if err != nil {
		panic(err)
	}
	fmt.Printf("got lease: %v\n", leaseId.ID)
	respCh, err := client.KeepAlive(context.Background(), leaseId.ID)
	if err != nil {
		panic(err)
	}
	go func() {
		for kaResp := range respCh {
			fmt.Printf("KeepAliveResponse: %v, %v\n", kaResp.Revision, kaResp.TTL)
		}

		fmt.Printf("KeepAlive closed")
	}()

	resp, err := client.Txn(context.Background()).If(
		clientv3.Compare(clientv3.CreateRevision("/brokers/"+nodeId), "=", 0),
	).Then(
		clientv3.OpPut("/brokers/"+nodeId, nodeId, clientv3.WithLease(leaseId.ID)),
	).Commit()
	if err != nil {
		panic(err)
	}
	if !resp.Succeeded {
		fmt.Printf("node already exists")
		return
	}
	fmt.Printf("register node %v done\n", nodeId)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ctx

	grpcServer.Serve(lis)
}

// 1. key 按照 hash 结果分 group，然后每个 group 共享一个 max_seq（用于降低保存 max_seq 的数量），group 也需要能自动分裂/合并
// 2. key 按照 hash 结果分 bundle，一个服务实例负责一个/多个 bundle（bundle 中保存下界/上界，且需要能自动分裂/合并/转移 own）
// 3. 同一个 group 的必须在同一个 bundle 吗？（看起来不是必须的，不过当前最好就这么实现，而且如果不同的话可能导致更新 group.max_seq 的时候有很大冲突）
// 4. 这就要求 hash 函数必须相同（MurmurHash/crc64）
// 5. 为什么有了 bundle 还需要 group？
//    5.1 bundle 是 allocer own 的界限，它是把 {tanant}/{namespace} 下的 key 进行划分
//    5.2 group 是 max_seq 保存的界限，它是把 {tenant}/{namespace} 下的 key 进行划分
//    5.3 从上描述来看，它们两个是类似的，那么是否还要分开使用呢？
//    5.4 感觉并不必要，只不过可能导致 bundle 很多？（但是现实是 group 也会很多）

// 1. 中心是一个 etcd，保存所有 broker/dispenser/allocer/alloter/distributor 信息、
//   controller 节点信息（负责 bundle/group 的分裂/合并）、
//   bundle 信息（包含 bundle 分配给哪个 broker）、
//   group 信息（上下界、max_seq 等）
// 2. 内存中保存具体的 key -> cur_seq 信息即可

// 对于 allocer 存活性的管理，是采用 lease，还是类似 kubernetes 使用显示的 heartbeat 更好呢？
// 1. kubernetes 从 heartbeat 迁移到了 lease，主要是因为 lease 带来的更新更少
