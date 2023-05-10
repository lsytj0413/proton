package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lsytj0413/proton/pb"
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(),
	)
	conn, err := grpc.Dial("127.0.0.1:8080", opts...)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	cli := pb.NewAllocerServiceClient(conn)

	n := 10000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			last := uint64(0)
			key := uuid.New().String()
			for j := 0; j < rand.Intn(500)+100; j++ {
				cur, err := cli.Alloc(context.Background(), &pb.AllocRequest{
					Key: key,
				})
				if err != nil {
					panic(err)
				}

				if cur.ID <= last {
					panic(fmt.Errorf("key [%v] last [%v], cur [%v]", key, last, cur.ID))
				}
				last = cur.ID

				if rand.Intn(100) == 1 {
					fmt.Printf("got: %v, %v\n", key, last)
				}
			}
		}()
	}

	wg.Wait()
	fmt.Printf("done\n")
}
