/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
	"golang.org/x/time/rate"
)

var (
	stressEdgeClients   int
	stressEdgePartID    int32
	stressEdgeSpaceID   int32
	stressEdgeVertexes  int
	stressEdgeRateLimit int
)

// stressEdgeCmd represents the stressEdge command
var stressEdgeCmd = &cobra.Command{
	Use:   "stressEdge",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		stressEdge()
	},
}

func doStressEdge(client *storage.GraphStorageServiceClient, spaceID nebula.GraphSpaceID, partID nebula.PartitionID, edgeType nebula.EdgeType, src int, dst int, rank int64, idx string) (*storage.ExecResponse, error) {
	srcData := [8]byte{}
	dstData := [8]byte{}
	binary.LittleEndian.PutUint64(srcData[:], uint64(src))
	binary.LittleEndian.PutUint64(dstData[:], uint64(dst))
	propIdx := &nebula.Value{
		SVal: []byte(idx),
	}
	props := []*nebula.Value{propIdx}
	eKey := storage.EdgeKey{
		Src: &nebula.Value{
			SVal: srcData[:],
		},
		Dst: &nebula.Value{
			SVal: dstData[:],
		},
		Ranking:  rank,
		EdgeType: edgeType,
	}
	// fmt.Printf("key: %+v\n", eKey)
	edges := []*storage.NewEdge_{
		{
			Key:   &eKey,
			Props: props,
		},
	}
	parts := map[nebula.PartitionID][]*storage.NewEdge_{
		int32(partID): edges,
	}
	req := storage.AddEdgesRequest{
		SpaceID: spaceID,
		Parts:   parts,
	}

	return client.ChainAddEdges(&req)
}

func stressEdge() {
	raftCluster, err := createRaftCluster(stressEdgeSpaceID, stressEdgePartID)
	if err != nil {
		glog.Fatal("failed creating raft cluster: %+v", err)
	}
	defer raftCluster.Close()

	clients := []*NebulaClient{}
	for i := 0; i < stressEdgeClients; i++ {
		client := newNebulaClient(i, raftCluster)
		if err := client.ResetConn(stressEdgeSpaceID, stressEdgePartID); err != nil {
			glog.Fatalf("failed creatint nebula client: %+v", err)
		}
		clients = append(clients, client)
	}

	limit := rate.Every(time.Microsecond * time.Duration(stressEdgeRateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	var wg sync.WaitGroup
	wg.Add(len(clients))
	fmt.Printf("putting kvs...\n")
	for i := range clients {
		// go func(id int, client *storage.GraphStorageServiceClient) {
		go func(id int) {
			client := clients[id]
			for x := 0; x < stressEdgeVertexes; x++ {
				for y := 0; y < stressEdgeVertexes; y++ {
					value := fmt.Sprintf("%d-value1-%d", id, x)
					limiter.Wait(ctx)
					resp, err := doStressEdge(client.client, stressEdgeSpaceID, stressEdgePartID, 2, x, y, int64(id), value)
					if err != nil {

						// panic(err)
						// if strings.Contains(err.Error(), "i/o timeout") {
						// 	client.RestConn(stressEdgeSpaceID, stressEdgePartID)
						// } else if strings.Contains(err.Error(), "Invalid data length") {
						// 	client.RestConn(stressEdgeSpaceID, stressEdgePartID)
						// } else if strings.Contains(err.Error(), "Not enough frame size") {
						// 	client.RestConn(stressEdgeSpaceID, stressEdgePartID)
						// } else if strings.Contains(err.Error(), "put failed: out of sequence response") {
						// 	client.RestConn(stressEdgeSpaceID, stressEdgePartID)
						// } else if strings.Contains(err.Error(), "Bad version in") {
						// 	client.RestConn(stressEdgeSpaceID, stressEdgePartID)
						// } else if strings.Contains(err.Error(), "broken pipe") {
						// 	client.RestConn(stressEdgeSpaceID, stressEdgePartID)
						// } else {
						// 	fmt.Printf("fuck: %+v\n", err)
						// }

						continue
					}

					if len(resp.Result_.FailedParts) == 0 {
						// ignore
					} else {
						fpart := resp.Result_.FailedParts[0]
						// fmt.Println(fpart)
						switch fpart.Code {
						case nebula.ErrorCode_E_LEADER_CHANGED:
							// if fpart.Leader != nil {
							// 	leaderAddr := fmt.Sprintf("%s:%d", fpart.Leader.Host, fpart.Leader.Port)
							// 	fmt.Printf("connecting to leader %s for client %d\n", leaderAddr, id)
							// }
							glog.Warningf("error inserting edge, leader change: %+v", resp.Result_.FailedParts)
							client.ResetConn(stressEdgeSpaceID, stressEdgePartID)
						case nebula.ErrorCode_E_CONSENSUS_ERROR:
						case nebula.ErrorCode_E_WRITE_WRITE_CONFLICT:
							// client.ResetConn(stressEdgeSpaceID, stressEdgePartID)
							// ignore
						default:
							glog.Warningf("unknown error inserting edge: %+v", resp.Result_.FailedParts)
							client.ResetConn(stressEdgeSpaceID, stressEdgePartID)
							// ignore
						}
					}

				}
			}
			wg.Done()
		}(i)
		// }(i, clients[i])

		// fmt.Println(getResp)
	}

	wg.Wait()
	fmt.Printf("done putting kvs...\n")
}

func newNebulaConn(addr string) (*storage.GraphStorageServiceClient, error) {
	timeout := thrift.SocketTimeout(4 * time.Second)
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(65536)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %+v", err)
	}

	if !client.IsOpen() {
		panic("transport is off")
	}

	return client, nil
}

type NebulaClient struct {
	id      int
	client  *storage.GraphStorageServiceClient
	cluster *raft.RaftCluster
}

func newNebulaClient(id int, cluster *raft.RaftCluster) *NebulaClient {
	c := NebulaClient{
		id:      id,
		cluster: cluster,
	}

	return &c
}

func (c *NebulaClient) ResetConn(spaceID nebula.GraphSpaceID, partID nebula.PartitionID) error {
	host, err := c.cluster.GetLeader()
	if err != nil {
		return err
	}

	// TODO specify port
	addr := fmt.Sprintf("%s:%d", host, 9779)
	client, err := newNebulaConn(addr)
	if err != nil {
		return err
	}

	c.client = client
	return nil
}

func RunBasicStorag() {

}

func init() {
	rootCmd.AddCommand(stressEdgeCmd)

	stressEdgeCmd.Flags().Int32VarP(&stressEdgePartID, "partID", "p", 1, "part id")
	stressEdgeCmd.Flags().Int32VarP(&stressEdgeSpaceID, "spaceID", "s", 1, "space id")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeClients, "clients", "c", 1, "concurrent clients")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeVertexes, "vertexes", "x", 1, "vertexes")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeRateLimit, "rateLimit", "r", 1000, "rate limit(request per r us)")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stressEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stressEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
