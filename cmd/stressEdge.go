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
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
	"golang.org/x/time/rate"
)

const (
	defaultStressEdgeBatchSize int = 1
)

type StressEdgeOpts struct {
	enableToss            bool
	clients               int
	vertexes              int
	rateLimit             int
	batchSize             int
	loopForever           bool
	independentClientRank bool
}

var stressEdgeOpts StressEdgeOpts

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

func doStressEdge(client *storage.GraphStorageServiceClient,
	spaceID nebula.GraphSpaceID,
	partID nebula.PartitionID,
	edgeType nebula.EdgeType,
	edges []Edge) (*storage.ExecResponse, error) {

	nebulaEdges := []*storage.NewEdge_{}
	glog.Infof("batch inserting %d edges", len(edges))
	for _, edge := range edges {
		srcData := [8]byte{}
		dstData := [8]byte{}
		binary.LittleEndian.PutUint64(srcData[:], uint64(edge.src))
		binary.LittleEndian.PutUint64(dstData[:], uint64(edge.dst))
		// binary.LittleEndian.PutUint64(srcData[:], uint64(1))
		// binary.LittleEndian.PutUint64(dstData[:], uint64(2))

		propIdx := &nebula.Value{
			SVal: []byte(edge.idx),
		}
		props := []*nebula.Value{propIdx}
		eKey := storage.EdgeKey{
			Src: &nebula.Value{
				SVal: srcData[:],
			},
			Dst: &nebula.Value{
				SVal: dstData[:],
			},
			// Ranking:  rank,
			// Ranking: int64(dst),
			Ranking:  edge.rank,
			EdgeType: edgeType,
		}
		// fmt.Printf("key: %+v\n", eKey)
		nebulaEdges = append(nebulaEdges, &storage.NewEdge_{
			Key:   &eKey,
			Props: props,
		})
	}
	parts := map[nebula.PartitionID][]*storage.NewEdge_{
		int32(partID): nebulaEdges,
	}
	req := storage.AddEdgesRequest{
		SpaceID: spaceID,
		Parts:   parts,
	}

	if stressEdgeOpts.enableToss {
		return client.ChainAddEdges(&req)
	} else {
		return client.AddEdges(&req)
	}
}

func stressEdge() {
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	defer raftCluster.Close()
	glog.Infof("toss: %+v", stressEdgeOpts.enableToss)

	clients := []gonebula.NebulaClient{}
	for i := 0; i < stressEdgeOpts.clients; i++ {
		client := gonebula.NewDefaultNebulaClient(i, raftCluster)
		if err := client.ResetConn(); err != nil {
			glog.Fatalf("failed creatint nebula client: %+v", err)
		}
		clients = append(clients, client)
	}

	limit := rate.Every(time.Microsecond * time.Duration(stressEdgeOpts.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	var wg sync.WaitGroup
	wg.Add(len(clients))
	fmt.Printf("inserting edges...\n")
	ei := getEdgeItem("known2")
	if ei == nil {
		panic("failed getting edge known2")
	}
	etype := ei.EdgeType

	for i := range clients {
		// go func(id int, client *storage.GraphStorageServiceClient) {
		go func(id int) {
			client := clients[id]
			edges := []Edge{}
			var rank int64 = 0
			if stressEdgeOpts.independentClientRank {
				rank = int64(id)
			}
			for stressEdgeOpts.loopForever {
				for from := 0; from < stressEdgeOpts.vertexes; from++ {
					for to := 0; to < stressEdgeOpts.vertexes; to++ {
						idx := fmt.Sprintf("%d-value1-%d", from, to)
						glog.V(2).Infof("prepare edge with value: %s", idx)
						edge := Edge{
							src:  int64(from),
							dst:  int64(to),
							idx:  idx,
							rank: rank,
							// rank: int64(id),
						}
						edges = append(edges, edge)
						if len(edges) < stressEdgeOpts.batchSize {
							if from < stressEdgeOpts.vertexes-1 || to < stressEdgeOpts.vertexes-1 {
								continue
							} else {
								// send request
								glog.V(2).Infof("reach end, sending %d edges", len(edges))
							}
						}

						glog.V(2).Infof("sending %d edges", len(edges))
						limiter.Wait(ctx)
						// resp, err := doStressEdge(client.GetClient(), globalSpaceID, globalPartitionID, 2, edges)
						resp, err := doStressEdge(client.GetClient(), globalSpaceID, globalPartitionID, etype, edges)
						edges = []Edge{}
						fmt.Printf("insert resp: %+v, err: %+v\n", resp, err)
						if err != nil {
							// panic(err)
							if strings.Contains(err.Error(), "i/o timeout") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "Invalid data length") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "Not enough frame size") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "put failed: out of sequence response") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "Bad version in") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "broken pipe") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "out of sequence response") {
								client.ResetConn()
							} else if strings.Contains(err.Error(), "EOF") {
								client.ResetConn()
							} else {
								// fmt.Printf("fuck: %+v\n", err)
								panic(err)
							}

							continue
						}

						if len(resp.Result_.FailedParts) == 0 {
							// ignore
						} else {
							fpart := resp.Result_.FailedParts[0]
							// fmt.Println(fpart)
							switch fpart.Code {
							case nebula.ErrorCode_E_LEADER_CHANGED:
							case nebula.ErrorCode_E_OUTDATED_TERM:
								// if fpart.Leader != nil {
								// 	leaderAddr := fmt.Sprintf("%s:%d", fpart.Leader.Host, fpart.Leader.Port)
								// 	fmt.Printf("connecting to leader %s for client %d\n", leaderAddr, id)
								// }
								glog.Warningf("error inserting edge, leader change: %+v", resp.Result_.FailedParts)
								client.ResetConn()
							case nebula.ErrorCode_E_CONSENSUS_ERROR:
							case nebula.ErrorCode_E_WRITE_WRITE_CONFLICT:
								// client.ResetConn(stressEdgeSpaceID, stressEdgePartID)
								// ignore
							default:
								glog.Warningf("unknown error inserting edge: %+v", resp.Result_.FailedParts)
								client.ResetConn()
								// ignore
							}
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
	glog.Info("done inserting edges...\n")
}

func init() {
	rootCmd.AddCommand(stressEdgeCmd)

	stressEdgeCmd.Flags().BoolVarP(&stressEdgeOpts.enableToss, "toss", "t", true, "enable toss")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeOpts.clients, "clients", "c", 1, "concurrent clients")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeOpts.vertexes, "vertexes", "x", 1, "vertexes")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeOpts.rateLimit, "rateLimit", "r", 1000, "rate limit(request per r us)")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeOpts.batchSize, "batch", "b", defaultStressEdgeBatchSize, "batch size")
	stressEdgeCmd.Flags().BoolVarP(&stressEdgeOpts.loopForever, "forever", "f", false, "loop forever")
	stressEdgeCmd.Flags().BoolVarP(&stressEdgeOpts.independentClientRank, "independentClientRank", "k", false, "independent rank for each client")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stressEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stressEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
