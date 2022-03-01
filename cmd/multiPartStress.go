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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
	"golang.org/x/time/rate"
)

type MultiPartStressOpts struct {
	concurrencyPerPart int
	vertexes           int
	spaceName          string
	partIDArray        []int
	isShotgunMode      bool
	batchSize          int
	rateLimit          int
}

var multiPartStressOpts MultiPartStressOpts

// multiPartStressCmd represents the multiPartStress command
var multiPartStressCmd = &cobra.Command{
	Use:   "multiPartStress",
	Short: "send requests to specified storage partition",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		multiPartStress()
	},
}

// multiPartStress constructs multuple storage clients to insert edges into each part of the space concurrently.
func multiPartStress() {
	// Configs for meta client
	addr := fmt.Sprintf("%s:%d", "meta1", 9559)
	option := gonebula.MetaOption{
		Timeout:    100 * time.Millisecond,
		BufferSize: 4096,
	}

	// Get the meta client
	mclient, err := gonebula.NewMetaClient(addr, option)

	// Get spaceID and total partition number by space name
	getSpaceResp, err := mclient.GetSpaceByName(multiPartStressOpts.spaceName)
	if err != nil {
		glog.Fatal(err)
	}
	spaceID := getSpaceResp.GetItem().SpaceID
	totalPartNum := getSpaceResp.GetItem().Properties.GetPartitionNum()

	// Get edge items
	edgeItem, err := mclient.GetEdgeItemByName(spaceID, "known2")

	// Construct raft cluster
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	defer raftCluster.Close()

	// Constrcut storage clients
	sclients, err := constructStorageClients(multiPartStressOpts.concurrencyPerPart, raftCluster)

	limit := rate.Every(time.Microsecond * time.Duration(stressEdgeOpts.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	var wg sync.WaitGroup
	wg.Add(int(totalPartNum))

	for curPart := int32(0); curPart < totalPartNum; curPart++ {
		// Send requests concurrently
		conccurentStressOnSinglePart(sclients, spaceID, curPart, edgeItem, *limiter, ctx)

	}
}

// constructStorageClients builds n storage clients
func constructStorageClients(num int, raftCluster *raft.RaftCluster) ([]gonebula.NebulaClient, error) {
	sClients := []gonebula.NebulaClient{}
	for i := 0; i < num; i++ {
		client := gonebula.NewDefaultNebulaClient(i, raftCluster)
		if err := client.ResetConn(); err != nil {
			return nil, fmt.Errorf("error init conn: %+v", err)
		}
		sClients = append(sClients, client)
	}
	return sClients, nil
}

// conccurentStressOnSinglePart uses goroutine to concurrently insert edges into the specified graph id and partition id.
// edges will be created
func conccurentStressOnSinglePart(
	sclients []gonebula.NebulaClient,
	spaceID nebula.GraphSpaceID,
	partID nebula.PartitionID,
	edgeItem *meta.EdgeItem,
	limiter rate.Limiter,
	ctx context.Context) {

	var wg sync.WaitGroup
	wg.Add(int(len(sclients)))

	// Get edge type
	etype := edgeItem.EdgeType

	for i := range sclients {
		go func(id int) {
			client := sclients[id]
			edges := []Edge{}
			var rank int64 = 0
			if stressEdgeOpts.independentClientRank {
				rank = int64(id)
			}

			// kind of tricky, once and stressEdgeOpts.loopForever together make the loop
			// either run once or forever
			glog.Infof("client %d running loop, vertexes %d", id, stressEdgeOpts.vertexes)
			once := true
			for stressEdgeOpts.loopForever || once {
				once = false

				for from := 0; from < stressEdgeOpts.vertexes; from++ {
					for to := 0; to < stressEdgeOpts.vertexes; to++ {
						idx := fmt.Sprintf("%d-value1-%d-from-%d", from, to, id)
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
								// batch size fullfiled, send request
								glog.V(2).Infof("reach batch size, sending %d edges", len(edges))
							}
						}

						glog.V(2).Infof("sending %d edges", len(edges))
						limiter.Wait(ctx)
						resp, err := doStressEdge(client.GetClient(), globalSpaceID, globalPartitionID, etype, edges)
						edges = []Edge{}
						glog.V(2).Infof("insert resp: %+v, err: %+v", resp, err)
						if err != nil {
							switch {
							case strings.Contains(err.Error(), "i/o timeout"):
							case strings.Contains(err.Error(), "Invalid data length"):
							case strings.Contains(err.Error(), "Not enough frame size"):
							case strings.Contains(err.Error(), "put failed: out of sequence response"):
							case strings.Contains(err.Error(), "Bad version in"):
							case strings.Contains(err.Error(), "broken pipe"):
							case strings.Contains(err.Error(), "out of sequence response"):
							case strings.Contains(err.Error(), "EOF"):
								client.ResetConn()
							default:
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
	}
	wg.Wait()
	glog.Info("done inserting edges...\n")
}

func init() {
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// multiPartStressCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// multiPartStressCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.AddCommand(multiPartStressCmd)

	multiPartStressCmd.Flags().StringVarP(&multiPartStressOpts.spaceName,
		"spaceName", "spaceName", "", "the name of the space")
	multiPartStressCmd.Flags().IntSliceVarP(&multiPartStressOpts.partIDArray,
		"partIDs", "partid", []int{}, "the partID where the requests sent to")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.concurrencyPerPart,
		"con", "concurrence",
		1, "the number of concurrent clients per partition")
	multiPartStressCmd.Flags().BoolVarP(&multiPartStressOpts.isShotgunMode,
		"isShotgunMode", "shotgun", false, "whether to enable shotgun mode")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.batchSize, "batch", "b", 1, "batch size")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.rateLimit, "rateLimit", "r", 100, "rate limit(request per r us)")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.vertexes,
		"vertexes", "v", 10, `the number of vertexes. This is used to generate edges, 
		the total number of edges is v*v. v*v edges will be inserted per client per part`)

}
