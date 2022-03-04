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
	spaceName             string
	partIDArray           []int
	concurrencyPerPart    int
	vertexes              int
	isShotgunMode         bool
	batchSize             int
	rateLimit             int
	loopForever           bool
	independentClientRank bool
	enableToss            bool
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
	glog.Infof("Multiple partition stress starting...\n")
	glog.Infof("MultiPartStressOpts:\n")
	glog.Infof("%+v\n", multiPartStressOpts)

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
	if err != nil {
		glog.Fatal(err)
	}

	// Construct raft cluster
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	defer raftCluster.Close()

	// Build limiter
	limit := rate.Every(time.Microsecond * time.Duration(multiPartStressOpts.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	// Build partID array
	partIDArray := []int{}
	if len(multiPartStressOpts.partIDArray) > 0 {
		// Validate partID
		for _, partID := range multiPartStressOpts.partIDArray {
			if partID > int(totalPartNum) {
				glog.Fatalf("the partID %d in the parameter does not exist in the space given", partID)
			}
		}
		partIDArray = multiPartStressOpts.partIDArray
	} else {
		partIDArray = getPartIDArray(int(totalPartNum))
	}

	// Send requests to each part
	glog.Infof("parts list:%v\n", partIDArray)

	var wg sync.WaitGroup
	wg.Add(len(partIDArray) * multiPartStressOpts.concurrencyPerPart)

	for _, curPart := range partIDArray {
		// Constrcut storage clients
		glog.Infof("preparing to insert edges to part %d, constructing clients...", curPart)

		sclients, err := constructStorageClients(raftCluster)
		if err != nil {
			glog.Fatal(err)
		}

		// Send requests concurrently
		go conccurentStressOnSinglePart(sclients, spaceID, int32(curPart), edgeItem, *limiter, ctx, &wg)
	}
	wg.Wait()
}

// constructStorageClients builds n storage clients
func constructStorageClients(raftCluster *raft.RaftCluster) ([]gonebula.NebulaClient, error) {
	clients := []gonebula.NebulaClient{}
	// Iterate the hosts list and build connection
	for _, p := range raftCluster.GetPeers() {
		addr := fmt.Sprintf("%s:%d", p.GetHost(), 9779)
		for i := 0; i < multiPartStressOpts.concurrencyPerPart; i++ {
			client := gonebula.NewFixedTargetNebulaClient(addr)
			if err := client.ResetConn(); err != nil {
				glog.Fatalf("failed creating nebula client: %+v", err)
			}
			clients = append(clients, client)
		}
	}
	return clients, nil
}

// conccurentStressOnSinglePart uses goroutine to concurrently insert edges into the specified graph id and partition id.
// edges will be created
func conccurentStressOnSinglePart(
	sclients []gonebula.NebulaClient,
	spaceID nebula.GraphSpaceID,
	partID nebula.PartitionID,
	edgeItem *meta.EdgeItem,
	limiter rate.Limiter,
	ctx context.Context,
	wg *sync.WaitGroup) {
	glog.Infof("sending request to part %d", partID)

	// Get edge type
	etype := edgeItem.EdgeType

	// var wg sync.WaitGroup
	// wg.Add(int(len(sclients)))
	glog.Infof("client concurrency: %d", len(sclients))
	for i := range sclients {
		go func(id int) {
			defer wg.Done()

			client := sclients[id]
			edges := []Edge{}
			var rank int64 = 0
			if multiPartStressOpts.independentClientRank {
				rank = int64(id)
			}

			// kind of tricky, once and multiPartStressOpts.loopForever together make the loop
			// either run once or forever
			glog.Infof("client %d running loop, vertexes %d", id, multiPartStressOpts.vertexes)
			once := true
			for multiPartStressOpts.loopForever || once {
				once = false

				for from := 0; from < multiPartStressOpts.vertexes; from++ {
					for to := 0; to < multiPartStressOpts.vertexes; to++ {
						idx := fmt.Sprintf("%d-value1-%d-from-%d", from, to, id)
						glog.V(2).Infof("prepare edge with value: %s", idx)
						edge := Edge{
							src:  int64(from),
							dst:  int64(to),
							idx:  idx,
							rank: rank,
						}
						edges = append(edges, edge)

						if len(edges) < multiPartStressOpts.batchSize {
							if from < multiPartStressOpts.vertexes-1 || to < multiPartStressOpts.vertexes-1 {
								continue
							} else {
								// batch size fullfiled, send request
								glog.V(2).Infof("reach batch size, sending %d edges", len(edges))
							}
						}

						glog.V(2).Infof("sending %d edges", len(edges))
						limiter.Wait(ctx)
						resp, err := doStressEdge(
							client.GetClient(), spaceID, partID, etype, edges, multiPartStressOpts.enableToss)
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
								glog.Errorf("Panic error during insertion: %+v\n", err.Error())
								panic(err)
							}
							continue
						}

						if len(resp.Result_.FailedParts) == 0 {
							// ignore
						} else {
							fpart := resp.Result_.FailedParts[0]
							switch fpart.Code {
							case nebula.ErrorCode_E_LEADER_CHANGED:
							case nebula.ErrorCode_E_OUTDATED_TERM:
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

		}(i)
	}
	glog.Info("done inserting edges...\n")
}

// getPartIDArray returns an array consists of consecutive ints from 1 to totalPartNum
func getPartIDArray(totalPartNum int) []int {
	a := make([]int, totalPartNum)
	for i := range a {
		a[i] = i + 1
	}
	return a
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
		"spaceName", "n", "", "the name of the space")
	multiPartStressCmd.Flags().IntSliceVarP(&multiPartStressOpts.partIDArray,
		"partIDs", "", []int{}, "the partID where the requests sent to")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.concurrencyPerPart,
		"concurrence", "c", 1, "the number of concurrent clients per partition")
	multiPartStressCmd.Flags().BoolVarP(&multiPartStressOpts.isShotgunMode,
		"isShotgunMode", "", false, "whether to enable shotgun mode")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.batchSize,
		"batch", "b", 1, "batch size")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.rateLimit,
		"rateLimit", "r", 100, "rate limit(request per r us)")
	multiPartStressCmd.Flags().IntVarP(&multiPartStressOpts.vertexes,
		"vertexes", "x", 10, `the number of vertexes. This is used to generate edges, 
		the total number of edges is v*v. v*v edges will be inserted per client per part`)
	multiPartStressCmd.Flags().BoolVarP(&multiPartStressOpts.loopForever,
		"loopForever", "l", false, "whether to keep inserting forever")
	multiPartStressCmd.Flags().BoolVarP(&multiPartStressOpts.independentClientRank,
		"independentClientRank", "k", false, "independent rank for each client")
	multiPartStressCmd.Flags().BoolVarP(&multiPartStressOpts.enableToss,
		"enableToss", "t", false, "whether enable toss. If enabled, ChainAddEdges() will be called instead of AddEdges()")
}
