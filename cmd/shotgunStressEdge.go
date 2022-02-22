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
	"fmt"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
)

// shotgunStressEdgeCmd represents the shotgunStressEdge command
var shotgunStressEdgeCmd = &cobra.Command{
	Use:   "shotgunStressEdge",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		shotgunStressEdge()
	},
}

type ShotgunEdgeOpts struct {
	clients    int
	enableToss bool
	vertexes   int
	rateLimit  int
	batchSize  int
}

var shotgunEdgeOpts ShotgunEdgeOpts

func shotgunStressEdge() {
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	defer raftCluster.Close()
	glog.Infof("toss: %+v", shotgunEdgeOpts.enableToss)

	clients := []gonebula.NebulaClient{}
	for _, p := range raftCluster.GetPeers() {
		addr := fmt.Sprintf("%s:%d", p.GetHost(), 9779)
		for i := 0; i < shotgunEdgeOpts.clients; i++ {
			client := gonebula.NewFixedTargetNebulaClient(addr)
			if err := client.ResetConn(); err != nil {
				glog.Fatalf("failed creating nebula client: %+v", err)
			}
			clients = append(clients, client)
		}
	}

	// limit := rate.Every(time.Microsecond * time.Duration(shotgunEdgeOpts.rateLimit))
	// limiter := rate.NewLimiter(limit, 1024)
	// ctx := context.TODO()

	var wg sync.WaitGroup
	wg.Add(len(clients))
	fmt.Printf("inserting edges...\n")
	for i := range clients {
		// go func(id int, client *storage.GraphStorageServiceClient) {
		go func(id int) {
			client := clients[id]
			edges := []Edge{}
			for from := 0; from < shotgunEdgeOpts.vertexes; from++ {
				for to := 0; to < shotgunEdgeOpts.vertexes; to++ {
					idx := fmt.Sprintf("%d-value1-%d", from, to)
					glog.V(2).Infof("prepare edge with value: %s", idx)
					edge := Edge{
						src:  int64(from),
						dst:  int64(to),
						idx:  idx,
						rank: int64(id),
					}
					edges = append(edges, edge)
					if len(edges) < shotgunEdgeOpts.batchSize {
						if from < shotgunEdgeOpts.vertexes-1 || to < shotgunEdgeOpts.vertexes-1 {
							continue
						} else {
							// send request
							glog.V(2).Infof("reach end, sending %d edges", len(edges))
						}
					}

					glog.V(2).Infof("sending %d edges", len(edges))
					// limiter.Wait(ctx)
					resp, err := doStressEdge(client.GetClient(), globalSpaceID, globalPartitionID, 2, edges)
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
			wg.Done()
		}(i)
		// }(i, clients[i])

		// fmt.Println(getResp)
	}

	wg.Wait()
	glog.Info("done inserting edges...\n")

}

func init() {
	rootCmd.AddCommand(shotgunStressEdgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// shotgunStressEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// shotgunStressEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	shotgunStressEdgeCmd.Flags().IntVarP(&shotgunEdgeOpts.clients, "clients", "c", 1, "clients")
	shotgunStressEdgeCmd.Flags().BoolVarP(&shotgunEdgeOpts.enableToss, "toss", "t", true, "enalbe toss")
	shotgunStressEdgeCmd.Flags().IntVarP(&shotgunEdgeOpts.vertexes, "vertexes", "x", 1, "number of vertexes")
	shotgunStressEdgeCmd.Flags().IntVarP(&shotgunEdgeOpts.rateLimit, "rateLimit", "r", 1000, "rate limit(request per us)")
	shotgunStressEdgeCmd.Flags().IntVarP(&shotgunEdgeOpts.batchSize, "batch", "b", 1, "batch size")
}
