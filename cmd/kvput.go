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
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
	"golang.org/x/time/rate"
)

var (
	kvputClients int
	// kvputPartID    int32
	// kvputSpaceID   int32
	kvputVertexes  int
	kvputRateLimit int
)

// stressEdgeCmd represents the stressEdge command
var kvputCmd = &cobra.Command{
	Use:   "kvput",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		kvput()
	},
}

func kvput() {
	clients := []gonebula.NebulaClient{}
	glog.Infof("%d clients", kvputClients)
	glog.Info("building raft cluster...")
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	defer raftCluster.Close()
	glog.Info("done build raft cluster...")

	glog.Info("building kv clients...")
	for i := 0; i < kvputClients; i++ {
		client := gonebula.NewDefaultNebulaClient(i, raftCluster)
		if err := client.ResetConn(); err != nil {
			glog.Fatalf("error init conn: %+v", err)
		}
		clients = append(clients, client)
	}
	glog.Info("done building kv clients")
	limit := rate.Every(time.Microsecond * time.Duration(kvputRateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	var wg sync.WaitGroup
	wg.Add(len(clients))
	glog.Info("putting kvs...")
	for i := range clients {
		// go func(id int, client *storage.GraphStorageServiceClient) {
		go func(id int) {
			client := clients[id]
			for x := 0; x < kvputVertexes; x++ {
				for y := 0; y < kvputVertexes; y++ {
					key := fmt.Sprintf("key-%d-%d-%d", x, y, id)
					value := fmt.Sprintf("value-%d-%d-%d", x, y, id)
					limiter.Wait(ctx)
					req := storage.KVPutRequest{
						SpaceID: globalSpaceID,
						Parts: map[nebula.PartitionID][]*nebula.KeyValue{
							globalSpaceID: {
								{
									Key:   []byte(key),
									Value: []byte(value),
								},
							},
						},
					}
					resp, err := client.GetClient().Put(&req)
					// glog.Infof("put resp: %+v, err: %+v", resp, err)
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
						} else {
							// panic(err)
							glog.Errorf("unknown error: %+v", err)
							// fmt.Printf("fuck: %+v\n", err)
						}

						continue
					}

					if len(resp.Result_.FailedParts) == 0 {
						glog.V(2).Infof("done putting kv")
					} else {
						fpart := resp.Result_.FailedParts[0]
						switch fpart.Code {
						case nebula.ErrorCode_E_LEADER_CHANGED:
							// if fpart.Leader != nil {
							// 	leaderAddr := fmt.Sprintf("%s:%d", fpart.Leader.Host, fpart.Leader.Port)
							// 	fmt.Printf("connecting to leader %s for client %d\n", leaderAddr, id)
							// }
							glog.Warningf("kvput failed: %+v", resp.Result_.FailedParts)
							client.ResetConn()
						case nebula.ErrorCode_E_CONSENSUS_ERROR:
							// client.ResetConn(kvputSpaceID, kvputPartID)
							// ignore
						default:
							glog.Warningf("kvput failed: %+v", resp.Result_.FailedParts)
							client.ResetConn()
							// ignore
						}
					}

				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	fmt.Printf("done putting kvs...\n")
}

func init() {
	rootCmd.AddCommand(kvputCmd)

	kvputCmd.Flags().IntVarP(&kvputClients, "clients", "c", 1, "concurrent clients")
	kvputCmd.Flags().IntVarP(&kvputVertexes, "vertexes", "x", 1, "loops")
	kvputCmd.Flags().IntVarP(&kvputRateLimit, "rateLimit", "r", 1000, "rate limit(request per r us)")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stressEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stressEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
