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
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
	"golang.org/x/time/rate"
)

var (
	kvputClients   int
	kvputPartID    int32
	kvputSpaceID   int32
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
	clients := []*NebulaClient{}
	glog.Infof("%d clients", kvputClients)
	raftCluster, err := createRaftCluster(kvputSpaceID, kvputPartID)
	if err != nil {
		glog.Fatal("failed creating raft cluster: %+v", err)
	}
	defer raftCluster.Close()

	for i := 0; i < kvputClients; i++ {
		client := newNebulaClient(i, raftCluster)
		if err := client.ResetConn(kvputSpaceID, kvputPartID); err != nil {
			glog.Fatalf("error init conn: %+v", err)
		}
		clients = append(clients, client)
	}

	limit := rate.Every(time.Microsecond * time.Duration(kvputRateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	var wg sync.WaitGroup
	wg.Add(len(clients))
	fmt.Printf("putting kvs...\n")
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
						SpaceID: kvputSpaceID,
						Parts: map[nebula.PartitionID][]*nebula.KeyValue{
							kvputPartID: {
								{
									Key:   []byte(key),
									Value: []byte(value),
								},
							},
						},
					}
					resp, err := client.client.Put(&req)
					// glog.Infof("put resp: %+v, err: %+v", resp, err)
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
						switch fpart.Code {
						case nebula.ErrorCode_E_LEADER_CHANGED:
							// if fpart.Leader != nil {
							// 	leaderAddr := fmt.Sprintf("%s:%d", fpart.Leader.Host, fpart.Leader.Port)
							// 	fmt.Printf("connecting to leader %s for client %d\n", leaderAddr, id)
							// }
							glog.Warningf("kvput failed: %+v", resp.Result_.FailedParts)
							client.ResetConn(kvputSpaceID, kvputPartID)
						case nebula.ErrorCode_E_CONSENSUS_ERROR:
							// client.ResetConn(kvputSpaceID, kvputPartID)
							// ignore
						default:
							glog.Warningf("kvput failed: %+v", resp.Result_.FailedParts)
							client.ResetConn(kvputSpaceID, kvputPartID)
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

	kvputCmd.Flags().Int32VarP(&kvputPartID, "partID", "p", 1, "part id")
	kvputCmd.Flags().Int32VarP(&kvputSpaceID, "spaceID", "s", 1, "space id")
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
