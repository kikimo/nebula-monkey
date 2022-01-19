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
	clientsPerRaftInstance       int
	defaultClientPerRaftInstance int = 16
	rogueKVBatchSize             int
	rogueKVRateLimit             int
	defaultRogueKVRateLimit      int = 200
	defaultRogueKVBatchSize      int = 1
)

// roguekvCmd represents the roguekv command
var roguekvCmd = &cobra.Command{
	Use:   "roguekv",
	Short: "run kv put for every raft instance regardless of whether it's leader or not",
	Long: `run kv put for every raft instance regardless of whether it's leader or not.
This command try reproduce data inconsistant in raft when they loose leadership`,
	Run: func(cmd *cobra.Command, args []string) {
		runRogueKV()
	},
}

func runRogueKV() {
	clients := []gonebula.NebulaClient{}
	glog.Info("building storage clients...")
	for _, rp := range raftPeers {
		addr := fmt.Sprintf("%s:%d", rp, 9779)
		for i := 0; i < clientsPerRaftInstance; i++ {
			glog.Infof("building storage clients: %s, %d", addr, i)
			c := gonebula.NewFixedTargetNebulaClient(addr)
			err := c.ResetConn()
			if err != nil {
				// panic(err)
				glog.Errorf("error initiating client: %+v", err)
			}

			clients = append(clients, c)
		}
	}
	glog.Infof("done building storage clients, %d clients in total.", len(clients))
	limit := rate.Every(time.Microsecond * time.Duration(rogueKVRateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		glog.Infof("starting %d client to put kv", i)
		go func(id int, client gonebula.NebulaClient) {
			maxLoop := 65536 * 65536
			glog.Infof("client %d putting kv...", id)
			for l := 0; l < maxLoop; l++ {
				// TODO: make kv list
				if client.GetClient() == nil {
					err := client.ResetConn()
					if err != nil {
						continue
					}
				}

				kvs := []*nebula.KeyValue{}
				for k := 0; k < rogueKVBatchSize; k++ {
					key := fmt.Sprintf("key-%d-%d-%d", l, k, id)
					value := fmt.Sprintf("value-%d-%d-%d", l, k, id)
					kvs = append(kvs, &nebula.KeyValue{
						Key:   []byte(key),
						Value: []byte(value),
					})
				}
				limiter.Wait(ctx)

				req := storage.KVPutRequest{
					SpaceID: globalSpaceID,
					Parts: map[nebula.PartitionID][]*nebula.KeyValue{
						globalPartitionID: kvs,
					},
				}

				resp, err := client.GetClient().Put(&req)
				// glog.Infof("put resp: %+v, err: %+v", resp, err)
				if err != nil {
					// panic(err)
					if strings.Contains(err.Error(), "i/o timeout") {
						// client.ResetConn()
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
						glog.Errorf("unknown err: %+v", err)
						if err := client.ResetConn(); err != nil {
							time.Sleep(time.Millisecond * 500)
							glog.Errorf("error reseting conn: %+v", err)
						}
						// panic(err)
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
						// glog.Warningf("kvput failed: %+v", resp.Result_.FailedParts)
						// client.ResetConn()
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
			wg.Done()
		}(i, c)
	}

	wg.Wait()
}

func init() {
	rootCmd.AddCommand(roguekvCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// roguekvCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// roguekvCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	roguekvCmd.Flags().IntVarP(&clientsPerRaftInstance, "clients", "c", defaultClientPerRaftInstance, "clients per raft instance")
	roguekvCmd.Flags().IntVarP(&rogueKVBatchSize, "batch", "b", defaultRogueKVBatchSize, "batch kv size")
	roguekvCmd.Flags().IntVarP(&rogueKVRateLimit, "rate", "r", defaultRogueKVRateLimit, "one query every rate microsecond")
}
