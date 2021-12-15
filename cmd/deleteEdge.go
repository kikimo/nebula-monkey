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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

var (
	deleteEdgeBatchSize        int
	defaultDeleteEdgeBatchSize int = 1
)

// deleteEdgeCmd represents the deleteEdge command
var deleteEdgeCmd = &cobra.Command{
	Use:   "deleteEdge",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		runDeleteEdges()
	},
}

func runDeleteEdges() {
	edgeName := "known2"
	numClients := 1
	clients := []*storage.GraphStorageServiceClient{}
	edges, err := getEdges(edgeName, false)
	if err != nil {
		glog.Fatal(err)
	}

	edgeItem := getEdgeItem(edgeName)
	if edgeItem == nil {
		glog.Fatal("edge item is nil")
	}
	glog.V(2).Infof("edge: %+v", edgeItem)
	edgeType := edgeItem.EdgeType
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	glog.V(2).Infof("raft cluster: %+v", raftCluster.String())
	leader, err := raftCluster.GetLeader()
	if err != nil {
		// glog.Fatal(err)
		glog.Fatalf("%+v", err)
	}

	addr := fmt.Sprintf("%s:%d", leader, 9779)
	for i := 0; i < numClients; i++ {
		client, err := newNebulaConn(addr)
		if err != nil {
			// glog.Fatal(err)
			glog.Infof("%+v", err)
		}
		clients = append(clients, client)
	}

	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			glog.Infof("client %d deleting edge", idx)
			client := clients[idx]
			pos := 0
			batch := 1
			for pos < len(edges) {
				end := pos + batch
				if end > len(edges) {
					end = len(edges)
				}
				glog.Infof("client %d deleting edge from %d to %d", idx, pos, end)

				candidates := edges[pos:end]
				dedges := []*storage.EdgeKey{}
				for _, e := range candidates {
					srcData := [8]byte{}
					dstData := [8]byte{}
					binary.LittleEndian.PutUint64(srcData[:], uint64(e.src))
					binary.LittleEndian.PutUint64(dstData[:], uint64(e.dst))

					key := storage.EdgeKey{
						Src: &nebula.Value{
							SVal: srcData[:],
						},
						Dst: &nebula.Value{
							SVal: dstData[:],
						},
						Ranking:  e.rank,
						EdgeType: edgeType,
					}
					dedges = append(dedges, &key)
				}
				req := storage.DeleteEdgesRequest{
					SpaceID: globalSpaceID,
					Parts: map[int32][]*storage.EdgeKey{
						globalPartitionID: dedges,
					},
				}

				// resp, err := client.ChainDeleteEdges(&req)
				resp, err := client.DeleteEdges(&req)
				if err != nil {
					glog.Fatal(err)
				}

				if len(resp.Result_.FailedParts) > 0 {
					glog.Fatal(resp.Result_.FailedParts)
				}

				pos = end
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func init() {
	rootCmd.AddCommand(deleteEdgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// deleteEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// deleteEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	deleteEdgeCmd.Flags().IntVarP(&deleteEdgeBatchSize, "batch", "b", defaultDeleteEdgeBatchSize, "delete batch size")
}
