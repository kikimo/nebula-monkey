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
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

var (
	deleteEdgeChaos             bool
	deleteEdgeBatchSize         int
	defaultDeleteEdgeBatchSize  int = 1
	deleteEdgeDeleteType        string
	defaultDeleteEdgeDeleteType string              = "toss"
	deletTypes                  map[string]struct{} = map[string]struct{}{
		"toss": {},
		"in":   {},
		"out":  {},
	}
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
		if _, ok := deletTypes[deleteEdgeDeleteType]; !ok {
			glog.Fatalf("unknown delete type: %s", deleteEdgeDeleteType)
		}

		runDeleteEdges()
	},
}

type StorageClient struct {
	addr   string
	client *storage.GraphStorageServiceClient
}

func (c *StorageClient) ResetConn() {
	client, err := newNebulaConn(c.addr)
	if err != nil {
		glog.Fatal(err)
	}

	c.client = client
}

func NewStorageClient(addr string) *StorageClient {
	return &StorageClient{
		addr: addr,
	}
}

func runDeleteEdges() {
	edgeName := "known2"
	numClients := 1
	clients := []*StorageClient{}

	if deleteEdgeChaos {
		glog.Info("chaos delete enabled")
	}

	edges := []Edge{}
	var err error
	switch deleteEdgeDeleteType {
	case "in":
		if edges, err = getEdges(edgeName, InEdge); err != nil {
			glog.Fatalf("error getting in edge: %+v", err)
		}

	case "out":
		if edges, err = getEdges(edgeName, OutEdge); err != nil {
			glog.Fatalf("error getting out edge: %+v", err)
		}

	case "toss":
		if edges, err = getEdges(edgeName, AllEdge); err != nil {
			glog.Fatal("error gett")
		}
	default:
		glog.Fatalf("unknown delete edge type: %s", deleteEdgeDeleteType)
	}

	edgeItem := getEdgeItem(edgeName)
	if edgeItem == nil {
		glog.Fatal("edge item is nil")
	}
	glog.V(2).Infof("edge: %+v", edgeItem)
	edgeType := edgeItem.EdgeType
	if deleteEdgeDeleteType == "in" {
		edgeType = -edgeType
	}

	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	glog.V(2).Infof("raft cluster: %+v", raftCluster.String())
	leader, err := raftCluster.GetLeader()
	if err != nil {
		// glog.Fatal(err)
		glog.Fatalf("%+v", err)
	}

	addr := fmt.Sprintf("%s:%d", leader, 9779)
	for i := 0; i < numClients; i++ {
		if !deleteEdgeChaos {
			client := NewStorageClient(addr)
			if err != nil {
				// glog.Fatal(err)
				glog.Infof("%+v", err)
			}
			client.ResetConn()
			clients = append(clients, client)
		} else {
			for _, peer := range raftPeers {
				paddr := fmt.Sprintf("%s:%d", peer, 9779)
				client := NewStorageClient(paddr)
				if err != nil {
					// glog.Fatal(err)
					glog.Infof("%+v", err)
				}
				client.ResetConn()
				clients = append(clients, client)
			}
		}
	}

	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(idx int) {
			glog.Infof("client %d deleting edge", idx)
			client := clients[idx]
			pos := 0
			for pos < len(edges) {
				end := pos + deleteEdgeBatchSize
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
				var resp *storage.ExecResponse
				var err error
				if deleteEdgeDeleteType == "toss" {
					glog.Infof("chain deleting edges")
					resp, err = client.client.ChainDeleteEdges(&req)
				} else {
					resp, err = client.client.DeleteEdges(&req)
				}

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
						panic(err)
						// fmt.Printf("fuck: %+v\n", err)
					}

					glog.Infof("err: %+v", err)
					continue
				}

				if len(resp.Result_.FailedParts) > 0 {
					// glog.Fatal(resp.Result_.FailedParts)
					// ignore
					glog.Infof("resp: %+v", resp.Result_.FailedParts)
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
	deleteEdgeCmd.Flags().StringVarP(&deleteEdgeDeleteType, "delete", "d", defaultDeleteEdgeDeleteType, "delete type: {toss|in|out}")
	deleteEdgeCmd.Flags().BoolVarP(&deleteEdgeChaos, "chaos", "o", false, "enable chaos")
}
