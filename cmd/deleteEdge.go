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
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

type EdgeIterType string

const (
	FullIter EdgeIterType = "full"
	ScanIter EdgeIterType = "scan"
)

var (
	deleteEdgeChaos             bool
	deleteEdgeBatchSize         int
	deleteEdgeVertexes          int
	deleteEdgeRanks             int
	defaultDeleteEdgeBatchSize  int = 1
	deleteEdgeDeleteType        string
	defaultDeleteEdgeDeleteType string = "toss"
	deleteEdgeClients           int
	deleteEdgeIterType          string
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
	client, err := gonebula.NewNebulaConn(c.addr)
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

type FullEdgeIterator struct {
	vertexes int
	ranks    int
	eType    nebula.EdgeType

	from     int
	to       int
	currRank int
}

type ScanEdgeIterator struct {
	edges    []Edge
	edgeType nebula.EdgeType
	pos      int
}

func (itr *ScanEdgeIterator) NextEdge() *storage.EdgeKey {
	glog.V(3).Infof("pos %d, edges size: %d", itr.pos, len(itr.edges))
	if itr.pos >= len(itr.edges) {
		return nil
	}
	e := itr.edges[itr.pos]
	itr.pos++

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
		EdgeType: itr.edgeType,
	}

	return &key
}

func loadEdges(edgeName string) []Edge {
	var edges []Edge
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

	return edges
}

func NewScanEdgeIterator(edges []Edge, edgeType nebula.EdgeType) EdgeIterator {
	itr := ScanEdgeIterator{
		edges:    edges,
		edgeType: edgeType,
	}

	return &itr
}

type EdgeIterator interface {
	NextEdge() *storage.EdgeKey
}

func NewFullEdgeIterator(vertexes int, ranks int, eType nebula.EdgeType) EdgeIterator {
	return &FullEdgeIterator{
		vertexes: vertexes,
		ranks:    ranks,
		eType:    eType,
		from:     0,
		to:       0,
		currRank: 0,
	}
}

func (itr *FullEdgeIterator) NextEdge() *storage.EdgeKey {
	if itr.currRank >= itr.ranks {
		return nil
	}

	srcData := [8]byte{}
	dstData := [8]byte{}
	binary.LittleEndian.PutUint64(srcData[:], uint64(itr.from))
	binary.LittleEndian.PutUint64(dstData[:], uint64(itr.to))

	key := storage.EdgeKey{
		Src: &nebula.Value{
			SVal: srcData[:],
		},
		Dst: &nebula.Value{
			SVal: dstData[:],
		},
		Ranking:  int64(itr.currRank),
		EdgeType: int32(itr.eType),
	}
	// dedges = append(dedges, &key)
	itr.to++
	if itr.to == itr.vertexes {
		itr.from++
		itr.to = 0
	}

	if itr.from == itr.vertexes {
		itr.from = 0
		itr.currRank++
	}

	return &key
}

func runDeleteEdges() {
	edgeName := "known2"
	numClients := deleteEdgeClients
	clients := []*StorageClient{}

	if deleteEdgeChaos {
		glog.Info("chaos delete enabled")
	}

	// edges := []Edge{}
	var err error

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

	var edges []Edge
	if EdgeIterType(deleteEdgeIterType) == ScanIter {
		edges = loadEdges(edgeName)
	}

	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(idx int) {
			glog.Infof("client %d deleting edge", idx)
			client := clients[idx]
			var edgeItr EdgeIterator

			switch EdgeIterType(deleteEdgeIterType) {
			case ScanIter:
				edgeItr = NewScanEdgeIterator(edges, edgeType)
			case FullIter:
				edgeItr = NewFullEdgeIterator(deleteEdgeVertexes, deleteEdgeRanks, edgeType)
			}
			end := false
			for !end {
				candidates := []*storage.EdgeKey{}
				for x := 0; x < deleteEdgeBatchSize; x++ {
					e := edgeItr.NextEdge()
					if e == nil {
						end = true
						glog.Infof("%d delete reach end, candidate size now: %d", idx, len(candidates))
						// break END
						break
					}

					candidates = append(candidates, e)
				}

				if len(candidates) == 0 {
					break
				}

				req := storage.DeleteEdgesRequest{
					SpaceID: globalSpaceID,
					Parts: map[int32][]*storage.EdgeKey{
						globalPartitionID: candidates,
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

				glog.V(2).Infof("delete edge resp: %+v", resp)

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

			}
			/*
				for pos < len(edges) {
					end := pos + deleteEdgeBatchSize
					if end > len(edges) {
						end = len(edges)
					}
					glog.Infof("client %d deleting edge from %d to %d", idx, pos, end)

					candidates := edges[pos:end]
					pos = end
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
				}
			*/
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
	deleteEdgeCmd.Flags().IntVarP(&deleteEdgeClients, "clients", "c", 1, "num clients")
	deleteEdgeCmd.Flags().IntVarP(&deleteEdgeVertexes, "vertexes", "x", 1, "num vertexes")
	deleteEdgeCmd.Flags().StringVarP(&deleteEdgeIterType, "itype", "q", "full", "iter type: {full|scan}")
	// deleteEdgeCmd.MarkFlagRequired("vertexes")
	deleteEdgeCmd.Flags().IntVarP(&deleteEdgeRanks, "ranks", "r", 1, "num ranks")
	// deleteEdgeCmd.MarkFlagRequired("ranks")
}
