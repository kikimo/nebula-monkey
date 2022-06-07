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
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
	"github.com/vesoft-inc/nebula-go/v3/nebula/storage"
)

type EdgeType int

const (
	OutEdge EdgeType = iota
	InEdge
	AllEdge
)

type CheckEdgeOpts struct {
	ignoreTimestamp bool
}

var checkEdgeOpts CheckEdgeOpts

// checkEdgesCmd represents the checkEdges command
var checkEdgesCmd = &cobra.Command{
	Use:   "checkEdges",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		runCheckEdge()
	},
}

// FIXME(wwl): what if there are more than one meta
func newMetaClient(metaAddr string) *meta.MetaServiceClient {
	option := gonebula.MetaOption{
		Timeout:    100 * time.Millisecond,
		BufferSize: 4096,
	}

	// addr := fmt.Sprintf("%s:%d", "meta1", 9559)
	addr := globalOpts.metaAddrs[0]
	client, err := gonebula.NewMetaClient(addr, option)
	if err != nil {
		panic(err)
	}

	return client
}

func getSpaceByName(spaceName string) *meta.GetSpaceResp {
	client := newMetaClient(globalOpts.metaAddrs[0])
	defer func() {
		if err := client.Close(); err != nil {
			glog.Errorf("error closing meta client: %+v", err)
		}
	}()

	resp, err := gonebula.GetSpaceByName(client, spaceName)
	if err != nil {
		panic(err)
	}

	return resp
}

// TODO: refactor me
func getEdgeItem(edgeName string, spaceID int32) *meta.EdgeItem {
	client := newMetaClient(globalOpts.metaAddrs[0])
	defer func() {
		if err := client.Close(); err != nil {
			glog.Errorf("error closing meta client: %+v", err)
		}
	}()

	req := meta.ListEdgesReq{
		SpaceID: spaceID,
	}
	resp, err := client.ListEdges(&req)
	glog.V(2).Infof("list edge resp: %+v", resp)
	if err != nil {
		glog.Fatal(err)
	}

	if resp.Code != nebula.ErrorCode_SUCCEEDED {
		glog.Fatal(resp)
	}

	for _, item := range resp.GetEdges() {
		glog.V(2).Infof("found edge: %s", string(item.EdgeName))
		if string(item.EdgeName) == edgeName {
			return item
		}
	}

	return nil
}

type Edge struct {
	src  int64
	dst  int64
	rank int64
	idx  string
	ts   *nebula.DateTime
}

func (e *Edge) Key() string {
	return fmt.Sprintf("%d->%d@%d", e.src, e.dst, e.rank)
}

func (e *Edge) String() string {
	ts := "nil"
	if e.ts != nil {
		ts = e.ts.String()
	}

	return fmt.Sprintf("%d->%d@%d(idx: %s, ts: %s)", e.src, e.dst, e.rank, e.idx, ts)
}

func (e *Edge) Equals(o Edge) bool {
	if checkEdgeOpts.ignoreTimestamp {
		return e.dst == o.dst && e.src == o.src &&
			e.rank == o.rank && e.idx == o.idx
	}

	return e.dst == o.dst && e.src == o.src &&
		e.rank == o.rank && e.idx == o.idx &&
		(e.ts == nil && o.ts == nil ||
			(e.ts != nil && o.ts != nil && *e.ts == *o.ts))
}

func getEdges(edgeName string, edgeType EdgeType, spaceID int32) ([]Edge, error) {
	switch edgeType {
	case OutEdge:
		return doGetEdges(edgeName, false, spaceID)
	case InEdge:
		return doGetEdges(edgeName, true, spaceID)
	case AllEdge:
		var wg sync.WaitGroup

		var outEdges, inEdges []Edge
		var outErr, inErr error

		wg.Add(2)
		go func() {
			outEdges, outErr = doGetEdges(edgeName, false, spaceID)
			wg.Done()
		}()

		go func() {
			inEdges, inErr = doGetEdges(edgeName, true, spaceID)
			wg.Done()
		}()
		wg.Wait()

		if outErr != nil {
			return nil, outErr
		}

		if inErr != nil {
			return nil, inErr
		}

		mergeEdges := func(a, b []Edge) []Edge {
			emap := map[string]struct{}{}
			edges := []Edge{}
			for i, e := range a {
				k := e.Key()
				if _, ok := emap[k]; ok {
					continue
				}

				edges = append(edges, a[i])
				emap[k] = struct{}{}
			}

			for i, e := range b {
				k := e.Key()
				if _, ok := emap[k]; ok {
					continue
				}

				edges = append(edges, b[i])
				emap[k] = struct{}{}
			}

			return edges
		}
		edges := mergeEdges(outEdges, inEdges)
		return edges, nil
	default:
		return nil, fmt.Errorf("unknown edge type: %d", edgeType)
	}
}

func doGetEdges(edgeName string, reverse bool, spaceID int32) ([]Edge, error) {
	edges := []Edge{}
	edgeItem := getEdgeItem(edgeName, spaceID)
	if edgeItem == nil {
		return nil, fmt.Errorf("edge item is nil")
	}
	// glog.V(2).Infof("edge: %+v", edgeItem)
	edgeType := edgeItem.EdgeType
	if reverse {
		edgeType = -edgeType
	}

	glog.V(2).Infof("scanning edge with type: %d", edgeType)
	raftCluster := createRaftCluster(globalSpaceID, globalPartitionID)
	// glog.V(2).Infof("raft cluster: %+v", raftCluster.String())
	leader, err := raftCluster.GetLeader()
	if err != nil {
		// glog.Fatal(err)
		return nil, fmt.Errorf("%+v", err)
	}

	addr := fmt.Sprintf("%s:%d", leader, 9779)
	client, err := gonebula.NewNebulaConn(addr)
	if err != nil {
		// glog.Fatal(err)
		return nil, fmt.Errorf("%+v", err)
	}

	glog.V(2).Infof("client: %+v", client)
	// client.ScanEdge()
	props := [][]byte{[]byte("_src"), []byte("_type"), []byte("_rank"), []byte("_dst"), []byte("idx"), []byte("ts")}

	var nextCursor []byte
	for {
		req := &storage.ScanEdgeRequest{
			Limit:   4096,
			SpaceID: globalSpaceID,
			Parts: map[int32]*storage.ScanCursor{
				globalPartitionID: {
					NextCursor: nextCursor,
				},
			},
			ReturnColumns: []*storage.EdgeProp{
				{
					Type:  edgeType,
					Props: props,
				},
			},
		}

		resp, err := client.ScanEdge(req)
		if err != nil {
			glog.Fatal(err)
		}
		ds := resp.GetProps()
		glog.Infof("scanning %d edge with cursor: %+v", len(ds.GetRows()), nextCursor)
		for _, r := range ds.GetRows() {
			src, dst := r.Values[0].IVal, r.Values[3].IVal
			rank := r.Values[2].IVal
			idx := string(r.Values[4].SVal)
			ts := r.Values[5].DtVal
			if reverse {
				src, dst = dst, src
			}

			// glog.Infof("type: %d", *r.Values[1].IVal)
			// glog.Infof("src: %d, dst: %d@rank: %d", *src, *dst, *rank)
			edge := Edge{
				src:  *src,
				dst:  *dst,
				idx:  idx,
				ts:   ts,
				rank: *rank,
			}
			edges = append(edges, edge)
		}

		glog.Infof("scan edge resp cursors: %+v", resp.Cursors)
		if resp.Cursors[globalPartitionID].NextCursor == nil {
			break
		}

		nextCursor = resp.Cursors[globalPartitionID].NextCursor
	}
	// glog.Info("resp: %+v", resp)

	return edges, nil
}

func runCheckEdge() {
	edgeName := "known2"

	outEdges, err := doGetEdges(edgeName, false, 1)
	if err != nil {
		glog.Fatal(err)
	}

	inEdges, err := doGetEdges(edgeName, true, 1)
	if err != nil {
		glog.Fatal(err)
	}

	toMap := func(edges []Edge) map[string]*Edge {
		emap := map[string]*Edge{}
		for i, e := range edges {
			emap[e.Key()] = &edges[i]
		}

		return emap
	}
	outEdgeMap := toMap(outEdges)
	inEdgeMap := toMap(inEdges)

	glog.Infof("out size: %d\n", len(outEdgeMap))
	glog.Infof("in size: %d\n", len(inEdgeMap))

	for k, e := range outEdgeMap {
		glog.V(2).Infof("out edge: %s", k)
		if _, ok := inEdgeMap[k]; !ok {
			glog.Infof("missing in edge: %s", e.Key())
		}
	}

	for k, e := range inEdgeMap {
		glog.V(2).Infof("in edge: %s", k)
		if _, ok := outEdgeMap[k]; !ok {
			glog.Infof("missing out edge: %s", e.Key())
			// glog.Infof("out edges: %+v", outEdgeMap)
		}
	}

	for k, outEdge := range outEdgeMap {
		if _, ok := inEdgeMap[k]; !ok {
			continue
		}

		inEdge := inEdgeMap[k]
		if !outEdge.Equals(*inEdge) {
			glog.Infof("edge mismatch, out Edge: %s, in edge: %s", outEdge.String(), inEdge.String())
		}
	}
}

func init() {
	rootCmd.AddCommand(checkEdgesCmd)

	// Here you will define your flags and configuration settings.
	checkEdgesCmd.Flags().BoolVarP(&checkEdgeOpts.ignoreTimestamp, "ignoreTimestamp", "", false, "ignore timestamp check")

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// checkEdgesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// checkEdgesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
