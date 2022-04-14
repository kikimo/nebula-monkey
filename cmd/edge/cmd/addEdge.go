/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"encoding/binary"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/storage"
)

type AddEdgeOpts struct {
	space              string
	edge               string
	storages           []string
	vertexes           int
	clients            int
	batch              int
	meta               string
	enableToss         bool
	randomVertexOffset bool
}

var addEdgeOpts AddEdgeOpts

// addEdgeCmd represents the addEdge command
var addEdgeCmd = &cobra.Command{
	Use:   "addEdge",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunAddEdge()
	},
}

type VertexStore struct {
	vertexes int
	total    int
	idx      int
}

func (vs *VertexStore) Take(n int) [][2]uint64 {
	ret := [][2]uint64{}
	for i := 0; i < n; i++ {
		if vs.idx >= vs.total {
			break
		}

		from := vs.idx/vs.vertexes + 1
		to := vs.idx%vs.vertexes + 1
		ret = append(ret, [2]uint64{uint64(from), uint64(to)})
		vs.idx++
	}

	return ret
}

type Edge struct {
	src  uint64
	dst  uint64
	rank int64
	idx  string
	ts   *nebula.DateTime
}

func NewVertexStore(vertexes int) *VertexStore {
	vs := &VertexStore{
		vertexes: vertexes,
		total:    vertexes * vertexes,
		idx:      0,
	}

	return vs
}

func doStressEdge(client *storage.GraphStorageServiceClient,
	spaceID nebula.GraphSpaceID,
	partID nebula.PartitionID,
	edgeType nebula.EdgeType,
	edges []Edge) (*storage.ExecResponse, error) {

	nebulaEdges := []*storage.NewEdge_{}
	glog.V(2).Infof("batch inserting %d edges", len(edges))
	for _, edge := range edges {
		srcData := [8]byte{}
		dstData := [8]byte{}
		glog.V(1).Infof("inserting edge: %+v", edge)
		binary.LittleEndian.PutUint64(srcData[:], uint64(edge.src))
		binary.LittleEndian.PutUint64(dstData[:], uint64(edge.dst))
		// binary.LittleEndian.PutUint64(srcData[:], uint64(1))
		// binary.LittleEndian.PutUint64(dstData[:], uint64(2))

		propIdx := &nebula.Value{
			SVal: []byte(edge.idx),
		}
		props := []*nebula.Value{propIdx}
		eKey := storage.EdgeKey{
			Src: &nebula.Value{
				SVal: srcData[:],
			},
			Dst: &nebula.Value{
				SVal: dstData[:],
			},
			// Ranking:  rank,
			// Ranking: int64(dst),
			Ranking:  edge.rank,
			EdgeType: edgeType,
		}
		// fmt.Printf("key: %+v\n", eKey)
		nebulaEdges = append(nebulaEdges, &storage.NewEdge_{
			Key:   &eKey,
			Props: props,
		})
	}
	parts := map[nebula.PartitionID][]*storage.NewEdge_{
		int32(partID): nebulaEdges,
	}
	req := storage.AddEdgesRequest{
		SpaceID: spaceID,
		Parts:   parts,
	}

	if addEdgeOpts.enableToss {
		return client.ChainAddEdges(&req)
	} else {
		return client.AddEdges(&req)
	}
}

func RunAddEdge() {
	mopt := gonebula.MetaOption{
		Timeout:    8 * time.Second,
		BufferSize: 16 * 1024,
	}
	mclient, err := gonebula.NewMetaClient(addEdgeOpts.meta, mopt)
	if err != nil {
		glog.Fatal(err)
	}

	// get space ID
	sResp, err := gonebula.GetSpaceByName(mclient, addEdgeOpts.space)
	if err != nil {
		glog.Fatal(err)
	}
	spaceID := sResp.Item.GetSpaceID()
	glog.Infof("space name: %s, id: %d", addEdgeOpts.space, spaceID)

	// get edge ID
	ei, err := gonebula.GetEdgeItem(mclient, spaceID, addEdgeOpts.edge)
	if err != nil {
		glog.Fatal(err)
	}
	edgeType := ei.EdgeType
	glog.Infof("edge name: %s, type: %d", addEdgeOpts.edge, edgeType)

	// 3. craete storage clients
	var wg sync.WaitGroup
	wg.Add(addEdgeOpts.clients * len(addEdgeOpts.storages))
	newConn := func(saddr string) (*storage.GraphStorageServiceClient, error) {
		sclient, err := gonebula.NewStorageClient(saddr)
		return sclient, err
	}
	addrs := []string{}
	for i := 0; i < addEdgeOpts.clients; i++ {
		addrs = append(addrs, addEdgeOpts.storages...)
	}

	// ts := uint64(time.Now().UnixNano())
	ts := uint64(0)
	for i, c := range addrs {
		go func(clientID int, saddr string) {
			// 4. vertex store
			client, err := newConn(saddr)
			if err != nil {
				glog.Fatal(err)
			}

			vs := NewVertexStore(addEdgeOpts.vertexes)
			for {
				edges := vs.Take(addEdgeOpts.batch)
				if len(edges) == 0 {
					break
				}
				// glog.V(3).Infof("edge: %+v", edges)

				fullEdges := []Edge{}
				for _, e := range edges {
					e[0] += ts
					e[1] += ts
					glog.V(5).Infof("insert edge: %+v", e)
					e := Edge{
						src:  e[0] + ts,
						dst:  e[1] + ts,
						rank: 0,
						idx:  strconv.FormatInt(time.Now().UnixNano(), 10),
					}
					fullEdges = append(fullEdges, e)
				}

				resetConn := func() error {
					if client != nil {
						client.Close()
					}

					for i := 0; i < 32; i++ {
						client, err = newConn(saddr)
						if err != nil {
							client = nil
							time.Sleep(100 * time.Millisecond)
							glog.Errorf("error reseting conn %d try: %+v", i, err)
							continue
						}

						break
					}

					if client == nil {
						glog.Fatalf("failed resting conn for client %s", saddr)
					}

					return err
				}
				resp, err := doStressEdge(client, spaceID, 1, edgeType, fullEdges)
				if err == nil && len(resp.Result_.FailedParts) == 0 {
					glog.V(2).Infof("done inserting edge: %+v", fullEdges)
				}

				// glog.V(2).Infof("%s insert resp: %+v, err: %+v", saddr, resp, err)
				if err != nil {
					glog.Errorf("error: %+v, server: %s", err, saddr)
					// panic(err)
					if strings.Contains(err.Error(), "i/o timeout") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "Invalid data length") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "Not enough frame size") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "put failed: out of sequence response") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "Bad version in") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "broken pipe") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "out of sequence response") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "EOF") {
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					} else if strings.Contains(err.Error(), "loadshedding request") {
						time.Sleep(8 * time.Millisecond)
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
					glog.V(2).Infof("%s part error: %+v", saddr, resp.Result_.FailedParts)
					switch fpart.Code {
					case nebula.ErrorCode_E_LEADER_CHANGED,
						nebula.ErrorCode_E_OUTDATED_TERM:
						// if fpart.Leader != nil {
						// 	leaderAddr := fmt.Sprintf("%s:%d", fpart.Leader.Host, fpart.Leader.Port)
						// 	fmt.Printf("connecting to leader %s for client %d\n", leaderAddr, id)
						// }
						glog.V(1).Infof("%s error inserting edge, leader change: %+v", saddr, resp.Result_.FailedParts)
						time.Sleep(10 * time.Millisecond)
					case nebula.ErrorCode_E_CONSENSUS_ERROR,
						nebula.ErrorCode_E_WRITE_WRITE_CONFLICT,
						nebula.ErrorCode_E_RAFT_BUFFER_OVERFLOW,
						nebula.ErrorCode_E_DATA_CONFLICT_ERROR,
						nebula.ErrorCode_E_RAFT_ATOMIC_OP_FAILED,
						nebula.ErrorCode_E_RAFT_TERM_OUT_OF_DATE:

						// client.ResetConn(stressEdgeSpaceID, stressEdgePartID)
						// ignore
						glog.V(1).Infof("%s error inserting edge, term out of date: %+v", saddr, resp.Result_.FailedParts)
						time.Sleep(10 * time.Millisecond)
					default:
						glog.Warningf("unknown error inserting edge: %+v", resp.Result_.FailedParts)
						if err := resetConn(); err != nil {
							glog.Infof("error reseting conn: %+v", err)
							continue
						}
					}
				}

			}

			wg.Done()
		}(i, c)
	}

	wg.Wait()

}

func init() {
	rootCmd.AddCommand(addEdgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	defaultStores := []string{
		"store1:9779",
		"store2:9779",
		"store3:9779",
		"store4:9779",
		"store5:9779",
	}

	addEdgeCmd.Flags().StringVarP(&addEdgeOpts.space, "space", "", "test", "space name")
	addEdgeCmd.Flags().StringVarP(&addEdgeOpts.edge, "edge", "", "known2", "edge name")
	addEdgeCmd.Flags().StringArrayVarP(&addEdgeOpts.storages, "store", "", defaultStores, "stores")
	addEdgeCmd.Flags().IntVarP(&addEdgeOpts.vertexes, "vertexes", "", 65536, "vertexes")
	addEdgeCmd.Flags().IntVarP(&addEdgeOpts.clients, "clients", "", 8, "clients")
	addEdgeCmd.Flags().IntVarP(&addEdgeOpts.batch, "batch", "", 8, "batch")
	addEdgeCmd.Flags().StringVarP(&addEdgeOpts.meta, "meta", "", "meta1:9559", "meta addr")
	addEdgeCmd.Flags().BoolVarP(&addEdgeOpts.enableToss, "enableToss", "", false, "enable toss")
	addEdgeCmd.Flags().BoolVarP(&addEdgeOpts.randomVertexOffset, "randomVertexOffset", "", true, "random vertex offset")
}
