/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/storage"
)

type AddVertexOpts struct {
	space              string
	storages           []string
	vertexes           int
	tags               []string
	clients            int
	batch              int
	meta               string
	randomVertexOffset bool
}

var addVertexOpts AddVertexOpts

// addVertexCmd represents the addVertex command
var addVertexCmd = &cobra.Command{
	Use:   "addVertex",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunAddVertex()
	},
}

func RunAddVertex() {
	mopt := gonebula.MetaOption{
		Timeout:    8 * time.Second,
		BufferSize: 16 * 1024,
	}
	mclient, err := gonebula.NewMetaClient(addVertexOpts.meta, mopt)
	if err != nil {
		glog.Fatal(err)
	}

	// get space ID
	sResp, err := gonebula.GetSpaceByName(mclient, addVertexOpts.space)
	if err != nil {
		glog.Fatal(err)
	}
	spaceID := sResp.Item.GetSpaceID()
	glog.Infof("space name: %s, id: %d", addVertexOpts.space, spaceID)

	// get edge ID
	tagIDs := []int32{}
	for _, tagName := range addVertexOpts.tags {
		tagID, err := gonebula.GetTagID(mclient, spaceID, tagName)
		if err != nil {
			panic(fmt.Sprintf("error finding tag %s: %+v", tagName, err))
		}

		tagIDs = append(tagIDs, tagID)
	}

	// 3. craete storage clients
	var wg sync.WaitGroup
	wg.Add(addVertexOpts.clients * len(addVertexOpts.storages))
	newConn := func(saddr string) (*storage.GraphStorageServiceClient, error) {
		sclient, err := gonebula.NewStorageClient(saddr)
		return sclient, err
	}
	addrs := []string{}
	for i := 0; i < addVertexOpts.clients; i++ {
		addrs = append(addrs, addVertexOpts.storages...)
	}

	ts := int64(0)
	if addVertexOpts.randomVertexOffset {
		ts = int64(time.Now().UnixNano())
	}

	for i, c := range addrs {
		go func(clienID int, saddr string) {
			glog.Infof("starting client: %s", saddr)
			client, err := newConn(saddr)
			if err != nil {
				glog.Fatal(err)
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

			for x := 0; x < math.MaxInt64; x++ {
				propNames := map[nebula.TagID][][]byte{}
				tags := []*storage.NewTag_{}
				for _, tagID := range tagIDs {
					nameProp := nebula.Value{
						SVal: []byte(fmt.Sprintf("%d-%d", clienID, time.Now().UnixNano())),
					}
					var age int64 = time.Now().UnixNano()
					agePrpp := nebula.Value{
						IVal: &age,
					}

					tag := storage.NewTag_{
						TagID: tagID,
						// Props: []*nebula.Value{&prop},
						Props: []*nebula.Value{&nameProp, &agePrpp},
					}

					tags = append(tags, &tag)
					propNames[tagID] = [][]byte{[]byte("name"), []byte("age")}
				}

				// vid := uint64(8848) + uint64(ts)
				vid := uint64(x) + uint64(ts)
				buf := make([]byte, 8)
				binary.LittleEndian.PutUint64(buf, vid)
				v := storage.NewVertex_{
					Id: &nebula.Value{
						SVal: buf,
					},
					Tags: tags,
				}

				parts := map[nebula.PartitionID][]*storage.NewVertex_{
					1: {&v},
				}

				// parts := map[]]type
				req := &storage.AddVerticesRequest{
					SpaceID:   spaceID,
					Parts:     parts,
					PropNames: propNames,
				}
				resp, err := client.AddVertices(req)
				glog.Infof("resp: %+v, err: %+v", resp, err)
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
	glog.Infof("done exit")
}

func init() {
	rootCmd.AddCommand(addVertexCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addVertexCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addVertexCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	defaultStores := []string{
		"store1:9779",
		"store2:9779",
		"store3:9779",
		"store4:9779",
		"store5:9779",
	}

	addVertexCmd.Flags().StringVarP(&addVertexOpts.space, "space", "", "test", "space name")
	addVertexCmd.Flags().StringArrayVarP(&addVertexOpts.tags, "tags", "", []string{"tg1"}, "tag list")
	addVertexCmd.Flags().StringArrayVarP(&addVertexOpts.storages, "store", "", defaultStores, "stores")
	addVertexCmd.Flags().IntVarP(&addVertexOpts.vertexes, "vertexes", "", 65536, "vertexes")
	addVertexCmd.Flags().IntVarP(&addVertexOpts.clients, "clients", "", 8, "clients")
	addVertexCmd.Flags().IntVarP(&addVertexOpts.batch, "batch", "", 8, "batch")
	addVertexCmd.Flags().StringVarP(&addVertexOpts.meta, "meta", "", "meta1:9559", "meta addr")
	addVertexCmd.Flags().BoolVarP(&addVertexOpts.randomVertexOffset, "randomVertexOffset", "", true, "random vertex offset")
}
