/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

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
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
	"github.com/vesoft-inc/nebula-go/v3/nebula/storage"
	"golang.org/x/time/rate"
)

type PutOpts struct {
	space        string
	metaAddr     string
	clients      int
	vertexes     int
	rateLimit    int
	offset       int
	perClientKey bool
}

var putOpts PutOpts

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunPut()
	},
}

func RunPut() {
	metaOpt := gonebula.MetaOption{
		Timeout:    1 * time.Second,
		BufferSize: 1024 * 1024,
	}
	mClient, err := gonebula.NewMetaClient(putOpts.metaAddr, metaOpt)
	if err != nil {
		glog.Fatal(err)
	}

	// get storage info
	lhReq := &meta.ListHostsReq{
		Type: meta.ListHostType_STORAGE,
	}

	lhResp, err := mClient.ListHosts(lhReq)
	if err != nil {
		panic(err)
	}
	hosts := lhResp.GetHosts()
	glog.Infof("storage hosts: %+v", hosts)

	// get space and part info
	spcReq := &meta.GetSpaceReq{
		SpaceName: []byte(putOpts.space),
	}
	spcResp, err := mClient.GetSpace(spcReq)
	if err != nil {
		panic(err)
	}

	si := spcResp.GetItem()
	spaceID := si.GetSpaceID()
	sdesc := si.GetProperties()
	pnum := sdesc.PartitionNum
	glog.Infof("space id: %d, pnum: %d", spaceID, pnum)

	limit := rate.Every(time.Microsecond * time.Duration(putOpts.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()
	globalTS := time.Now().UnixNano()

	clientID := 0
	var wg sync.WaitGroup
	for _, h := range hosts {
		for p := 1; p <= int(pnum); p++ {
			for i := 0; i < putOpts.clients; i++ {
				clientID++
				wg.Add(1)
				go func(clientID int, part int, h *meta.HostItem) {
					// 1. create connection
					addr := fmt.Sprintf("%s:%d", h.HostAddr.Host, h.HostAddr.Port)
					sclient, err := gonebula.NewStorageClient(addr)
					if err != nil {
						glog.Error(err)
					}

					resetConn := func() {
						if sclient != nil && sclient.IsOpen() {
							sclient.Close()
						}
						sclient = nil

						sclient, err = gonebula.NewNebulaConn(addr)
						if err != nil {
							glog.Error(err)
							time.Sleep(500 * time.Millisecond)
						}
					}

					// 2. create payload
					offset := putOpts.offset
					ts := time.Now().UnixNano()
					for x := offset; x < putOpts.vertexes+offset; x++ {
						for y := offset; y < putOpts.vertexes+offset; y++ {
							parts := make(map[int32][]*nebula.KeyValue)
							// for p := 1; p <= int(pnum); p++ {
							// for p := 1; p <= 1; p++ {
							var key string
							if !putOpts.perClientKey {
								key = fmt.Sprintf("key-%d-%d-%d-%d", part, x, y, globalTS)
							} else {
								key = fmt.Sprintf("key-%d-%d-%d-%d-%d", part, x, y, clientID, ts)
							}

							val := fmt.Sprintf("val-%d-%d-%d-%d-%d", part, x, y, clientID, ts)
							kv := &nebula.KeyValue{
								Key:   []byte(key),
								Value: []byte(val),
							}
							// parts[int32(p)] = []*nebula.KeyValue{kv}
							parts[int32(part)] = []*nebula.KeyValue{kv}
							// }

							putReq := &storage.KVPutRequest{
								SpaceID: spaceID,
								Parts:   parts,
							}

							limiter.Wait(ctx)
							// key = fmt.Sprintf("key-partID-%d-%d-%d", x, y, clientID)
							glog.V(2).Infof("client %d put key %s", clientID, key)
							if sclient == nil {
								resetConn()

								if sclient == nil {
									continue
								}
							}

							resp, err := sclient.Put(putReq)
							if err != nil {
								glog.Errorf("error puting key %s", key)
								glog.Errorf("error kvput: %+v", err)
								// TODO: handle error, reconnect if necessary

								// panic(err)
								if strings.Contains(err.Error(), "i/o timeout") {
									resetConn()
								} else if strings.Contains(err.Error(), "Invalid data length") {
									resetConn()
								} else if strings.Contains(err.Error(), "Not enough frame size") {
									resetConn()
								} else if strings.Contains(err.Error(), "put failed: out of sequence response") {
									resetConn()
								} else if strings.Contains(err.Error(), "Bad version in") {
									resetConn()
								} else if strings.Contains(err.Error(), "broken pipe") {
									resetConn()
								} else {
									// panic(err)
									glog.Errorf("unknown error: %+v", err)
									// fmt.Printf("fuck: %+v\n", err)
								}

								continue
							}

							fp := resp.GetResult_().GetFailedParts()
							if len(fp) > 0 {
								glog.V(2).Infof("kvput failed parts: %+v", fp)
							} else {
								glog.V(1).Infof("done puting key %s", key)
							}
						}
					}

					wg.Done()
				}(i, p, h)
			}
		}
	}

	wg.Wait()
	// gonebula.NewStorageClient("")
	// now create connection and send payload
}

func init() {
	rootCmd.AddCommand(putCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// putCmd.PersistentFlags().String("foo", "", "A help for foo")
	putCmd.Flags().StringVarP(&putOpts.space, "space", "", "test", "space name")
	putCmd.Flags().StringVarP(&putOpts.metaAddr, "meta-addr", "", "meta1:9559", "meta addr")
	putCmd.Flags().IntVarP(&putOpts.clients, "clients", "", 1, "concurrent clients per storage host")
	putCmd.Flags().IntVarP(&putOpts.vertexes, "vertexes", "", 1024, "number of vertexes")
	putCmd.Flags().IntVarP(&putOpts.rateLimit, "rate-limit", "", 1000, "every rateLimit microsecond per request")
	putCmd.Flags().IntVarP(&putOpts.offset, "offset", "", 0, "vertex offset")
	putCmd.Flags().BoolVarP(&putOpts.perClientKey, "per-client-key", "", false, "per client key")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// putCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
