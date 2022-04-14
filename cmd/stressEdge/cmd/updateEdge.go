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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	nebula "github.com/vesoft-inc/nebula-go/v3"
	"golang.org/x/time/rate"
)

var log = nebula.DefaultLogger{}

type UpdateEdgeOpts struct {
	clients    int
	space      string
	edge       string
	vertexes   int
	metaAddr   string
	graphAddrs []string
	rateLimit  int
	loops      int
}

var updateEdgeOpts UpdateEdgeOpts

// updateEdgeCmd represents the updateEdge command
var updateEdgeCmd = &cobra.Command{
	Use:   "updateEdge",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		updateEdge()
	},
}

func updateEdge() {
	// batchSize := gqlStressEdgeOpts.batchSize
	graphHosts := []nebula.HostAddress{}

	for _, gaddr := range updateEdgeOpts.graphAddrs {
		parts := strings.Split(gaddr, ":")
		if len(parts) != 2 {
			glog.Fatalf("illegal graph addr: %s", gaddr)
		}
		host := parts[0]
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			glog.Fatalf("illegal graph addr: %s, failed parsing graph port: %+v", gaddr, port)
		}

		graphHosts = append(graphHosts, nebula.HostAddress{
			Host: host,
			Port: port,
		})
	}

	poolConfig := nebula.GetDefaultConf()
	poolConfig.MaxConnPoolSize = 2048
	pool, err := nebula.NewConnectionPool(graphHosts, poolConfig, log)
	if err != nil {
		log.Fatal(fmt.Sprintf("err: %+v", err))
	}
	defer pool.Close()

	username, passwd := "root", "nebula"
	var wg sync.WaitGroup
	wg.Add(updateEdgeOpts.clients)

	limit := rate.Every(time.Microsecond * time.Duration(updateEdgeOpts.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	for i := 0; i < updateEdgeOpts.clients; i++ {
		time.Sleep(10 * time.Millisecond)
		go func(clientID int) {
			session, err := pool.GetSession(username, passwd)
			if err != nil {
				log.Fatal(fmt.Sprintf("err: %+v", err))
			}
			defer session.Release()

			rs, err := session.Execute(fmt.Sprintf("use %s", updateEdgeOpts.space))
			if err != nil {
				log.Fatal(fmt.Sprintf("err: %+v", err))
			}
			log.Info(fmt.Sprintf("rs: %+v", rs))

			// ts := time.Now().Nanosecond()
			// ts := 0
			for j := 0; j < updateEdgeOpts.loops; j++ {
				// UPDATE EDGE "player101" -> "player100" OF follow SET degree = 96
				// gql := "insert edge known2(idx) values "
				for src := 0; src < updateEdgeOpts.vertexes; src++ {
					for dst := 0; dst < updateEdgeOpts.vertexes; dst++ {
						limiter.Wait(ctx)
						// updateSql := gql + fmt.Sprintf("%d->%d:(%d)", src, dst, ts)
						updategql := fmt.Sprintf(`update edge %d -> %d of %s set idx = "%d"`, src+1, dst+1, updateEdgeOpts.edge, time.Now().UnixNano())
						glog.V(1).Infof("updating edge: %s", updategql)
						rs, err := session.Execute(updategql)
						if err != nil {
							glog.Errorf(err.Error())
						}

						glog.V(3).Infof("update resp: %+v", rs)
					}
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func init() {
	rootCmd.AddCommand(updateEdgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// updateEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// updateEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	// clients    int
	// space      string
	// edge       string
	// vertexes   string
	// metaAddr   string
	// graphAddrs []string
	updateEdgeCmd.Flags().IntVarP(&updateEdgeOpts.clients, "clients", "", 16, "clients number")
	updateEdgeCmd.Flags().StringVarP(&updateEdgeOpts.space, "space", "", "test", "space name")
	updateEdgeCmd.Flags().StringVarP(&updateEdgeOpts.edge, "edge", "", "known2", "edge name")
	updateEdgeCmd.Flags().IntVarP(&updateEdgeOpts.vertexes, "vertexes", "", 1024, "number of vertexes")
	updateEdgeCmd.Flags().StringVarP(&updateEdgeOpts.metaAddr, "metaAddr", "", "meta1", "meta addr")
	updateEdgeCmd.Flags().StringArrayVarP(&updateEdgeOpts.graphAddrs, "graphAddrs", "", []string{"graph1"}, "graph addrs")
	updateEdgeCmd.Flags().IntVarP(&updateEdgeOpts.rateLimit, "rateLimit", "", 1, "")
	updateEdgeCmd.Flags().IntVarP(&updateEdgeOpts.loops, "loops", "", 1, "")
}
