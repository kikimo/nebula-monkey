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

// gqlStressEdgeCmd represents the gqlStressEdge command
var gqlStressEdgeCmd = &cobra.Command{
	Use:   "gqlStressEdge",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("gqlStressEdge inserting edges")
		gqlStressEdge()
	},
}

type GQLStressEdgeOpts struct {
	batchSize  int
	clients    int
	loops      int
	rateLimit  int
	graphAddrs []string
}

var gqlStressEdgeOpts GQLStressEdgeOpts

var log = nebula.DefaultLogger{}

func gqlStressEdge() {
	spaceName := globalOpts.spaceName
	// batchSize := gqlStressEdgeOpts.batchSize
	graphHosts := []nebula.HostAddress{}

	for _, gaddr := range gqlStressEdgeOpts.graphAddrs {
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
	wg.Add(gqlStressEdgeOpts.clients)

	limit := rate.Every(time.Microsecond * time.Duration(1000000/gqlStressEdgeOpts.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	for i := 0; i < gqlStressEdgeOpts.clients; i++ {
		go func(clientID int) {
			session, err := pool.GetSession(username, passwd)
			if err != nil {
				log.Fatal(fmt.Sprintf("err: %+v", err))
			}
			defer session.Release()

			rs, err := session.Execute(fmt.Sprintf("use %s", spaceName))
			if err != nil {
				log.Fatal(fmt.Sprintf("err: %+v", err))
			}
			log.Info(fmt.Sprintf("rs: %+v", rs))

			count := 0
			for j := 0; j < gqlStressEdgeOpts.loops; j++ {
				gql := "insert edge known2(idx) values "
				// insert edge known2(idx) values 0->1:("kk");
				values := []string{}
				for k := 0; k < gqlStressEdgeOpts.batchSize; k++ {
					count++
					v := fmt.Sprintf("%d->%d:('%d')", clientID, count, count)
					values = append(values, v)
				}
				gql = gql + strings.Join(values[:], ",")
				rs, err = session.Execute(gql)
				// fmt.Printf("gql: %s\n", gql)
				if err != nil {
					log.Fatal(fmt.Sprintf("%+v", err))
				}
				// log.Info(fmt.Sprintf("insert rs: %+v", rs))

				log.Info(fmt.Sprintf("client %d loop %d(total %d): %+v, errMsg: %s", clientID, j, gqlStressEdgeOpts.loops, rs.IsSucceed(), rs.GetErrorMsg()))
				limiter.Wait(ctx)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func init() {
	rootCmd.AddCommand(gqlStressEdgeCmd)

	// Here you will define your flags and configuration settings.
	gqlStressEdgeCmd.Flags().IntVarP(&gqlStressEdgeOpts.batchSize, "batch-size", "", 1, "batch size")
	gqlStressEdgeCmd.Flags().IntVarP(&gqlStressEdgeOpts.clients, "clients", "", 1, "number of clients")
	gqlStressEdgeCmd.Flags().IntVarP(&gqlStressEdgeOpts.loops, "loops", "", 1, "number of loops")
	gqlStressEdgeCmd.Flags().IntVarP(&gqlStressEdgeOpts.rateLimit, "rate-limit", "", 100, "qps")
	gqlStressEdgeCmd.Flags().StringArrayVarP(&gqlStressEdgeOpts.graphAddrs, "graph-addr", "", []string{"graph1:9669"}, "graph addrs")

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// gqlStressEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// gqlStressEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
