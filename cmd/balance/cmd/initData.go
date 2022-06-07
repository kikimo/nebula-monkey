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
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
)

type InitDataOpts struct {
	parts     int
	replicas  int
	rateLimit int
	clients   int
}

var initDataOpts InitDataOpts

// space      string
// graphAddrs []string
// rateLimit  int
// clients    int
// batchSize  int
// loops      int

// initDataCmd represents the initData command
var initDataCmd = &cobra.Command{
	Use:   "initData",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunInitData()
	},
}

func RunInitData() {
	username, passwd := "root", "nebula"
	pool, err := gonebula.CreateGraphConnPool(globalBalanceOpts.graphs, 2048)
	if err != nil {
		glog.Fatal(err)
	}
	defer pool.Close()

	session, err := pool.GetSession(username, passwd)
	if err != nil {
		glog.Fatal(err)
	}
	defer session.Release()

	// 1. add hosts
	glog.Infof("adding hosts...")
	for _, h := range globalBalanceOpts.storages {
		// TODO:use default port 9779
		addHosts := fmt.Sprintf(`add hosts "%s":9779`, h)
		glog.Info(addHosts)
		gonebula.ExecGQL(session, addHosts)
	}
	time.Sleep(10 * time.Second)

	// 2. create space
	glog.Info("creating space...")
	createSpace := fmt.Sprintf("CREATE SPACE `%s` (partition_num = %d, replica_factor = %d, vid_type = INT64, atomic_edge = false)", globalBalanceOpts.space, initDataOpts.parts, initDataOpts.replicas)
	glog.Infof("%s", createSpace)

	gonebula.ExeGqlNoError(session, createSpace)
	for {
		time.Sleep(1 * time.Second)
		_, err := gonebula.ExecGQL(session, fmt.Sprintf("use %s", globalBalanceOpts.space))
		if err == nil {
			break
		}

		glog.Infof("wait for space %s to be created", globalBalanceOpts.space)
	}

	// 3. create edge
	glog.Info("creating edge...")
	createEdge := "create edge known2(idx string, ts datetime default datetime());"
	glog.Info(createEdge)
	gonebula.ExeGqlNoError(session, createEdge)
	// wait for edge to be created
	time.Sleep(10 * time.Second)
}

func init() {
	rootCmd.AddCommand(initDataCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// initDataCmd.PersistentFlags().String("foo", "", "A help for foo")
	initDataCmd.Flags().IntVarP(&initDataOpts.parts, "parts", "", 128, "number of parts")
	initDataCmd.Flags().IntVarP(&initDataOpts.replicas, "replicas", "", 5, "number of replicas")
	// initDataCmd.Flags().IntVarP(&initDataOpts.rateLimit, "rate-limit", "", 10000, "rate limit")
	// initDataCmd.Flags().IntVarP(&initDataOpts.clients, "clients", "", 16, "num of clients")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// initDataCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
