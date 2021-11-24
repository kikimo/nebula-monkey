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
	"math/rand"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/goremote"
	"github.com/kikimo/nebula-monkey/pkg/network"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/spf13/cobra"
)

var (
	partLeaderSpaceID  int32
	partLeaderPartID   int32
	partLeaderInterval int
)

// partCmd represents the part command
var partLeaderCmd = &cobra.Command{
	Use:   "partLeader",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunPartition()
	},
}

func RunPartition() {
	// 1. create network manager
	mgr := network.NewNetworkManager()
	hosts := []network.Host{}
	for _, p := range raftPeers {
		hosts = append(hosts, network.Host(p))
	}

	for _, h := range hosts {
		c, err := goremote.NewSSHClientBuilder().WithHost(string(h)).Build()
		if err != nil {
			glog.Fatalf("error creating ssh client: %+v", err)
		}

		mgr.RegisterHost(h, c)
	}

	// 2. create raft cluster
	cluster := raft.NewRaftCluster(partLeaderSpaceID, partLeaderPartID)
	for _, h := range raftPeers {
		id := h
		err := cluster.RegisterHost(id, h)
		if err != nil {
			glog.Fatalf("error registering raft host: %+v", err)
		}
	}

	previousLeader := ""
	for {
		leaderId, err := cluster.GetLeader()
		if err != nil {
			glog.Warningf("error finding leader: %+v\n", err)
			time.Sleep(16 * time.Millisecond)
			continue
		}

		glog.Infof("found current leader: %s, previous leader: %s", leaderId, previousLeader)
		if leaderId == previousLeader {
			time.Sleep(64 * time.Millisecond)
			continue
		}

		previousLeader = leaderId

		leaderPart := network.Partition{network.Host(leaderId)}
		nonLeaderPart := network.Partition{}
		for _, h := range hosts {
			if string(h) != leaderId {
				nonLeaderPart = append(nonLeaderPart, h)
			}
		}

		rand.Shuffle(len(nonLeaderPart), func(i, j int) {
			nonLeaderPart[i], nonLeaderPart[j] = nonLeaderPart[j], nonLeaderPart[i]
		})
		parts := []network.Partition{leaderPart, nonLeaderPart}
		glog.Infof("making partitions: %+v\n", parts)
		if err := mgr.MakePartition(parts); err != nil {
			fmt.Printf("error makeing raft partition %+v: %+v", parts, err)
		}

		glog.Infof("raft partitioned to: %+v\n", parts)
		// time.Sleep(time.Duration(rand.Int()%3) * time.Second)
		time.Sleep(time.Duration(partLeaderInterval) * time.Millisecond)
	}
	// 3. now, partition leader
	// mgr.Run(hosts[0], "ls -l /root")
	// mgr.Run(hosts[0], "iptables -C INPUT -s store2 -j DROP")
	// mgr.Disconnect(hosts[0], hosts[1])
	// mgr.Connect(hosts[0], hosts[1])
	// fmt.Printf("hosts: %+v\n", hosts)
	// fmt.Println("part called")
}

func init() {
	rootCmd.AddCommand(partLeaderCmd)
	partLeaderCmd.Flags().Int32VarP(&partLeaderSpaceID, "space", "s", 1, "specify nebula space id")
	partLeaderCmd.Flags().Int32VarP(&partLeaderPartID, "part", "t", 1, "specify nebula partition space id")
	partLeaderCmd.Flags().IntVarP(&partLeaderInterval, "interval", "i", 1000, "interval between setting partition(milliseconds)")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// partCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// partCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
