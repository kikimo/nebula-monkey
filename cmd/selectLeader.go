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
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/remote"
	"github.com/spf13/cobra"
)

var (
	selectLeaderTargetLeader    string
	selectLeaderInterval        int
	selectLeaderDefaultInterval int = 1600
)

// selectLeaderCmd represents the selectLeader command
var selectLeaderCmd = &cobra.Command{
	Use:   "selectLeader",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		runSelectLeader()
	},
}

func runSelectLeader() {
	if selectLeaderTargetLeader == "" {
		glog.Fatalf("target leader cannot be empty")
	}
	remoteCtl := createRemoteController()
	raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)

	hostFound := false
	hosts := remoteCtl.GetHosts()
	for _, host := range hosts {
		if host == remote.Host(selectLeaderTargetLeader) {
			hostFound = true
			break
		}
	}

	if !hostFound {
		glog.Fatalf("host %s not found", selectLeaderTargetLeader)
	}

	interval := time.Millisecond * time.Duration(selectLeaderInterval)
	for {
		leader, err := raftCluster.GetLeader()
		if err != nil {
			glog.Fatal(err)
		}

		glog.Infof("current leader: %s", leader)
		if leader == selectLeaderTargetLeader {
			glog.Infof("%s promoted to leader, return now", selectLeaderTargetLeader)
			break
		}

		glog.Infof("isolating leader %s", leader)
		if err := remoteCtl.IsolateHost(remote.Host(leader)); err != nil {
			glog.Fatalf("error isolating leader %s: %+v", leader, err)
		}
		time.Sleep(interval)

		glog.Infof("rejoin host %s", leader)
		if err := remoteCtl.HealAll(); err != nil {
			glog.Fatalf("error rejoining host %s: %+v", leader, err)
		}
	}
}

func init() {
	rootCmd.AddCommand(selectLeaderCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// selectLeaderCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// selectLeaderCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	selectLeaderCmd.Flags().StringVarP(&selectLeaderTargetLeader, "leader", "l", "", "target leader")
	selectLeaderCmd.MarkFlagRequired("leader")
	selectLeaderCmd.Flags().IntVarP(&selectLeaderInterval, "interval", "i", selectLeaderDefaultInterval, "isolating interval, default 1600")
}
