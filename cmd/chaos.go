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
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/chaos"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
)

var (
	chaosCommand           string
	chaosSpaceId           nebula.GraphSpaceID
	chaosPartId            nebula.PartitionID
	chaosOperationInterval int
)

const (
	CommandPartLeader    = "partLeader"
	CommandSuspendLeader = "suspendLeader"
	CommandFigure8       = "figure8"
	RestartLeader        = "restartLeader"
	RandomHalves         = "randomHalves"
	MinorLeaderPart      = "minorLeaderPart"
	PartOneByOne         = "partOneByOne"
)

// chaosCmd represents the chaos command
var chaosCmd = &cobra.Command{
	Use:   "chaos",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		var _cmd chaos.ChaosCommand

		glog.Infof("interval: %d", chaosOperationInterval)
		interval := time.Duration(chaosOperationInterval) * time.Millisecond
		switch chaosCommand {
		case CommandPartLeader:
			remoteCtl := createRemoteController()
			raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewPartLeaderCommand(remoteCtl, raftCluster, interval)

		case CommandSuspendLeader:
			glog.Info("executing suspending leader test...")
			remoteCtl := createRemoteController()
			raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewSuspendLeaderCommand(remoteCtl, raftCluster, interval)

		case CommandFigure8:
			remoteCtl := createRemoteController()
			raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewFigure8Command(remoteCtl, raftCluster, interval)

		case RestartLeader:
			glog.Info("executing restarting leader test...")
			remoteCtl := createRemoteController()
			raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewRestartLeaderCommand(remoteCtl, raftCluster, interval)

		case RandomHalves:
			glog.Info("executing restarting leader test...")
			remoteCtl := createRemoteController()
			raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewRandomHalvesCommand(remoteCtl, raftCluster, interval)

		case MinorLeaderPart:
			glog.Info("executing minor leader part leader test...")
			remoteCtl := createRemoteController()
			raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewLeaderMinorPartCommand(remoteCtl, raftCluster, interval)

		case PartOneByOne:
			glog.Info("executing minor leader part leader test...")
			remoteCtl := createRemoteController()
			// raftCluster := createRaftCluster(chaosSpaceId, chaosPartId)
			_cmd = chaos.NewPartOneByOne(remoteCtl, nil, interval)

		default:
			cmd.PrintErrf("unknown strategy: %s\n", chaosCommand)
			os.Exit(1)
		}

		_cmd.Execute()
		// TODO closed at exit
	},
}

func init() {
	rootCmd.AddCommand(chaosCmd)

	chaosCmd.Flags().StringVarP(&chaosCommand, "command", "m", "partLeader", "chaos command, available options includeing: [partLeader, suspendLeader,figure8, restartLeader, randomHalves, minorLeaderPart, partOneByOne]")
	chaosCmd.Flags().Int32VarP(&chaosSpaceId, "space", "s", 1, "nebula space id")
	chaosCmd.Flags().Int32VarP(&chaosPartId, "part", "p", 1, "nebula part id")
	chaosCmd.Flags().IntVarP(&chaosOperationInterval, "interval", "i", 1000, "chao operation interval(unit: ms)")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// chaosCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// chaosCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
