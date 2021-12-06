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
	"flag"
	"fmt"
	"os"

	"github.com/golang/glog"
	"github.com/kikimo/goremote"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vesoft-inc/nebula-go/v2/nebula"

	"github.com/spf13/viper"
)

var cfgFile string

var raftPeers []string
var defaultRaftPeers []string = []string{
	"store1",
	"store2",
	"store3",
	"store4",
	"store5",
}

var (
	graphSpaceID nebula.GraphSpaceID
	partitionID  nebula.PartitionID
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "nebula-monkey",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		flag.Parse()
	},
}

func createRemoteController() *remote.RemoteController {
	ctrl := remote.NewRemoteController()
	hosts := []remote.Host{}
	for _, p := range raftPeers {
		hosts = append(hosts, remote.Host(p))
	}

	for _, h := range hosts {
		c, err := goremote.NewSSHClientBuilder().WithHost(string(h)).Build()
		if err != nil {
			glog.Errorf("error creating ssh client: %+v", err)
		}

		ctrl.RegisterHost(h, c)
	}

	return ctrl
}

func createRaftCluster(spaceID nebula.GraphSpaceID, partID nebula.PartitionID) *raft.RaftCluster {
	cluster := raft.NewRaftCluster(spaceID, partID)

	for _, h := range raftPeers {
		id := h
		if err := cluster.RegisterHost(id, h); err != nil {
			glog.Fatalf("error creating raft cluster: %+v", err)
		}
	}

	return cluster
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.nebula-monkey.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.PersistentFlags().StringArrayVarP(&raftPeers, "peers", "e", defaultRaftPeers, "specify raft peers")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".nebula-monkey" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".nebula-monkey")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
