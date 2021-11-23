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
	"github.com/kikimo/goremote"
	"github.com/kikimo/nebula-monkey/pkg/network"
	"github.com/spf13/cobra"
)

// healAllCmd represents the healAll command
var healAllCmd = &cobra.Command{
	Use:   "healAll",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		mgr := network.NewNetworkManager()
		defer mgr.Close()
		for _, p := range raftPeers {
			client, err := goremote.NewSSHClientBuilder().WithHost(p).Build()
			if err != nil {
				panic(err)
			}

			if err := mgr.RegisterHost(network.Host(p), client); err != nil {
				panic(err)
			}
		}

		if err := mgr.HealAll(); err != nil {
			panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(healAllCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// healAllCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// healAllCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
