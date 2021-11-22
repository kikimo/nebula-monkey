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

// partCmd represents the part command
var partCmd = &cobra.Command{
	Use:   "part",
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
	hosts := []network.Host{
		"store1",
		"store2",
		"store3",
		"store4",
		"store5",
	}

	mgr := network.NewNetworkManager()
	for _, h := range hosts {
		c, err := goremote.NewSSHClientBuilder().WithHost(string(h)).Build()
		if err != nil {
			panic(err)
		}

		mgr.RegisterHost(h, c)
	}

	// mgr.Run(hosts[0], "ls -l /root")
	// mgr.Run(hosts[0], "iptables -C INPUT -s store2 -j DROP")
	mgr.Disconnect(hosts[0], hosts[1])
	mgr.Connect(hosts[0], hosts[1])
	// fmt.Printf("hosts: %+v\n", hosts)
	// fmt.Println("part called")
}

func init() {
	rootCmd.AddCommand(partCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// partCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// partCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
