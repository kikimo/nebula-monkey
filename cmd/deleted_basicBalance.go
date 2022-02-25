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

	"github.com/spf13/cobra"
)

// basicBalanceCmd represents the basicBalance command
var basicBalanceCmd = &cobra.Command{
	Use:   "basicBalance",
	Short: "Basic balance test",
	Long: `Basic balance test: assumption:
		1. test space with
			1.1 5 replicas
			1.2 128 parts
			1.3. 400w edges
			1.4 on 7 nodes
	
		what we gonna do: a all-in-one balance test implementation
			1. remove two nodes and balance data
				+ wait for balance to complete or fail
			2. add two nodes and balance data
			+ wait for balance to complete or fail
			3. random partition network
			4. keep inserting data
			5. check result
			5.1 balance succ of failed?
				5.2 node offline?
			5.3 ok to insert edge?
				5.4 data consistency
`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("basicBalance called")
	},
}

func runBasicBalance() {
}

func init() {
	rootCmd.AddCommand(basicBalanceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// basicBalanceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// basicBalanceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
