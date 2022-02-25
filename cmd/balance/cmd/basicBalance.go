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
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/kikimo/nebula-monkey/pkg/rand"
	"github.com/spf13/cobra"
	nebula "github.com/vesoft-inc/nebula-go/v3"
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

What we gonna do: an all-in-one balance test implementation
	1. balance back and forth
	2. network isolation one by one
	3. restart storage one by one

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
		5.4 data consistency`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("basicBalance called")
		runBasicBalance()
	},
}

type BalanceManager struct {
	session *nebula.Session
}

func (bm *BalanceManager) BalanceAcrossZoneAndWait() error {
	zones, err := gonebula.GetZones(bm.session)
	if err != nil {
		return err
	}

	for _, z := range zones {
		gonebula.ExecGQL(bm.session, fmt.Sprintf("alter space %s add zone %s", globalBalanceOpts.space, z))
	}
	time.Sleep(4 * time.Second)

	rs := gonebula.ExeGqlNoError(bm.session, "submit job balance across zone")
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.Infof("balance job id: %d", *jobID)

	return bm.waitForBalanceJob(*jobID)
}

func (bm *BalanceManager) BalanceRemoveZonesAndWait(numZones int) error {
	zones, err := gonebula.GetZones(bm.session)
	if err != nil {
		return err
	}
	glog.Infof("total zones: %+v", zones)

	if len(zones)/2 < numZones {
		return fmt.Errorf("cannot remove %d zones, greater than half of total zone number %d", numZones, len(zones))
	}

	victimIndexes := rand.RandomChoiceFrom(len(zones), 2)
	// glog.Info("victim indexes: %+v", victimIndexes)
	victims := []string{}
	for _, idx := range victimIndexes {
		glog.Infof("adding victim: %s", zones[idx])
		victims = append(victims, zones[idx])
	}
	glog.Infof("victimes: %+v", victims)

	strings.Join(victims, ",")
	balanceRemoveStmt := fmt.Sprintf("submit job balance across zone remove %s", strings.Join(victims, ","))
	glog.Infof("balance removing zone: %+v", victims)
	rs := gonebula.ExeGqlNoError(bm.session, balanceRemoveStmt)
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.Infof("balance remove zone job id: %d", *jobID)

	return bm.waitForBalanceJob(*jobID)
}

func (bm *BalanceManager) waitForBalanceJob(jobID int64) error {
	for {
		rs := gonebula.ExeGqlNoError(bm.session, "show jobs")
		found := false
		for _, row := range rs.GetRows() {
			if jobID != *row.Values[0].IVal {
				continue
			}

			found = true
			status := string(row.Values[2].SVal)
			if status == "FINISHED" || status == "FAILED" {
				glog.Infof("job %d finished with status %s, return now", jobID, status)
				return nil
			}

			glog.Infof("job %d status %s, waiting...", jobID, status)
			time.Sleep(4 * time.Second)
			break
		}

		if !found {
			glog.Fatalf("job %d not found", jobID)
		}
	}
}

func NewBalanceManager(session *nebula.Session) *BalanceManager {
	bm := &BalanceManager{
		session: session,
	}

	return bm
}

// CREATE SPACE test (partition_num = 128, replica_factor = 5, charset = utf8, collate = utf8_bin, vid_type = INT64, atomic_edge = false)
// create edge known2(idx string, ts datetime default datetime());
func runBasicBalance() {
	glog.Info("basic balance running...")
	var log = nebula.DefaultLogger{}
	// batchSize := gqlStressEdgeOpts.batchSize
	graphHosts := []nebula.HostAddress{}

	for _, gaddr := range globalBalanceOpts.graphs {
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
		glog.Fatalf("err: %+v", err)
	}
	defer pool.Close()

	username, passwd := "root", "nebula"
	session, err := pool.GetSession(username, passwd)
	if err != nil {
		glog.Fatalf("failed getting session: %+v", err)
	}
	defer session.Release()

	gonebula.ExecGQL(session, fmt.Sprintf("use %s", globalBalanceOpts.space))
	bm := NewBalanceManager(session)
	for {
		if err := bm.BalanceAcrossZoneAndWait(); err != nil {
			glog.Fatalf("error balance across zone: %+v", err)
		}
		time.Sleep(4 * time.Second)

		if err := bm.BalanceRemoveZonesAndWait(2); err != nil {
			glog.Fatalf("error balance remove zone: %+v", err)
		}
		time.Sleep(4 * time.Second)
	}
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
