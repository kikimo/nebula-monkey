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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	nmrand "github.com/kikimo/nebula-monkey/pkg/rand"
	"github.com/spf13/cobra"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type B55Opts struct {
	loops int
	retry bool
}

var b55Opts B55Opts

// b55Cmd represents the b55 command
var b55Cmd = &cobra.Command{
	Use:   "b55",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		runB55()
	},
}

func init() {
	rootCmd.AddCommand(b55Cmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// b55Cmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// b55Cmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	b55Cmd.Flags().IntVarP(&b55Opts.loops, "loops", "", -1, "loops, default -1 (will loop forever)")
	b55Cmd.Flags().BoolVarP(&b55Opts.retry, "retry", "", true, "retry on fail")
}

type BalanceManager struct {
	session *nebula.Session
	space   string
}

func NewBalanceManager(session *nebula.Session, space string) (*BalanceManager, error) {
	rs, err := gonebula.ExecGQL(session, fmt.Sprintf("use %s", space))
	if err != nil {
		return nil, err
	}

	if !rs.IsSucceed() {
		return nil, fmt.Errorf("error switching space %s: %s", space, rs.GetErrorMsg())
	}

	bm := &BalanceManager{
		session: session,
		space:   space,
	}

	return bm, nil
}

func (bm *BalanceManager) Close() {
	if bm.session != nil {
		bm.session.Release()
	}
}

func (bm *BalanceManager) AddAllZoneToSpace() error {
	zones, err := gonebula.GetZones(bm.session)
	if err != nil {
		return err
	}

	for _, z := range zones {
		rs, err := gonebula.ExecGQL(bm.session, fmt.Sprintf("alter space %s add zone %s", bm.space, z))
		if err != nil {
			glog.Errorf("failed add zone %s: %+v", z, err)
			continue
		}

		if !rs.IsSucceed() {
			glog.Errorf("failed add zone %s: %s", z, rs.GetErrorMsg())
		}
	}

	time.Sleep(4 * time.Second)
	return nil
}

func (bm *BalanceManager) GetSpaceZones() ([]string, error) {
	// TODO
	return nil, nil
}

func (bm *BalanceManager) BalanceAcrossZone() (int64, error) {
	// TODO
	return 0, nil
}

func (bm *BalanceManager) BalanceData() (int64, error) {
	gql := fmt.Sprintf("submit job balance data;")
	rs := gonebula.ExeGqlNoError(bm.session, gql)
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.V(1).Infof("balance data job id: %d", *jobID)

	return *jobID, nil
}

func withQuote(victims []string) []string {
	ret := make([]string, len(victims))
	for i, v := range victims {
		parts := strings.Split(v, ":")
		ret[i] = fmt.Sprintf(`"%s":%s`, parts[0], parts[1])
	}

	return ret
}

func (bm *BalanceManager) BalanceRemoveHosts(victims []string) (int64, error) {
	gql := "balance data remove " + strings.Join(withQuote(victims), ", ")
	glog.Infof("balance remove node: %+v", gql)
	rs := gonebula.ExeGqlNoError(bm.session, gql)
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.V(1).Infof("balance data job id: %d", *jobID)

	return *jobID, nil
}

func (bm *BalanceManager) BalanceRemoveZone(victimZones []string) (int64, error) {
	gql := fmt.Sprintf("submit job balance across zone remove %s;", strings.Join(victimZones, ", "))
	rs := gonebula.ExeGqlNoError(bm.session, gql)
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.V(1).Infof("balance remove zone job id: %d, victim zone: %+v", *jobID, victimZones)

	return *jobID, nil
}

func (bm *BalanceManager) BalanceDataAndWait(retry bool) error {
	jobID, err := bm.BalanceData()
	if err != nil {
		return err
	}

	for {
		status, err := bm.WaitForJob(jobID)
		if err != nil {
			return err
		}

		if status == "FINISHED" || status == "STOPPED" {
			glog.V(1).Infof("balance data %d finished, return now", jobID)
			return nil
		}

		glog.V(1).Infof("balance data job %d finished with status %s", jobID, status)
		if !retry {
			glog.V(1).Infof("no more retry for job %d, return now", jobID)
			return nil
		}

		if err := bm.RecoverJob(jobID); err != nil {
			glog.Fatalf("failed recover job %d: %+v", jobID, err)
			return err
		}

	}
}

func (bm *BalanceManager) RemoveHostBalanceAndWait(victims []string, retry bool) error {
	jobID, err := bm.BalanceRemoveHosts(victims)
	if err != nil {
		return err
	}

	for {
		status, err := bm.WaitForJob(jobID)
		if err != nil {
			return err
		}

		if status == "FINISHED" {
			glog.V(1).Infof("balance remove job %d finished, return now", jobID)
			return nil
		}

		glog.V(1).Infof("balance data job %d finished with status %s", jobID, status)
		if !retry {
			glog.V(1).Infof("no more retry for job %d, return now", jobID)
			return nil
		}

		if err := bm.RecoverJob(jobID); err != nil {
			glog.Fatalf("failed recover job %d: %+v", jobID, err)
			return err
		}
	}
}

func (bm *BalanceManager) BalanceRemoveZoneAndWait(victims []string, retry bool) error {
	jobID, err := bm.BalanceRemoveZone(victims)
	if err != nil {
		return err
	}

	for {
		status, err := bm.WaitForJob(jobID)
		if err != nil {
			return err
		}

		if status == "FINISHED" {
			glog.V(1).Infof("balance remove job %d finished, return now", jobID)
			return nil
		}

		glog.V(1).Infof("balance remove job %d finished with status %s", jobID, status)
		if !retry {
			glog.V(1).Infof("no more retry for job %d, return now", jobID)
			return nil
		}

		if err := bm.RecoverJob(jobID); err != nil {
			glog.Fatalf("failed recover job %d: %+v", jobID, err)
			return err
		}
	}
}

func (bm *BalanceManager) RecoverJob(jobID int64) error {
	glog.V(1).Infof("recover job %d", jobID)
	gql := fmt.Sprintf("recover job %d;", jobID)
	gonebula.ExeGqlNoError(bm.session, gql)

	return nil
}

func (bm *BalanceManager) WaitForJob(jobID int64) (string, error) {
	for {
		rs := gonebula.ExeGqlNoError(bm.session, "show jobs")
		found := false
		for _, row := range rs.GetRows() {
			if jobID != *row.Values[0].IVal {
				continue
			}

			found = true
			status := string(row.Values[2].SVal)
			if status == "FINISHED" || status == "FAILED" || status == "STOPPED" {
				return status, nil
			}

			glog.V(1).Infof("job %d status %s, waiting...", jobID, status)
			time.Sleep(4 * time.Second)
			break
		}

		if !found {
			return "", fmt.Errorf("job %d not found", jobID)
		}
	}
}

func doRunBalanceData(wg sync.WaitGroup, bm *BalanceManager) error {
	loopForever := b55Opts.loops <= 0
	loops := b55Opts.loops
	for loopForever || loops > 0 {
		if loops > 0 {
			loops--
		}

		// 1. balance data
		glog.V(1).Infof("balance data...")
		if err := bm.BalanceDataAndWait(true); err != nil {
			return err
		}

		// 2. remove host and balance
		victimIdexes := nmrand.RandomChoiceFrom(len(globalBalanceOpts.storages), 2)
		victims := []string{globalBalanceOpts.storages[victimIdexes[0]], globalBalanceOpts.storages[victimIdexes[1]]}
		if err := bm.RemoveHostBalanceAndWait(victims, true); err != nil {
			return err
		}
	}

	return nil
}

func doRunB55(wg sync.WaitGroup, bm *BalanceManager) error {
	zones, err := gonebula.GetZones(bm.session)
	if err != nil {
		return err
	}

	loopForever := b55Opts.loops <= 0
	loops := b55Opts.loops
	for loopForever || loops > 0 {
		if loops > 0 {
			loops--
		}

		// 1. add all zones
		glog.V(1).Infof("add all zones to space")
		if err := bm.AddAllZoneToSpace(); err != nil {
			return err
		}
		time.Sleep(4 * time.Second)

		// 2. balance remove zone
		victimsIdx := nmrand.RandomChoiceFrom(len(zones), 2)
		victims := []string{zones[victimsIdx[0]], zones[victimsIdx[1]]}
		if err := bm.BalanceRemoveZoneAndWait(victims, b55Opts.retry); err != nil {
			return err
		}
	}

	return nil
}

func quoteHost(h string) string {
	parts := strings.Split(h, ":")
	if len(parts) != 2 {
		panic(fmt.Errorf("illegal host: %s", h))
	}

	qs := fmt.Sprintf(`"%s":%s`, parts[0], parts[1])
	return qs
}

func runB55() {
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

	// 1. add all hosts
	for _, h := range globalBalanceOpts.storages {
		cmd := fmt.Sprintf(`add hosts %s`, quoteHost(h))
		glog.Infof("executing: %s", cmd)
		gonebula.ExecGQL(session, cmd)
	}

	bm, err := NewBalanceManager(session, globalBalanceOpts.space)
	if err != nil {
		glog.Fatal(err)
	}
	defer bm.Close()

	// hosts, err := gonebula.GetHosts(session)
	// if err != nil {
	// 	glog.Fatal(err)
	// }
	// glog.Infof("hosts: %+v", hosts)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		<-sigs
		glog.Infof("stopping balance test...")
		// atomic.StoreUint32(&stopped, 1)
		wg.Done()
	}()

	// go doRunB55(wg, bm)
	go doRunBalanceData(wg, bm)
	wg.Wait()
}
