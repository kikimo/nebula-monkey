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
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	nmrand "github.com/kikimo/nebula-monkey/pkg/rand"
	"github.com/spf13/cobra"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type BasicBalanceOpts struct {
	startScript   string
	stopScript    string
	loops         int
	retryOnFailed bool
}

var basicBalanceOpts BasicBalanceOpts

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
		5.2 storage core
		5.3 node offline?
		5.4 ok to insert edge?
		5.5 data consistency`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("basicBalance called")
		runBasicBalance()
	},
}

type BalanceManager struct {
	session *nebula.Session
}

func (bm *BalanceManager) addAllZones() error {
	zones, err := gonebula.GetZones(bm.session)
	if err != nil {
		return err
	}

	for _, z := range zones {
		gonebula.ExecGQL(bm.session, fmt.Sprintf("alter space %s add zone %s", globalBalanceOpts.space, z))
	}

	return nil
}

func (bm *BalanceManager) doBalanceAcrossZoneAndWait(ctx context.Context) (int64, error) {
	bm.addAllZones()
	time.Sleep(4 * time.Second)

	rs := gonebula.ExeGqlNoError(bm.session, "submit job balance across zone")
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.Infof("balance job id: %d", *jobID)

	return *jobID, nil
	// return bm.waitForBalanceJob(ctx, *jobID)
}

func (bm *BalanceManager) BalanceAcrossZoneAndWait(ctx context.Context, retry bool) error {
	var jobID int64
	var err error
	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("wait canceled, return now.")
			return fmt.Errorf("stopped")
		default:
		}

		jobID, err = bm.doBalanceAcrossZoneAndWait(ctx)
		if err != nil {
			return err
		}

		if !bm.IsEmptyJob(jobID) {
			break
		}

		glog.Infof("empty job id, retry")
	}

	for {
		status, err := bm.waitForBalanceJob(ctx, jobID)
		if err != nil {
			return err
		}

		if status == "FINISHED" {
			return nil
		}

		if bm.IsEmptyJob(jobID) {
			glog.Infof("empty job %d, return now", jobID)
			return nil
		}

		if !retry {
			return fmt.Errorf("balance across zone failed")
		}

		glog.Infof("job %d failed, retrying...", jobID)
		gonebula.ExeGqlNoError(bm.session, fmt.Sprintf("recover job %d", jobID))
	}
}

func (bm *BalanceManager) doBalanceRemoveZoneAndWait(ctx context.Context, numZones int) (int64, error) {
	zones, err := gonebula.GetZones(bm.session)
	if err != nil {
		return 0, err
	}
	glog.Infof("total zones: %+v", zones)

	if len(zones)/2 < numZones {
		return 0, fmt.Errorf("cannot remove %d zones, greater than half of total zone number %d", numZones, len(zones))
	}

	victimIndexes := nmrand.RandomChoiceFrom(len(zones), 2)
	// glog.Info("victim indexes: %+v", victimIndexes)
	victims := []string{}
	for _, idx := range victimIndexes {
		glog.V(1).Infof("adding victim: %s", zones[idx])
		victims = append(victims, zones[idx])
	}
	glog.V(1).Infof("victimes: %+v", victims)

	strings.Join(victims, ",")
	balanceRemoveStmt := fmt.Sprintf("submit job balance across zone remove %s", strings.Join(victims, ","))
	glog.Infof("balance removing zone: %+v", victims)
	rs := gonebula.ExeGqlNoError(bm.session, balanceRemoveStmt)
	jobID := rs.GetRows()[0].Values[0].IVal
	glog.Infof("balance remove zone job id: %d", *jobID)

	return *jobID, nil
	// return bm.waitForBalanceJob(ctx, *jobID)
}

func (bm *BalanceManager) BalanceRemoveZonesAndWait(ctx context.Context, numZones int, retry bool) (chan struct{}, error) {
	jobID, err := bm.doBalanceRemoveZoneAndWait(ctx, numZones)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{}, 1)

	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("wait canceled, return now.")
			return nil, fmt.Errorf("stopped")
		default:
		}

		status, err := bm.waitForBalanceJob(ctx, jobID)
		if err != nil {
			return nil, err
		}

		if status == "FINISHED" {
			return done, nil
		}

		if bm.IsEmptyJob(jobID) {
			glog.Infof("empty job %d, return now", jobID)
			return done, nil
		}

		if !retry {
			return nil, fmt.Errorf("balance across zone failed")
		}

		glog.Infof("job %d failed, retrying...", jobID)
		gonebula.ExeGqlNoError(bm.session, fmt.Sprintf("recover job %d", jobID))
	}
}

func (bm *BalanceManager) doWaitForBalanceJob(ctx context.Context, jobID int64) chan string {
	waitDone := make(chan string, 1)
	localDone := make(chan string)
	var stopped int32 = 0

	go func() {
		for atomic.LoadInt32(&stopped) == 0 {
			rs := gonebula.ExeGqlNoError(bm.session, "show jobs")
			found := false
			for _, row := range rs.GetRows() {
				if jobID != *row.Values[0].IVal {
					continue
				}

				found = true
				status := string(row.Values[2].SVal)
				if status == "FINISHED" || status == "FAILED" {
					localDone <- status
					return
				}

				glog.V(1).Infof("job %d status %s, waiting...", jobID, status)
				time.Sleep(4 * time.Second)
				break
			}

			if !found {
				localDone <- "error, job not found"
				return
			}
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			atomic.StoreInt32(&stopped, 1)
			waitDone <- "error, stopped"
		case ret := <-localDone:
			waitDone <- ret
		}
	}()

	return waitDone
}

func (bm *BalanceManager) waitForBalanceJob(ctx context.Context, jobID int64) (string, error) {
	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("wait canceled, return now.")
			return "", fmt.Errorf("stopped")
		default:
		}

		rs := gonebula.ExeGqlNoError(bm.session, "show jobs")
		found := false
		for _, row := range rs.GetRows() {
			if jobID != *row.Values[0].IVal {
				continue
			}

			found = true
			status := string(row.Values[2].SVal)
			if status == "FINISHED" || status == "FAILED" {
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

func (bm *BalanceManager) IsEmptyJob(jobID int64) bool {
	rs, err := gonebula.ExecGQL(bm.session, fmt.Sprintf("show job %d", jobID))
	if err != nil {
		glog.Warningf("error testing job: %+v", err)
		return true
	}
	glog.Infof("row count for job %d: %d", jobID, len(rs.GetRows()))

	return len(rs.GetRows()) == 2
}

func NewBalanceManager(session *nebula.Session) *BalanceManager {
	bm := &BalanceManager{
		session: session,
	}

	return bm
}

// new strategy
// 1. balance across all
// 2. random remove two
// loop:
// 3. add all hosts zone
// 4. randome remove another two

// CREATE SPACE test (partition_num = 128, replica_factor = 5, charset = utf8, collate = utf8_bin, vid_type = INT64, atomic_edge = false)
// create edge known2(idx string, ts datetime default datetime());
func runBasicBalance() {
	glog.Info("basic balance running...")
	var log = nebula.DefaultLogger{}
	// batchSize := gqlStressEdgeOpts.batchSize
	graphHosts := []nebula.HostAddress{}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	var stopped int32 = 0
	var doneWg sync.WaitGroup
	doneWg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-sigs
		glog.Infof("stopping balance test...")
		atomic.StoreInt32(&stopped, 1)
		cancel()
	}()

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

	gonebula.ExeGqlNoError(session, fmt.Sprintf("use %s", globalBalanceOpts.space))
	hosts, err := gonebula.GetHosts(session)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Infof("hosts: %+v", hosts)

	// sshClients := []*goremote.SSHClient{}
	// for _, h := range hosts {
	// 	sc, err := goremote.NewSSHClientBuilder().WithHost(h.Host).Build()
	// 	if err != nil {
	// 		glog.Fatal(err)
	// 	}

	// 	sshClients = append(sshClients, sc)
	// }

	// perform cleanup before exit
	// defer func() {
	// 	glog.Infof("restoring nebula-storaged...")
	// 	for _, c := range sshClients {
	// 		c.Run("/root/nebula-chaos-cluster/scripts/start-storage.sh")
	// 	}

	// 	for _, c := range sshClients {
	// 		c.Close()
	// 	}

	// 	time.Sleep(30 * time.Second)
	// 	glog.Info("collecting cluster status:")
	// 	nhosts := gonebula.ShowHosts(session)
	// 	for _, h := range nhosts {
	// 		if h.Status == "OFFLINE" {
	// 			glog.Errorf("host %s offline, detail: %+v", h.Host, h)
	// 		}
	// 	}
	// }()

	// // restart hosts
	// go func(sshClients []*goremote.SSHClient) {
	// 	glog.Infof("running restart manager...")
	// 	for atomic.LoadInt32(&stopped) == 0 {
	// 		// restart by turn
	// 		rand.Shuffle(len(sshClients), func(i, j int) {
	// 			sshClients[i], sshClients[j] = sshClients[j], sshClients[i]
	// 		})
	// 		glog.V(1).Infof("stopping storage by turn")

	// 		// now restart host one by one
	// 		// strategy 1: restart one by one
	// 		for _, h := range sshClients {
	// 			if atomic.LoadInt32(&stopped) != 0 {
	// 				break
	// 			}

	// 			glog.V(1).Infof("stopping` storage now...")
	// 			ret, err := h.Run(basicBalanceOpts.stopScript)
	// 			if err != nil {
	// 				glog.Errorf("error stopping storage, remote exec: %+v", err)
	// 			}

	// 			glog.V(1).Infof("stdout: %s", ret.Stdout)
	// 			glog.V(1).Infof("stderr: %s", ret.Stderr)

	// 			time.Sleep(4 * time.Second)
	// 			glog.V(1).Infof("starting storage now...")
	// 			ret, err = h.Run(basicBalanceOpts.startScript)
	// 			if err != nil {
	// 				glog.Errorf("error starting storage, remote exec: %+v", err)
	// 			}

	// 			glog.V(1).Infof("stdout: %s", ret.Stdout)
	// 			glog.V(1).Infof("stderr: %s", ret.Stderr)

	// 			time.Sleep(8 * time.Second)
	// 		}

	// 		// strategy 2: restart first two
	// 		// cnt := len(sshClients) / 2
	// 		// if cnt > 2 {
	// 		// 	cnt = 2
	// 		// }
	// 		// glog.Infof("restarting %d hosts", cnt)

	// 		// var wg sync.WaitGroup
	// 		// wg.Add(cnt)
	// 		// for i := 0; i < cnt; i++ {
	// 		// 	go func(h *goremote.SSHClient, idx int) {
	// 		// 		glog.Infof("restarting host: %d", idx)
	// 		// 		ret, err := h.Run("/root/nebula-chaos-cluster/scripts/stop_store.sh")
	// 		// 		if err != nil {
	// 		// 			glog.Errorf("remote exec: %+v", err)
	// 		// 		}

	// 		// 		glog.Infof("stdout: %s", ret.Stdout)
	// 		// 		glog.Infof("stderr: %s", ret.Stderr)

	// 		// 		time.Sleep(2 * time.Second)
	// 		// 		glog.Infof("starting storage now...")
	// 		// 		ret, err = h.Run("/root/nebula-chaos-cluster/scripts/start-storage.sh")
	// 		// 		if err != nil {
	// 		// 			glog.Errorf("remote exec: %+v", err)
	// 		// 		}

	// 		// 		glog.Infof("stdout: %s", ret.Stdout)
	// 		// 		glog.Infof("stderr: %s", ret.Stderr)

	// 		// 		time.Sleep(8 * time.Second)

	// 		// 		wg.Done()
	// 		// 	}(sshClients[i], i)
	// 		// }
	// 		// wg.Wait()
	// 	}

	// 	glog.Infof("restart manager stopped.")
	// 	doneWg.Done()
	// }(sshClients)

	go func(ctx context.Context, session *nebula.Session) {
		// time.Sleep(3600 * time.Second)
		glog.Infof("running balance manager job...")
		bm := NewBalanceManager(session)
		loops := basicBalanceOpts.loops
		bm.addAllZones()

		for atomic.LoadInt32(&stopped) == 0 && (loops < 0 || loops > 0) {
			if loops > 0 {
				loops--
			}

			if _, err := bm.BalanceRemoveZonesAndWait(ctx, 2, basicBalanceOpts.retryOnFailed); err != nil {
				glog.Errorf("balance remove zone failed: %+v", err)
			} else {
				glog.Error("done balance remove zone.")
			}
			time.Sleep(4 * time.Second)

			if err := bm.BalanceAcrossZoneAndWait(ctx, basicBalanceOpts.retryOnFailed); err != nil {
				glog.Errorf("balance across zone failed: %+v", err)
			} else {
				glog.Info("done balance across zone.")
			}
			time.Sleep(4 * time.Second)
		}

		glog.Infof("balance manager stopped.")
		atomic.StoreInt32(&stopped, 1)
		doneWg.Done()
	}(ctx, session)

	// wait for balance manager and restart loop to stop
	doneWg.Wait()
}

func init() {
	rootCmd.AddCommand(basicBalanceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// basicBalanceCmd.PersistentFlags().String("foo", "", "A help for foo")
	basicBalanceCmd.Flags().StringVarP(&basicBalanceOpts.startScript, "start-script", "", "/root/nebula-chaos-cluster/scripts/start-storage.sh", "path of start script")
	basicBalanceCmd.Flags().StringVarP(&basicBalanceOpts.stopScript, "stop-script", "", "/root/nebula-chaos-cluster/scripts/stop_store.sh", "path of stop script")
	basicBalanceCmd.Flags().IntVarP(&basicBalanceOpts.loops, "loops", "", 1, "balance loops")
	basicBalanceCmd.Flags().BoolVarP(&basicBalanceOpts.retryOnFailed, "retry", "", true, "retry on fail")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// basicBalanceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
