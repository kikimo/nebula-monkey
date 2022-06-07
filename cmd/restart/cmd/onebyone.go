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
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/goremote"
	"github.com/kikimo/nebula-monkey/pkg/gonebula"
	"github.com/spf13/cobra"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type OneByOneOpts struct {
	interval    int // restart interval, unit: second
	graphs      []string
	stopScript  string
	startScript string
}

var oneByOneOpts OneByOneOpts

// onebyoneCmd represents the onebyone command
var onebyoneCmd = &cobra.Command{
	Use:   "onebyone",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		runOneByOne()
	},
}

func init() {
	rootCmd.AddCommand(onebyoneCmd)
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// onebyoneCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// onebyoneCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	onebyoneCmd.Flags().IntVarP(&oneByOneOpts.interval, "interval", "", 16, "restart interval")
	onebyoneCmd.Flags().StringArrayVarP(&oneByOneOpts.graphs, "graph", "", []string{"graph1"}, "graph list")
	onebyoneCmd.Flags().StringVarP(&oneByOneOpts.startScript, "start-script", "", "/root/nebula-chaos-cluster/scripts/start-storage.sh", "start script")
	onebyoneCmd.Flags().StringVarP(&oneByOneOpts.stopScript, "stop-script", "", "/root/nebula-chaos-cluster/scripts/stop_store.sh", "stop script")
}

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

// 	glog.Infof("restart manager stopped.")
// 	doneWg.Done()
// }(sshClients)

type RestartController struct {
	session  *nebula.Session
	interval int
	hosts    []*Host
	stopped  uint32
}

func (rc *RestartController) Stop() {
	atomic.StoreUint32(&rc.stopped, 1)
}

func (rc *RestartController) IsStopped() bool {
	return atomic.LoadUint32(&rc.stopped) != 0
}

func (rc *RestartController) RecoverAll() {
	for _, h := range rc.hosts {
		_, err := h.sshClient.Run(oneByOneOpts.startScript)
		if err != nil {
			glog.Error("error recover %s: %+v", h.host.Host, err)
		}
	}
}

func randInt(lo, hi int) int {
	return rand.Intn(hi-lo) + lo
}

func (rc *RestartController) restartOneByOne(hosts []*Host) {
	for _, h := range hosts {
		if rc.IsStopped() {
			return
		}

		glog.Infof("restarting host: %s", h.host.Host)
		// TODO random interval
		pause := randInt(rc.interval/4, rc.interval)
		time.Sleep(time.Duration(pause) * time.Second)

		// restart by turn
		// now restart host one by one
		// strategy 1: restart one by one
		glog.V(1).Infof("stopping %s storage now...", h.host.Host)
		ret, err := h.sshClient.Run(oneByOneOpts.stopScript)
		if err != nil {
			glog.Errorf("error stopping %s, remote exec: %+v", h.host.Host, err)
		}

		glog.V(1).Infof("stdout: %s", ret.Stdout)
		glog.V(1).Infof("stderr: %s", ret.Stderr)

		time.Sleep(time.Duration(rc.interval) * time.Second)
		glog.V(1).Infof("starting %s now...", h.host.Host)
		ret, err = h.sshClient.Run(oneByOneOpts.startScript)
		if err != nil {
			glog.Errorf("error starting %s, remote exec: %+v", h.host.Host, err)
		}

		glog.V(1).Infof("stdout: %s", ret.Stdout)
		glog.V(1).Infof("stderr: %s", ret.Stderr)
		run := randInt(30, 60)
		glog.Infof("run for %d seconds", run)
		time.Sleep(time.Duration(run) * time.Second)

		// strategy 2: restart first two
		// cnt := len(sshClients) / 2
		// if cnt > 2 {
		// 	cnt = 2
		// }
		// glog.Infof("restarting %d hosts", cnt)

		// var wg sync.WaitGroup
		// wg.Add(cnt)
		// for i := 0; i < cnt; i++ {
		// 	go func(h *goremote.SSHClient, idx int) {
		// 		glog.Infof("restarting host: %d", idx)
		// 		ret, err := h.Run("/root/nebula-chaos-cluster/scripts/stop_store.sh")
		// 		if err != nil {
		// 			glog.Errorf("remote exec: %+v", err)
		// 		}

		// 		glog.Infof("stdout: %s", ret.Stdout)
		// 		glog.Infof("stderr: %s", ret.Stderr)

		// 		time.Sleep(2 * time.Second)
		// 		glog.Infof("starting storage now...")
		// 		ret, err = h.Run("/root/nebula-chaos-cluster/scripts/start-storage.sh")
		// 		if err != nil {
		// 			glog.Errorf("remote exec: %+v", err)
		// 		}

		// 		glog.Infof("stdout: %s", ret.Stdout)
		// 		glog.Infof("stderr: %s", ret.Stderr)

		// 		time.Sleep(8 * time.Second)

		// 		wg.Done()
		// 	}(sshClients[i], i)
		// }
		// wg.Wait()
	}
}

func (rc *RestartController) Run() {
	for !rc.IsStopped() {
		hosts := make([]*Host, len(rc.hosts))
		copy(hosts, rc.hosts)
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})

		rc.restartOneByOne(hosts)
	}
}

func (rc *RestartController) Close() {
	if rc.session != nil {
		rc.Close()
	}

	for _, h := range rc.hosts {
		if h.sshClient != nil {
			h.sshClient.Close()
		}
	}
}

type Host struct {
	host      gonebula.Host
	sshClient *goremote.SSHClient
}

func NewRestartController(session *nebula.Session, interval int, ghosts []gonebula.Host) (*RestartController, error) {
	hosts := []*Host{}
	for _, h := range ghosts {
		sc, err := goremote.NewSSHClientBuilder().WithHost(h.Host).Build()
		if err != nil {
			glog.Fatal(err)
		}

		host := &Host{
			host:      h,
			sshClient: sc,
		}
		hosts = append(hosts, host)
	}

	rc := &RestartController{
		session:  session,
		interval: interval,
		hosts:    hosts,
	}

	return rc, nil
}

func runOneByOne() {
	glog.Info("basic balance running...")
	var log = nebula.DefaultLogger{}
	// batchSize := gqlStressEdgeOpts.batchSize
	graphHosts := []nebula.HostAddress{}
	// ctx, cancel := context.WithCancel(context.Background())

	for _, gaddr := range oneByOneOpts.graphs {
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
	poolConfig.MaxConnPoolSize = 16
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

	hosts, err := gonebula.GetHosts(session)
	if err != nil {
		glog.Fatal(err)
	}

	glog.Infof("hosts: %+v", hosts)
	rc, err := NewRestartController(session, oneByOneOpts.interval, hosts)
	if err != nil {
		glog.Fatal(err)
	}
	defer rc.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		glog.Infof("stopping balance test...")
		rc.Stop()
	}()

	rc.Run()
	rc.RecoverAll()
}
