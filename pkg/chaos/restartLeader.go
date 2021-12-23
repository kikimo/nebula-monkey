package chaos

import (
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
)

type RestartLeaderCommand struct {
	remoteCtrl  *remote.RemoteController
	raftCluster *raft.RaftCluster
	interval    time.Duration
	BaseCommand
}

func (r *RestartLeaderCommand) Execute() {
	// hosts := r.remoteCtrl.GetHosts()
	// addr := "http://localhost:9001/RPC2"
	for !r.Stopped() {
		// 1. find leader
		glog.Info("retrieving raft leader...")
		leader, err := r.raftCluster.GetLeader()
		if err != nil {
			glog.Errorf("error getting leader: %+v", err)
			time.Sleep(r.interval)
			continue
		}

		// 2. stop leader
		glog.Infof("stopping leader: %s", leader)
		if err := r.remoteCtrl.StopProcess(remote.Host(leader)); err != nil {
			glog.Fatalf("error stoping storaged: %+v", err)
		}

		// 3. wait for new leader
		glog.Info("waiting new leader...")
		for !r.Stopped() {
			time.Sleep(r.interval)
			newLeader, err := r.raftCluster.GetLeader()
			if err != nil {
				glog.Errorf("error getting leader: %+v", err)
				continue
			}

			if newLeader == leader {
				// no new leader, keep waiting
				glog.V(2).Infof("no new leader found, old leader %s, keep waiting", leader)
				continue
			}

			glog.Infof("found new leader: %s", newLeader)
			break
		}

		// 4. start leader
		glog.Infof("starting stopped leader %s", leader)
		if err := r.remoteCtrl.StartProcess(remote.Host(leader)); err != nil {
			glog.Fatalf("failed starting storaged %s, err: %+v", leader, err)
		}
	}
}

func NewRestartLeaderCommand(remoteCtrl *remote.RemoteController, raftCluster *raft.RaftCluster, interval time.Duration) *RestartLeaderCommand {
	cmd := &RestartLeaderCommand{
		remoteCtrl:  remoteCtrl,
		raftCluster: raftCluster,
		interval:    interval,
	}

	return cmd
}
