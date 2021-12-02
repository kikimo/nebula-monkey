package chaos

import (
	"math/rand"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
)

type Figure8Command struct {
	remoteCtrl  *remote.RemoteController
	raftCluster *raft.RaftCluster
	interval    time.Duration
	BaseCommand
}

func (c *Figure8Command) Execute() {
	hosts := c.remoteCtrl.GetHosts()
	hostNum := len(hosts)
	majorityNum := hostNum/2 + 1
	downList := map[string]struct{}{}
	electionTimeout := 2000
	for !c.Stopped() {
		leaderID, err := c.raftCluster.GetLeader()

		if (rand.Int() % 1000) < 100 {
			ms := time.Duration(rand.Int()%(electionTimeout/2)) * time.Millisecond
			time.Sleep(ms)
		} else {
			ms := time.Duration(rand.Int()%13) * time.Millisecond
			time.Sleep(ms)
		}

		// isolate leader
		if err == nil {
			if err = c.remoteCtrl.IsolateHost(remote.Host(leaderID)); err != nil {
				glog.Error(err)
			}

			downList[leaderID] = struct{}{}
		}

		if len(downList) < majorityNum {
			s := rand.Int() % hostNum
			target := hosts[s]
			if _, ok := downList[string(target)]; ok {
				if err := c.remoteCtrl.RejoinHost(target); err != nil {
					glog.Error(err)
				}

				delete(downList, string(target))
			}
		}
	}
}

func NewFigure8Command(ctrl *remote.RemoteController, cluster *raft.RaftCluster, interval time.Duration) *Figure8Command {
	cmd := &Figure8Command{
		remoteCtrl:  ctrl,
		raftCluster: cluster,
		interval:    interval,
	}
	return cmd
}
