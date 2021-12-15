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
	glog.Infof("runing figure8 test...")
	hosts := c.remoteCtrl.GetHosts()
	hostNum := len(hosts)
	majorityNum := hostNum/2 + 1
	downList := map[string]struct{}{}
	for !c.Stopped() {
		leaderID, err := c.raftCluster.GetLeader()

		if (rand.Int() % 1000) < 100 {
			time.Sleep(c.interval)
		} else {
			ms := time.Duration(rand.Int()%13) * time.Millisecond
			time.Sleep(ms)
		}

		// isolate leader
		if err == nil {
			if err = c.remoteCtrl.IsolateHost(remote.Host(leaderID)); err != nil {
				glog.Error(err)
			}

			if _, ok := downList[leaderID]; !ok {
				downList[leaderID] = struct{}{}
				glog.Infof("leader %s isolated, down list: %+v", leaderID, downList)
			} else {
				glog.Infof("found isolated leader: %s, down list: %+v, continue", leaderID, downList)
			}
		}

		

		if len(downList) >= majorityNum {
			s := rand.Int() % hostNum
			target := hosts[s]
			if _, ok := downList[string(target)]; ok {
				glog.Infof("rejoing instance %s", target)
				if err := c.remoteCtrl.RejoinHost(target); err != nil {
					glog.Error(err)
				}

				delete(downList, string(target))
				for h := range downList {
					if err := c.remoteCtrl.IsolateHost(remote.Host(h)); err != nil {
						glog.Errorf("error isolate host %s: %+v", h, err)
					}
				}
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
