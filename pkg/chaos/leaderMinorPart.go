package chaos

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
)

type LeaderMinorPartCommand struct {
	remoteCtrl  *remote.RemoteController
	raftCluster *raft.RaftCluster
	interval    time.Duration
	BaseCommand
}

func (p *LeaderMinorPartCommand) Execute() {
	previousLeader := ""
	hosts := p.raftCluster.GetPeers()

	for !p.Stopped() {
		leaderId, err := p.raftCluster.GetLeader()
		if err != nil {
			glog.Warningf("error finding leader: %+v\n", err)
			time.Sleep(16 * time.Millisecond)
			continue
		}

		glog.Infof("found current leader: %s, previous leader: %s", leaderId, previousLeader)
		if leaderId == previousLeader {
			time.Sleep(64 * time.Millisecond)
			continue
		}

		previousLeader = leaderId
		leaderPart := remote.Partition{remote.Host(leaderId)}
		nonLeaderPart := remote.Partition{}
		for _, h := range hosts {
			if string(h.GetHost()) != leaderId {
				nonLeaderPart = append(nonLeaderPart, remote.Host(h.GetHost()))
			}
		}

		rand.Shuffle(len(nonLeaderPart), func(i, j int) {
			nonLeaderPart[i], nonLeaderPart[j] = nonLeaderPart[j], nonLeaderPart[i]
		})
		minorSize := len(hosts) / 2
		glog.Infof("minor size: %d", minorSize)
		for i := 0; i < minorSize-1; i++ {
			sz := len(nonLeaderPart)
			last := nonLeaderPart[sz-1]
			leaderPart = append(leaderPart, last)
			nonLeaderPart = nonLeaderPart[:sz-1]
			glog.Infof("move %s to leader part, leader part now: %+v", last, leaderPart)
		}

		parts := []remote.Partition{leaderPart, nonLeaderPart}
		glog.Infof("making partitions: %+v\n", parts)
		if err := p.remoteCtrl.MakePartition(parts); err != nil {
			fmt.Printf("error makeing raft partition %+v: %+v", parts, err)
		}

		glog.Infof("raft partitioned to: %+v\n", parts)
		time.Sleep(p.interval)
	}
}

func NewLeaderMinorPartCommand(remoteCtrl *remote.RemoteController, raftCluster *raft.RaftCluster, interval time.Duration) *LeaderMinorPartCommand {
	cmd := &LeaderMinorPartCommand{
		remoteCtrl:  remoteCtrl,
		raftCluster: raftCluster,
		interval:    interval,
	}

	return cmd
}
