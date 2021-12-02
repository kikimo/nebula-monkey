package chaos

import (
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
)

type SuspendLeaderCommand struct {
	remoteCtrl  *remote.RemoteController
	raftCluster *raft.RaftCluster
	interval    time.Duration
	BaseCommand
}

func (s *SuspendLeaderCommand) Execute() {
	previousLeader := ""

	for !s.Stopped() {
		leaderId, err := s.raftCluster.GetLeader()
		if err != nil {
			glog.Warningf("error finding leader: %+v\n", err)
			time.Sleep(16 * time.Millisecond)
			continue
		}

		glog.Infof("found current leader: %s, previous leader: %s", leaderId, previousLeader)
		_, err = s.remoteCtrl.SuspendStorage(remote.Host(leaderId))
		if err != nil {
			glog.Errorf("error suspending storage leader %s: %+v", leaderId, err)
		}

		// if leaderId == previousLeader {
		// 	time.Sleep(64 * time.Millisecond)
		// 	continue
		// }

		glog.Infof("suspending leader: %s", leaderId)
		previousLeader = leaderId
		time.Sleep(s.interval)
		_, err = s.remoteCtrl.ResumeStorage(remote.Host(leaderId))
		if err != nil {
			glog.Errorf("error resuming storage leader %s: %+v", leaderId, err)
		}
	}
}

func NewSuspendLeaderCommand(remoteCtrl *remote.RemoteController, raftCluster *raft.RaftCluster, interval time.Duration) *SuspendLeaderCommand {
	cmd := &SuspendLeaderCommand{
		remoteCtrl:  remoteCtrl,
		raftCluster: raftCluster,
		interval:    interval,
	}
	return cmd
}
