package chaos

import (
	"math/rand"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
)

type RandomHalvesCommand struct {
	remoteCtrl  *remote.RemoteController
	raftCluster *raft.RaftCluster
	interval    time.Duration
	BaseCommand
}

func partitionRandomHalves(hosts []remote.Host) []remote.Partition {
	copied := make([]remote.Host, len(hosts))
	copy(copied, hosts)

	rand.Shuffle(len(copied), func(i, j int) {
		copied[i], copied[j] = copied[j], copied[i]
	})

	mid := len(copied) / 2
	return []remote.Partition{
		copied[:mid], copied[mid:],
	}
}

func (r *RandomHalvesCommand) Execute() {
	hosts := r.remoteCtrl.GetHosts()
	for !r.Stopped() {
		parts := partitionRandomHalves(hosts)
		glog.Infof("making partitions: %+v", parts)
		if err := r.remoteCtrl.MakePartition(parts); err != nil {
			glog.Infof("failed making partition: %+v", err)
		} else {
			glog.Infof("done making partition")
		}

		time.Sleep(r.interval)
	}
}

func NewRandomHalvesCommand(remoteCtrl *remote.RemoteController, raftCluster *raft.RaftCluster, interval time.Duration) *RandomHalvesCommand {
	cmd := &RandomHalvesCommand{
		remoteCtrl:  remoteCtrl,
		raftCluster: raftCluster,
		interval:    interval,
	}

	return cmd
}
