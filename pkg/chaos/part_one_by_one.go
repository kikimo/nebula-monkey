package chaos

import (
	"math/rand"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/kikimo/nebula-monkey/pkg/remote"
)

type PartOneByOneCommand struct {
	remoteCtrl  *remote.RemoteController
	raftCluster *raft.RaftCluster
	interval    time.Duration
	BaseCommand
}

// func partitionRandomHalves(hosts []remote.Host) []remote.Partition {
// 	copied := make([]remote.Host, len(hosts))
// 	copy(copied, hosts)

// 	rand.Shuffle(len(copied), func(i, j int) {
// 		copied[i], copied[j] = copied[j], copied[i]
// 	})

// 	mid := len(copied) / 2
// 	return []remote.Partition{
// 		copied[:mid], copied[mid:],
// 	}
// }

func (r *PartOneByOneCommand) Execute() {
	for !r.Stopped() {
		hosts := r.remoteCtrl.GetHosts()
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})

		left, right := []remote.Host{}, hosts
		for len(right) > 0 {
			left = append(left, right[0])
			right = right[1:]
			parts := []remote.Partition{
				left, right,
			}
			glog.Infof("making parts: %+v", parts)
			r.remoteCtrl.MakePartition(parts)
			if rand.Int()%100 > 70 {
				time.Sleep(r.interval)
			} else {
				time.Sleep(3 * time.Second)
			}
		}
	}
}

func NewPartOneByOne(remoteCtrl *remote.RemoteController, raftCluster *raft.RaftCluster, interval time.Duration) *PartOneByOneCommand {
	cmd := &PartOneByOneCommand{
		remoteCtrl:  remoteCtrl,
		raftCluster: raftCluster,
		interval:    interval,
	}

	return cmd
}
