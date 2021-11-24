package raft

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/golang/glog"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/raftex"
)

const defaultRaftPort = 9780

type RaftInstance struct {
	host   string
	port   int
	client *raftex.RaftexServiceClient
}

func (r *RaftInstance) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// TODO make a singleton
type RaftCluster struct {
	hosts           map[string]*RaftInstance
	lock            sync.Mutex
	leader          string
	spaceID         nebula.GraphSpaceID
	partID          nebula.GraphSpaceID
	refreshInterval time.Duration
	lastTick        time.Time
}

func NewRaftCluster(spaceID nebula.GraphSpaceID, partID nebula.GraphSpaceID) *RaftCluster {
	return &RaftCluster{
		hosts:           make(map[string]*RaftInstance),
		spaceID:         spaceID,
		partID:          partID,
		refreshInterval: 100 * time.Millisecond,
	}
}

func (c *RaftCluster) Close() {
	for _, r := range c.hosts {
		r.client.Close()
	}
}

// TODO close me
// func (c *RaftCluster) refreshLeader() {
// 	go func() {
// 		for {
// 			time.Sleep(32 * time.Millisecond)
// 			c.doGetLeader()
// 		}
// 	}()
// }

func (c *RaftCluster) GetLeader() (string, error) {
	leader := c.leader
	if time.Since(c.lastTick) > c.refreshInterval {
		go func() {
			c.lock.Lock()
			defer c.lock.Unlock()

			if time.Since(c.lastTick) < c.refreshInterval {
				return
			}

			c.doGetLeader()
			c.lastTick = time.Now()
		}()
	}

	return leader, nil
}

func (c *RaftCluster) doGetLeader() {
	var leaderTerm int64 = 0

	for id, inst := range c.hosts {
		req := raftex.GetStateRequest{
			Space: int32(c.spaceID),
			Part:  int32(c.partID),
		}
		resp, err := inst.client.GetState(&req)
		if err != nil {
			glog.Errorf("error retrieving leader info from %s, err: %+v\n", id, err)
			if strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "Invalid data length") ||
				strings.Contains(err.Error(), "Not enough frame size") ||
				strings.Contains(err.Error(), "out of sequence response") ||
				strings.Contains(err.Error(), "Bad version in") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "EOF") {
				client, err := newRaftClient(inst.host, inst.port)
				if err == nil {
					inst.client = client
				} else {
					glog.Errorf("failed connecting to raft: %+v", err)
				}
			} else {
				glog.Fatalf("unknown error: %+v", err)
			}

			continue
		}

		if resp.IsLeader {
			glog.V(2).Infof("found leader of term: %d, leader: %s\n", resp.Term, id)
			if resp.Term > int64(leaderTerm) {
				glog.V(2).Infof("setting leader to: %s\n", id)
				c.leader = id
				leaderTerm = resp.Term
			}
		}
	}
}

func parseHost(h string) (host string, port int, err error) {
	i := strings.Index(h, ":")
	if i == -1 {
		host, port = h, defaultRaftPort
		return
	}

	host = h[:i]
	port, err = strconv.Atoi(h[i+1:])
	if err != nil {
		err = fmt.Errorf("error parsing raft host %s: %+v", h, err)
	}
	return
}

func (c *RaftCluster) RegisterHost(id string, host string) error {
	h, p, err := parseHost(host)
	if err != nil {
		return err
	}

	return c.RegisterHostWithPort(id, h, p)
}

func (c *RaftCluster) RegisterHostWithPort(id string, host string, port int) error {
	client, err := newRaftClient(host, port)
	if err != nil {
		return err
	}

	inst := &RaftInstance{
		host:   host,
		port:   port,
		client: client,
	}

	c.hosts[id] = inst
	return nil
}

func newRaftClient(host string, port int) (*raftex.RaftexServiceClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	timeout := thrift.SocketTimeout(4 * time.Second)
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		// return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
		return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(65536)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := raftex.NewRaftexServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		return nil, err
	}

	if !client.IsOpen() {
		return nil, fmt.Errorf("transport is off")
	}

	return client, nil
}
