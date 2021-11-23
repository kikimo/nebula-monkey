package raft

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/raftex"
)

const defaultRaftPort = 9780

type RaftInstance struct {
	host   string
	port   int
	client *raftex.RaftexServiceClient
}

type RaftCluster struct {
	hosts map[string]*RaftInstance
}

func NewRaftCluster() *RaftCluster {
	return &RaftCluster{
		hosts: make(map[string]*RaftInstance),
	}
}

func (c *RaftCluster) GetLeader(spaceID int, partID int) (string, error) {
	leaderId := ""
	var leaderTerm int64 = 0

	for id, inst := range c.hosts {
		req := raftex.GetStateRequest{
			Space: int32(spaceID),
			Part:  int32(partID),
		}
		resp, err := inst.client.GetState(&req)
		if err != nil {
			fmt.Printf("error retrieving leader info from %s, err: %+v\n", id, err)
		}

		if resp.IsLeader {
			fmt.Printf("found leader of term: %d, leader: %s\n", resp.Term, id)
			if resp.Term > int64(leaderTerm) {
				fmt.Printf("setting leader to: %s\n", id)
				leaderId = id
				leaderTerm = resp.Term
			}
		}
	}

	if leaderId != "" {
		return leaderId, nil
	}

	return leaderId, fmt.Errorf("leader not found")
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
	client, err := NewRaftClient(host, port)
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

func NewRaftClient(host string, port int) (*raftex.RaftexServiceClient, error) {
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
