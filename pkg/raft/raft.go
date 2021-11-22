package raft

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/raftex"
)

const defaultRaftPort = 9780

type RaftCluster struct {
	hosts       map[string]*raftex.RaftexServiceClient
	hostPortMap map[string]int
}

func NewRaftCluster() *RaftCluster {
	return &RaftCluster{
		hosts:       make(map[string]*raftex.RaftexServiceClient),
		hostPortMap: make(map[string]int),
	}
}

func (c *RaftCluster) GetLeader(spaceID int, partID int) (string, int, error) {
	leader := ""
	port := 0
	err := fmt.Errorf("leader not found")
	leaderTerm := 0

	for h, client := range c.hosts {
		req := raftex.GetStateRequest{
			Space: int32(spaceID),
			Part:  int32(partID),
		}
		resp, err := client.GetState(&req)
		if err != nil {
			fmt.Printf("error retrieving leader info from %s, port: %d, err: %+v\n", h, c.hostPortMap[h], err)
		}

		if resp.IsLeader {
			fmt.Printf("found leader of term: %d, leader: %s, port: %d\n", resp.Term, h, c.hostPortMap[h])
			if resp.Term > int64(leaderTerm) {
				leader = h
				port = c.hostPortMap[h]
				leaderTerm = int(resp.Term)
			}
		}
	}

	return leader, port, err
}

func (c *RaftCluster) RegisterHost(host string) error {
	return c.RegisterHostWithPort(host, defaultRaftPort)
}

func (c *RaftCluster) RegisterHostWithPort(host string, port int) error {
	client, err := NewRaftClient(host, port)
	if err != nil {
		return err
	}

	c.hostPortMap[host] = port
	c.hosts[host] = client
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
