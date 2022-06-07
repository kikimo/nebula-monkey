package gonebula

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/pkg/raft"
	"github.com/vesoft-inc/nebula-go/v3/nebula/storage"
)

func NewNebulaConn(addr string) (*storage.GraphStorageServiceClient, error) {
	timeout := thrift.SocketTimeout(4 * time.Second)
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(65536)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %+v", err)
	}

	if !client.IsOpen() {
		panic("transport is off")
	}

	return client, nil
}

type NebulaClient interface {
	ResetConn() error
	GetClient() *storage.GraphStorageServiceClient
}

type defaultNebulaClient struct {
	id      int
	addr    string
	client  *storage.GraphStorageServiceClient
	cluster *raft.RaftCluster
}

func (c *defaultNebulaClient) GetClient() *storage.GraphStorageServiceClient {
	return c.client
}

type FixedTargetNebulaCleint struct {
	addr   string
	client *storage.GraphStorageServiceClient
}

func (c *FixedTargetNebulaCleint) ResetConn() error {
	if c.client != nil && c.client.IsOpen() {
		c.client.Close()
	}

	// c.client, err := gonebula.NewStorageClient(c.addr)
	glog.Infof("storage addr: %+s", c.addr)
	client, err := NewStorageClient(c.addr)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *FixedTargetNebulaCleint) GetClient() *storage.GraphStorageServiceClient {
	return c.client
}

func NewFixedTargetNebulaClient(addr string) NebulaClient {
	return &FixedTargetNebulaCleint{
		addr: addr,
	}
}

func NewDefaultNebulaClient(id int, cluster *raft.RaftCluster) NebulaClient {
	c := defaultNebulaClient{
		id:      id,
		cluster: cluster,
		addr:    "",
	}

	return &c
}

func (c *defaultNebulaClient) ResetConn() error {
	host, err := c.cluster.GetLeader()
	// TODO: check is leader right?
	if err != nil {
		return err
	}

	glog.V(2).Infof("connecting to host %s", host)

	// TODO specify port
	c.addr = fmt.Sprintf("%s:%d", host, 9779)
	client, err := NewNebulaConn(c.addr)
	if err != nil {
		return err
	}

	c.client = client
	return nil
}
