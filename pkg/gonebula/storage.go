package gonebula

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

func NewStorageClient(addr string) (*storage.GraphStorageServiceClient, error) {
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
