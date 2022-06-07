package gonebula

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// import (
// 	"fmt"
// 	"math"
// 	"time"

// 	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
// 	"github.com/vesoft-inc/nebula-go/v2/nebula"
// )

type MetaOption struct {
	Timeout    time.Duration
	BufferSize int
}

// type HostAddrX struct {
// 	host       *nebula.HostAddr
// 	ts         time.Time
// 	metaClient *MetaClient
// 	currLeader int
// 	peers      []*nebula.HostAddr
// }

// type MetaClient struct {
// 	*meta.MetaServiceClient
// 	lock            Spinlock
// 	spacePartLeader map[nebula.GraphSpaceID]map[nebula.PartitionID]*HostAddrX
// }

func NewMetaClient(addr string, opt MetaOption) (*meta.MetaServiceClient, error) {
	timeout := thrift.SocketTimeout(opt.Timeout)
	// bufferSize := 128 << 10
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create a net.Conn-backed Transport,: %s", err.Error())
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(opt.BufferSize)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()
	metaClient := meta.NewMetaServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := metaClient.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %s", err.Error())
	}

	if !metaClient.IsOpen() {
		return nil, fmt.Errorf("transport is off")
	}

	/*
		lhReq := &meta.ListHostsReq{
			Type: meta.ListHostType_STORAGE,
		}

		lhResp, err := metaClient.ListHosts(lhReq)
		if err != nil {
			panic(err)
		}
		hosts := lhResp.GetHosts()
		hi := hosts[0]
		hi.GetHostAddr()

		spcReq := &meta.GetSpaceReq{}
		spcResp, err := metaClient.GetSpace(spcReq)
		if err != nil {
			panic(err)
		}

		si := spcResp.GetItem()
		si.GetSpaceID()
		sdesc := si.GetProperties()
		pnum := sdesc.PartitionNum
	*/

	return metaClient, nil
}

func GetSpaceByName(mclient *meta.MetaServiceClient, spaceName string) (*meta.GetSpaceResp, error) {
	req := meta.GetSpaceReq{
		SpaceName: []byte(spaceName),
	}
	resp, err := mclient.GetSpace(&req)
	if err != nil {
		return nil, fmt.Errorf("error getting space id, nested error: %+v", err)
	}

	return resp, nil
}

func GetTagID(mclient *meta.MetaServiceClient, spaceID nebula.GraphSpaceID, tagName string) (int32, error) {
	listTagReq := &meta.ListTagsReq{
		SpaceID: spaceID,
	}
	listTagResp, err := mclient.ListTags(listTagReq)
	if err != nil {
		return 0, err
	}

	for _, ti := range listTagResp.Tags {
		if string(ti.TagName) == tagName {
			return ti.TagID, nil
		}
	}

	return 0, errors.New("not found")
}

func GetEdgeItem(mclient *meta.MetaServiceClient, spaceID nebula.GraphSpaceID, edgeName string) (*meta.EdgeItem, error) {
	listEegesReq := &meta.ListEdgesReq{
		SpaceID: spaceID,
	}
	listEdgeResp, err := mclient.ListEdges(listEegesReq)
	if err != nil {
		return nil, fmt.Errorf("error list space edge: %+v", err)
	}

	for _, er := range listEdgeResp.Edges {
		if string(er.EdgeName) == edgeName {
			return er, nil
		}
	}

	return nil, fmt.Errorf("edge %s not found", edgeName)
}

// func (m *MetaClient) GetSpacePartLeader(spaceID nebula.GraphSpaceID, partID nebula.PartitionID) (*nebula.HostAddr, error) {
// 	m.lock.Lock()
// 	defer m.lock.Unlock()
// 	if _, ok := m.spacePartLeader[spaceID]; !ok {
// 		m.spacePartLeader[spaceID] = map[nebula.PartitionID]*HostAddrX{}
// 	}

// 	partLeaderMap := m.spacePartLeader[spaceID]
// 	var hostx *HostAddrX
// 	var err error
// 	if _, ok := partLeaderMap[partID]; !ok {
// 		// partLeaderMap[partID] =
// 		hostx, err = m.doGetSpacePartLeader(spaceID, partID)
// 		if err != nil {
// 			return nil, err
// 		}

// 		partLeaderMap[partID] = hostx
// 	} else {
// 		hostx = partLeaderMap[partID]
// 		hostx.currLeader = (hostx.currLeader + 1) % len(hostx.peers)
// 		hostx.host = hostx.peers[hostx.currLeader]
// 	}

// 	hostx = partLeaderMap[partID]
// 	return hostx.host, nil
// }

// func (m *MetaClient) doGetSpacePartLeader(spaceID nebula.GraphSpaceID, partID nebula.PartitionID) (*HostAddrX, error) {
// 	lpReq := meta.ListPartsReq{
// 		SpaceID: spaceID,
// 		PartIds: []nebula.PartitionID{partID},
// 	}

// 	partResp, err := m.ListParts(&lpReq)
// 	if err != nil {
// 		return nil, err
// 	}

// 	part := partResp.Parts[0]
// 	peers := part.GetPeers()
// 	// fmt.Printf("parts of part %d at space %d is: %+v and the leader %+v", partID, spaceID, part, part.GetLeader())
// 	leader := part.GetLeader()
// 	hostx := &HostAddrX{
// 		host:       leader,
// 		peers:      peers,
// 		ts:         time.Now(),
// 		metaClient: m,
// 	}

// 	for idx, h := range peers {
// 		if h.GetPort() == leader.Port && h.GetHost() == leader.Host {
// 			hostx.currLeader = idx
// 		}
// 	}

// 	return hostx, nil
// }
