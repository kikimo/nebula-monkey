package gonebula

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"golang.org/x/time/rate"

	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type GQLEdgeStresser struct {
	space      string
	graphAddrs []string
	rateLimit  int
	clients    int
	batchSize  int
	loops      int
}

func NewGQLEdgeStresser(space string, gaddrs []string, rateLimit int, clients int, batchSize int, loops int) *GQLEdgeStresser {
	return &GQLEdgeStresser{
		space:      space,
		graphAddrs: gaddrs,
		rateLimit:  rateLimit,
		clients:    clients,
		batchSize:  batchSize,
		loops:      loops,
	}
}

func (s *GQLEdgeStresser) Run() error {
	// batchSize := gqlStressEdgeOpts.batchSize
	graphHosts := []nebula.HostAddress{}

	for _, gaddr := range s.graphAddrs {
		parts := strings.Split(gaddr, ":")
		if len(parts) != 2 {
			glog.Fatalf("illegal graph addr: %s", gaddr)
		}
		host := parts[0]
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			glog.Fatalf("illegal graph addr: %s, failed parsing graph port: %+v", gaddr, port)
		}

		graphHosts = append(graphHosts, nebula.HostAddress{
			Host: host,
			Port: port,
		})
	}

	poolConfig := nebula.GetDefaultConf()
	poolConfig.MaxConnPoolSize = 2048
	var log = nebula.DefaultLogger{}
	pool, err := nebula.NewConnectionPool(graphHosts, poolConfig, log)
	if err != nil {
		log.Fatal(fmt.Sprintf("err: %+v", err))
	}
	defer pool.Close()

	username, passwd := "root", "nebula"
	var wg sync.WaitGroup
	wg.Add(s.clients)

	limit := rate.Every(time.Microsecond * time.Duration(1000000/s.rateLimit))
	limiter := rate.NewLimiter(limit, 1024)
	ctx := context.TODO()

	for i := 0; i < s.clients; i++ {
		go func(clientID int) {
			session, err := pool.GetSession(username, passwd)
			if err != nil {
				log.Fatal(fmt.Sprintf("err: %+v", err))
			}
			defer session.Release()

			rs, err := session.Execute(fmt.Sprintf("use %s", s.space))
			if err != nil {
				log.Fatal(fmt.Sprintf("err: %+v", err))
			}
			log.Info(fmt.Sprintf("rs: %+v", rs))

			count := 0
			for j := 0; j < s.loops; j++ {
				gql := "insert edge known2(idx) values "
				// insert edge known2(idx) values 0->1:("kk");
				values := []string{}
				for k := 0; k < s.batchSize; k++ {
					count++
					v := fmt.Sprintf("%d->%d:('%d')", clientID, count, count)
					values = append(values, v)
				}
				gql = gql + strings.Join(values[:], ",")
				rs, err = session.Execute(gql)
				// fmt.Printf("gql: %s\n", gql)
				if err != nil {
					log.Fatal(fmt.Sprintf("%+v", err))
				}
				// log.Info(fmt.Sprintf("insert rs: %+v", rs))

				log.Info(fmt.Sprintf("client %d loop %d(total %d): %+v, errMsg: %s", clientID, j, s.loops, rs.IsSucceed(), rs.GetErrorMsg()))
				limiter.Wait(ctx)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	return nil
}
