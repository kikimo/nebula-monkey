package gonebula

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/glog"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type Host struct {
	Host string
	Port int
}

// TODO move me to utils
func quoteS(vlist ...string) []string {
	ret := make([]string, len(vlist))
	for i, v := range vlist {
		ret[i] = fmt.Sprintf("`%s`", v)
	}

	return ret
}

func ExeGqlNoError(session *nebula.Session, stmt string) *nebula.ResultSet {
	rs, err := ExecGQL(session, stmt)
	if err != nil {
		glog.Fatalf("err gql: %s, err: %+v", stmt, err)
	}

	return rs
}

func GetHosts(session *nebula.Session) ([]Host, error) {
	hosts := []Host{}
	rs := ExeGqlNoError(session, "show hosts")
	for _, row := range rs.GetRows() {
		host := row.Values[0].SVal
		port := row.Values[1].IVal

		hosts = append(hosts, Host{
			Host: string(host),
			Port: int(*port),
		})
	}

	return hosts, nil
}

func ExecGQL(session *nebula.Session, stmt string) (*nebula.ResultSet, error) {
	rs, err := session.Execute(stmt)
	if err != nil {
		glog.V(1).Infof("failed executing stmt: %+v", err)
	}

	if rs != nil && !rs.IsSucceed() {
		glog.V(1).Infof("failed executing stmt: %s, err msg: %s", stmt, rs.GetErrorMsg())
	}

	if err != nil {
		return nil, err
	}

	if !rs.IsSucceed() {
		return nil, fmt.Errorf("error execute gql: %s", rs.GetErrorMsg())
	}

	return rs, nil
}

func GetZones(session *nebula.Session) ([]string, error) {
	zones := []string{}

	rs := ExeGqlNoError(session, "show zones")
	// colNames := rs.GetColNames()
	// glog.Infof("get col size: %d", rs.GetColSize())
	// for _, cn := range colNames {
	// 	glog.Infof("column name: %s", cn)
	// }
	for _, row := range rs.GetRows() {
		val := row.GetValues()
		zones = append(zones, string(val[0].SVal))
	}

	return quoteS(zones...), nil
}

type NebulaHost struct {
	Host               string
	Port               int
	Status             string
	LeaderCount        int
	LeaderDistribution string
	PartDistribution   string
}

func ShowHosts(session *nebula.Session) []NebulaHost {
	rt := ExeGqlNoError(session, "show hosts")
	if !rt.IsSucceed() {
		glog.Fatalf("error show host: %s", rt.GetErrorMsg())
	}

	hosts := []NebulaHost{}
	for _, row := range rt.GetRows() {
		h := NebulaHost{}
		cols := row.Values
		h.Host = string(cols[0].SVal)
		h.Port = int(*cols[1].IVal)
		h.Status = string(cols[3].SVal)
		h.LeaderCount = int(*cols[4].IVal)
		h.LeaderDistribution = string(cols[5].SVal)
		h.PartDistribution = string(cols[6].SVal)
		hosts = append(hosts, h)
	}

	return hosts
}

func CreateGraphConnPool(gaddrs []string, maxConsize int) (*nebula.ConnectionPool, error) {
	graphHosts := []nebula.HostAddress{}
	for _, gaddr := range gaddrs {
		parts := strings.Split(gaddr, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("illegal graph addr: %s", gaddr)
		}

		host := parts[0]
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("illegal graph addr: %s, failed parsing graph port: %+v", gaddr, port)
		}

		graphHosts = append(graphHosts, nebula.HostAddress{
			Host: host,
			Port: port,
		})
	}

	poolConfig := nebula.GetDefaultConf()
	poolConfig.MaxConnPoolSize = maxConsize
	var log = nebula.DefaultLogger{}
	pool, err := nebula.NewConnectionPool(graphHosts, poolConfig, log)
	if err != nil {
		return nil, fmt.Errorf("error creating graph conn pool: %+v", err)
	}

	return pool, nil
}
