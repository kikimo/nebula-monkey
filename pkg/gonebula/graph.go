package gonebula

import (
	"fmt"

	"github.com/golang/glog"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type Host struct {
	host string
	port int
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
		glog.Fatalf("err gql: %+v", err)
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
			host: string(host),
			port: int(*port),
		})
	}

	return hosts, nil
}

func ExecGQL(session *nebula.Session, stmt string) (*nebula.ResultSet, error) {
	rs, err := session.Execute(stmt)
	if err != nil {
		glog.Errorf("failed executing stmt: %+v", err)
	}

	if rs != nil && !rs.IsSucceed() {
		glog.Errorf("failed executing stmt, err msg: %s", rs.GetErrorMsg())
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
