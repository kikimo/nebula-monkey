package remote

import (
	"fmt"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/kikimo/goremote"
)

type Host string
type Partition []Host

type RemoteController struct {
	hosts map[Host]*goremote.SSHClient
}

func (c *RemoteController) GetHosts() []Host {
	hosts := []Host{}
	for h := range c.hosts {
		hosts = append(hosts, h)
	}

	return hosts
}

func (c *RemoteController) RejoinHost(host Host) error {
	errStr := ""

	for h := range c.hosts {
		if h == host {
			continue
		}

		if err := c.Connect(host, h); err != nil {
			errStr += "," + err.Error()
		}
	}

	if errStr == "" {
		return nil
	}

	return fmt.Errorf("error rejoining host %s: %+v", host, errStr)
}

func (c *RemoteController) IsolateHost(host Host) error {
	errStr := ""

	for h := range c.hosts {
		if h == host {
			continue
		}

		if err := c.Disconnect(host, h); err != nil {
			errStr += "," + err.Error()
		}
	}

	if errStr == "" {
		return nil
	}

	return fmt.Errorf("error isolating host %s: %+v", host, errStr)
}

// FIXME: no bool in return
func (n *RemoteController) CheckRuleExist(host Host, ruleCheckCmd string) (bool, error) {
	ret, err := n.hosts[host].Run(ruleCheckCmd)
	if strings.Contains(ret.Stderr, "iptables: Bad rule (does a matching rule exist in that chain?).") {
		return false, nil
	}

	if err == nil && ret.Err == nil {
		return true, nil
	}

	glog.V(2).Infof("error checking rule: %s, ret: %+v, err: %+v\n", ruleCheckCmd, ret, err)
	if err == nil {
		err = ret.Err
	}

	return false, err
}

func (n *RemoteController) Run(host Host, cmd string) (bool, error) {
	ret, err := n.hosts[host].Run(cmd)
	glog.V(2).Infof("ret: %+v, err: %+v\n", ret, err)
	if strings.Contains(ret.Stderr, "iptables: Bad rule (does a matching rule exist in that chain?).") {
		glog.V(2).Info("fuck\n")
	} else {
		glog.V(2).Info("shit\n")
	}
	glog.V(2).Infof("%s\n", ret.Stderr)
	return false, nil
}

// from host a, connect host b
func (n *RemoteController) doConnect(a, b Host) error {
	for {
		glog.V(2).Infof("connecting %s, %s\n", a, b)
		checkCmd := fmt.Sprintf("iptables -C INPUT -W 1000 -w 4 -s %s -j DROP", b)
		exists, err := n.CheckRuleExist(a, checkCmd)
		glog.V(2).Infof("check rule err: %+v, exists: %+v\n", err, exists)
		if err != nil {
			return err
		}

		if !exists {
			break
		}

		unblockCmd := fmt.Sprintf("iptables -D INPUT -W 1000 -w 4 -s %s -j DROP", b)
		ret, err := n.hosts[a].Run(unblockCmd)
		glog.V(2).Infof("unblocking %s, %s: %s, ret: %+v, err: %+v\n", a, b, unblockCmd, ret, err)
		if err != nil {
			return err
		}

		if ret.Err != nil {
			return ret.Err
		}
	}

	return nil
}

// from host a, disconnect host b
func (n *RemoteController) doDisconnect(a, b Host) error {
	checkCmd := fmt.Sprintf("iptables -C INPUT -W 1000 -w 4 -s %s -j DROP", b)
	exists, err := n.CheckRuleExist(a, checkCmd)
	glog.V(2).Infof("check exist: %+v, err: %+v\n", exists, err)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	blockCmd := fmt.Sprintf("iptables -A INPUT -W 1000 -w 4 -s %s -j DROP", b)
	ret, err := n.hosts[a].Run(blockCmd)
	glog.V(2).Infof("block ret: %+v, err: %+v\n", ret, err)
	if err != nil {
		return err
	}

	if ret.Err != nil {
		return ret.Err
	}

	return nil
}

func (n *RemoteController) RegisterHost(host Host, sshClient *goremote.SSHClient) error {
	if _, ok := n.hosts[host]; ok {
		return fmt.Errorf("host %s registered already", host)
	}

	if sshClient == nil {
		return fmt.Errorf("ssh client cannot be nil")
	}

	n.hosts[host] = sshClient
	return nil
}

func (n *RemoteController) Disconnect(a, b Host) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		if err := n.doDisconnect(a, b); err != nil {
			// return err
		}

		wg.Done()
	}()

	go func() {
		if err := n.doDisconnect(b, a); err != nil {
			// return err
		}

		wg.Done()
	}()

	wg.Wait()
	return nil
}

func (n *RemoteController) Connect(a, b Host) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		if err := n.doConnect(a, b); err != nil {
			// return err
		}

		wg.Done()
	}()

	go func() {
		if err := n.doConnect(b, a); err != nil {
			// return err
		}

		wg.Done()
	}()

	wg.Wait()
	return nil
}

func (n *RemoteController) ConnectPartition(part Partition) error {
	var wg sync.WaitGroup

	for i := 0; i < len(part); i++ {
		for j := i + 1; j < len(part); j++ {
			wg.Add(1)
			go func(a, b Host) {
				n.Connect(a, b)
				wg.Done()
			}(part[i], part[j])
		}
	}

	wg.Wait()
	return nil
}

func (n *RemoteController) disconnectTwoPartition(onePart, anotherPart Partition) error {
	var wg sync.WaitGroup

	for i := 0; i < len(onePart); i++ {
		for j := 0; j < len(anotherPart); j++ {
			wg.Add(1)
			go func(a, b Host) {
				n.Disconnect(a, b)
				wg.Done()
			}(onePart[i], anotherPart[j])
		}
	}

	wg.Wait()
	return nil
}

func (n *RemoteController) DisconnectPartitions(parts []Partition) error {
	var wg sync.WaitGroup
	// TODO handle error
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			wg.Add(1)
			go func(a, b Partition) {
				n.disconnectTwoPartition(a, b)
				wg.Done()
			}(parts[i], parts[j])
		}
	}
	wg.Wait()

	return nil
}

func (n *RemoteController) MakePartition(parts []Partition) error {
	var wg sync.WaitGroup
	for i := range parts {
		wg.Add(1)
		go func(part Partition) {
			n.ConnectPartition(part)
			wg.Done()
		}(parts[i])
	}

	wg.Add(1)
	go func() {
		n.DisconnectPartitions(parts)
		wg.Done()
	}()
	wg.Wait()
	return nil
}

func (n *RemoteController) HealAll() error {
	hosts := make([]Host, 0, len(n.hosts))
	for h := range n.hosts {
		hosts = append(hosts, h)
	}

	var wg sync.WaitGroup
	for i := 0; i < len(hosts); i++ {
		for j := i + 1; j < len(hosts); j++ {
			wg.Add(1)
			go func(x, y int) {
				n.Connect(hosts[x], hosts[y])
				wg.Done()
			}(i, j)
		}
	}

	wg.Wait()
	return nil
}

func (n *RemoteController) Close() {
	for _, c := range n.hosts {
		if c != nil {
			c.Close()
		}
	}
}

func NewRemoteController() *RemoteController {
	return &RemoteController{
		hosts: map[Host]*goremote.SSHClient{},
	}
}
