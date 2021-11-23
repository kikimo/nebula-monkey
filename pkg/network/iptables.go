package network

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kikimo/goremote"
)

type Host string
type Partition []Host

type NetworkManager struct {
	hosts map[Host]*goremote.SSHClient
}

func (n *NetworkManager) CheckRuleExist(host Host, ruleCheckCmd string) (bool, error) {
	ret, err := n.hosts[host].Run(ruleCheckCmd)
	if strings.Contains(ret.Stderr, "iptables: Bad rule (does a matching rule exist in that chain?).") {
		return false, nil
	}

	if err == nil && ret.Err == nil {
		return true, nil
	}

	fmt.Printf("error checking rule: %s, ret: %+v, err: %+v\n", ruleCheckCmd, ret, err)
	if err == nil {
		err = ret.Err
	}

	return false, err
}

func (n *NetworkManager) Run(host Host, cmd string) (bool, error) {
	ret, err := n.hosts[host].Run(cmd)
	fmt.Printf("ret: %+v, err: %+v\n", ret, err)
	if strings.Contains(ret.Stderr, "iptables: Bad rule (does a matching rule exist in that chain?).") {
		fmt.Printf("fuck\n")
	} else {
		fmt.Printf("shit\n")
	}
	fmt.Printf("%s\n", ret.Stderr)
	return false, nil
}

// from host a, connect host b
func (n *NetworkManager) doConnect(a, b Host) error {
	for {
		fmt.Printf("connecting %s, %s\n", a, b)
		checkCmd := fmt.Sprintf("iptables -C INPUT -W 1000 -w 4 -s %s -j DROP", b)
		exists, err := n.CheckRuleExist(a, checkCmd)
		fmt.Printf("check rule err: %+v, exists: %+v\n", err, exists)
		if err != nil {
			return err
		}

		if !exists {
			break
		}

		unblockCmd := fmt.Sprintf("iptables -D INPUT -W 1000 -w 4 -s %s -j DROP", b)
		ret, err := n.hosts[a].Run(unblockCmd)
		fmt.Printf("unblocking %s, %s: %s, ret: %+v, err: %+v\n", a, b, unblockCmd, ret, err)
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
func (n *NetworkManager) doDisconnect(a, b Host) error {
	checkCmd := fmt.Sprintf("iptables -C INPUT -W 1000 -w 4 -s %s -j DROP", b)
	exists, err := n.CheckRuleExist(a, checkCmd)
	fmt.Printf("check exist: %+v, err: %+v\n", exists, err)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	blockCmd := fmt.Sprintf("iptables -A INPUT -W 1000 -w 4 -s %s -j DROP", b)
	ret, err := n.hosts[a].Run(blockCmd)
	fmt.Printf("block ret: %+v, err: %+v\n", ret, err)
	if err != nil {
		return err
	}

	if ret.Err != nil {
		return ret.Err
	}

	return nil
}

func (n *NetworkManager) RegisterHost(host Host, sshClient *goremote.SSHClient) error {
	if _, ok := n.hosts[host]; ok {
		return fmt.Errorf("host %s registered already", host)
	}

	if sshClient == nil {
		return fmt.Errorf("ssh client cannot be nil")
	}

	n.hosts[host] = sshClient
	return nil
}

func (n *NetworkManager) Disconnect(a, b Host) error {
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

func (n *NetworkManager) Connect(a, b Host) error {
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

func (n *NetworkManager) ConnectPartition(part Partition) error {
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

func (n *NetworkManager) disconnectTwoPartition(onePart, anotherPart Partition) error {
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

func (n *NetworkManager) DisconnectPartitions(parts []Partition) error {
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

func (n *NetworkManager) MakePartition(parts []Partition) error {
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

func (n *NetworkManager) HealAll() error {
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

func (n *NetworkManager) Close() {
	for _, c := range n.hosts {
		if c != nil {
			c.Close()
		}
	}
}

func NewNetworkManager() *NetworkManager {
	return &NetworkManager{
		hosts: map[Host]*goremote.SSHClient{},
	}
}
