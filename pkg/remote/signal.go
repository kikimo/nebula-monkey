package remote

import "fmt"

func (n *RemoteController) SuspendStorage(host Host) (bool, error) {
	h, ok := n.hosts[host]
	if !ok {
		return false, fmt.Errorf("host %s not found", host)
	}

	cmd := "kill -s SIGSTOP $(pgrep nebula-storaged)"
	ret, err := h.Run(cmd)
	if err != nil {
		return false, fmt.Errorf("error suspending storage: %+v", err)
	}

	if ret.Err != nil {
		return false, fmt.Errorf("error suspending storage: %+v", ret.Err)
	}

	return true, nil
}

func (n *RemoteController) ResumeStorage(host Host) (bool, error) {
	h, ok := n.hosts[host]
	if !ok {
		return false, fmt.Errorf("host %s not found", host)
	}

	cmd := "kill -s SIGCONT $(pgrep nebula-storaged)"
	ret, err := h.Run(cmd)
	if err != nil {
		return false, fmt.Errorf("error resuming storage: %+v", err)
	}

	if ret.Err != nil {
		return false, fmt.Errorf("error resuming storage: %+v", ret.Err)
	}

	return true, nil

}
