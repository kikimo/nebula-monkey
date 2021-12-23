package remote

func (c *RemoteController) StopProcess(host Host) error {
	return c.hosts[host].supervisorClient.StopProcess("storaged", true)
}

func (c *RemoteController) StartProcess(host Host) error {
	return c.hosts[host].supervisorClient.StartProcess("storaged", true)
}
