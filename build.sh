#!/bin/bash

GOOS=linux GOARCH=amd64 go build && \
	scp nebula-monkey jepsen:/root/src && \
	scp nebula-monkey jepsen:/root/nebula-cluster/bin

