#!/bin/bash

GOOS=linux GOARCH=amd64 go build 

# scp nebula-monkey k8s:/home/vesoft/
# scp nebula-monkey jepsen:/root/src 

# scp nebula-monkey jepsen:/root/nebula-cluster/bin
scp nebula-monkey jepsen:/root/nebula-chaos-cluster-wwl/bin
scp nebula-monkey vesoft@k8s:/root/nebula-chaos-cluster/bin/nebula-monkey

