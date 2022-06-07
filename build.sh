#!/bin/bash

GOOS=linux GOARCH=amd64 go build 

# scp nebula-monkey k8s:/home/vesoft/
# scp nebula-monkey jepsen:/root/src 

# scp nebula-monkey jepsen:/root/nebula-cluster/bin
# scp nebula-monkey jepsen:/data/bug-raft-nebula/bin/
# scp nebula-monkey jepsen:/data/nebula-master/bin/
# scp nebula-monkey jepsen:/data/balance-fix-nebula/bin/
# scp nebula-monkey jepsen:/data/bug-toss-nebula/bin/
# scp nebula-monkey jepsen:/data/bug-raft-nebula/bin/
# scp nebula-monkey jepsen:/data/bug-toss-nebula/bin/
scp nebula-monkey jepsen:/var/www/html/softs/
scp nebula-monkey jepsen:/mnt/nfs_share/chaos-test-cases/nebula-chaos-cluster/bin
# scp nebula-monkey jepsen:/mnt/nfs_share/chaos-test-cases/bin/
# scp nebula-monkey jepsen:/data/raft-loop/bin/
# scp nebula-monkey jepsen:/data/another-balance/bin/
# scp nebula-monkey jepsen:/data/nebula-test/bin/
# scp nebula-monkey k8s:/data/nebula-threadlocal-crash/bin/
# scp nebula-monkey vesoft@k8s:/root/nebula-chaos-cluster/bin/nebula-monkey

