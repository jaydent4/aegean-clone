What improved system performance
1. (Most important) Tune batcher per service: sometimes batcher timeout constitutes most of a request's latency
2. Use pprof and OpenTelemtry to find aggregate CPU/block times + break down latency per request
3. Keep exec's serial runCoordinator light
4. Convert HTTP to TCP: network cost is nontrivial especially for a replication protocol
5. Disabling OpenTelemtry/lots of logging during test time
6. Having middle service fan out to only the primary batcher of the backend service
7. If you have a mac, have it be charging
8. If request sizes are large, increase docker resources

To use distributed:
1. Updated distributed_nodes
2. python setup/ssh_config.py distributed
3. python setup/hosts.py distributed
4. python setup/install.py [# of nodes]
5. python setup/repo.py [# of nodes] [optionally --upload]

To use docker:
1. python setup/ssh_config.py docker
2. python setup/hosts.py docker

Improving PBEO: no full state copy
Improving Aegean: tuning batcher, optimize merkle