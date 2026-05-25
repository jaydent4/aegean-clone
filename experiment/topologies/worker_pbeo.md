# worker_pbeo Service Topology

Source: [../architecture/worker_pbeo.yaml](../architecture/worker_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>pbeo"]
  backend["backend<br/>pbeo"]

  client --> middle
  middle --> backend
```
