# worker_aegean Service Topology

Source: [../architecture/worker_aegean.yaml](../architecture/worker_aegean.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean"]
  backend["backend<br/>aegean"]

  client --> middle
  middle --> backend
```
