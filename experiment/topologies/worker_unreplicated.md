# worker_unreplicated Service Topology

Source: [../architecture/worker_unreplicated.yaml](../architecture/worker_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>unreplicated"]
  backend["backend<br/>unreplicated"]

  client --> middle
  middle --> backend
```
