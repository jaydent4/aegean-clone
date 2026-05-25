# write_unreplicated Service Topology

Source: [../architecture/write_unreplicated.yaml](../architecture/write_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>unreplicated"]
  backend["backend<br/>unreplicated"]

  client --> middle
  middle --> backend
```
