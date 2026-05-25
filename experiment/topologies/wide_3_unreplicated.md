# wide_3_unreplicated Service Topology

Source: [../architecture/wide_3_unreplicated.yaml](../architecture/wide_3_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>unreplicated"]
  backend1["backend1<br/>unreplicated"]
  backend2["backend2<br/>unreplicated"]
  backend3["backend3<br/>unreplicated"]

  client --> middle
  middle --> backend1
  middle --> backend2
  middle --> backend3
```
