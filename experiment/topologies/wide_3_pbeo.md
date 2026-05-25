# wide_3_pbeo Service Topology

Source: [../architecture/wide_3_pbeo.yaml](../architecture/wide_3_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>pbeo"]
  backend1["backend1<br/>pbeo"]
  backend2["backend2<br/>pbeo"]
  backend3["backend3<br/>pbeo"]

  client --> middle
  middle --> backend1
  middle --> backend2
  middle --> backend3
```
