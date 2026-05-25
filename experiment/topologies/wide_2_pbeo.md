# wide_2_pbeo Service Topology

Source: [../architecture/wide_2_pbeo.yaml](../architecture/wide_2_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>pbeo"]
  backend1["backend1<br/>pbeo"]
  backend2["backend2<br/>pbeo"]

  client --> middle
  middle --> backend1
  middle --> backend2
```
