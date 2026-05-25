# wide_1_pbeo Service Topology

Source: [../architecture/wide_1_pbeo.yaml](../architecture/wide_1_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>pbeo"]
  backend1["backend1<br/>pbeo"]

  client --> middle
  middle --> backend1
```
