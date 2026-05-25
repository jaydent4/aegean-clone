# wide_2_aegean Service Topology

Source: [../architecture/wide_2_aegean.yaml](../architecture/wide_2_aegean.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean"]
  backend1["backend1<br/>aegean"]
  backend2["backend2<br/>aegean"]

  client --> middle
  middle --> backend1
  middle --> backend2
```
