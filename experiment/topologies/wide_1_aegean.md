# wide_1_aegean Service Topology

Source: [../architecture/wide_1_aegean.yaml](../architecture/wide_1_aegean.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean"]
  backend1["backend1<br/>aegean"]

  client --> middle
  middle --> backend1
```
