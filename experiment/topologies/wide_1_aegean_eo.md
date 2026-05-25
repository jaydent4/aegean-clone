# wide_1_aegean_eo Service Topology

Source: [../architecture/wide_1_aegean_eo.yaml](../architecture/wide_1_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean_eo"]
  backend1["backend1<br/>aegean_eo"]

  client --> middle
  middle --> backend1
```
