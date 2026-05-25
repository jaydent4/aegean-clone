# wide_2_aegean_eo Service Topology

Source: [../architecture/wide_2_aegean_eo.yaml](../architecture/wide_2_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean_eo"]
  backend1["backend1<br/>aegean_eo"]
  backend2["backend2<br/>aegean_eo"]

  client --> middle
  middle --> backend1
  middle --> backend2
```
