# deep_4_aegean_eo Service Topology

Source: [../architecture/deep_4_aegean_eo.yaml](../architecture/deep_4_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>aegean_eo"]
  deep2["deep2<br/>aegean_eo"]
  deep3["deep3<br/>aegean_eo"]
  deep4["deep4<br/>aegean_eo"]

  client --> deep1
  deep1 --> deep2
  deep2 --> deep3
  deep3 --> deep4
```
