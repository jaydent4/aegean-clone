# deep_2_aegean_eo Service Topology

Source: [../architecture/deep_2_aegean_eo.yaml](../architecture/deep_2_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>aegean_eo"]
  deep2["deep2<br/>aegean_eo"]

  client --> deep1
  deep1 --> deep2
```
