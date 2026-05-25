# deep_2_pbeo Service Topology

Source: [../architecture/deep_2_pbeo.yaml](../architecture/deep_2_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>pbeo"]
  deep2["deep2<br/>pbeo"]

  client --> deep1
  deep1 --> deep2
```
