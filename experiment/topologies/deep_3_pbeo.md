# deep_3_pbeo Service Topology

Source: [../architecture/deep_3_pbeo.yaml](../architecture/deep_3_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>pbeo"]
  deep2["deep2<br/>pbeo"]
  deep3["deep3<br/>pbeo"]

  client --> deep1
  deep1 --> deep2
  deep2 --> deep3
```
