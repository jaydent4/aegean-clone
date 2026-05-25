# deep_4_pbeo Service Topology

Source: [../architecture/deep_4_pbeo.yaml](../architecture/deep_4_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>pbeo"]
  deep2["deep2<br/>pbeo"]
  deep3["deep3<br/>pbeo"]
  deep4["deep4<br/>pbeo"]

  client --> deep1
  deep1 --> deep2
  deep2 --> deep3
  deep3 --> deep4
```
