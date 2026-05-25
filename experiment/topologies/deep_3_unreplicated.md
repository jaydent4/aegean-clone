# deep_3_unreplicated Service Topology

Source: [../architecture/deep_3_unreplicated.yaml](../architecture/deep_3_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>unreplicated"]
  deep2["deep2<br/>unreplicated"]
  deep3["deep3<br/>unreplicated"]

  client --> deep1
  deep1 --> deep2
  deep2 --> deep3
```
