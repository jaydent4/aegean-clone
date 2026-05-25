# deep_2_unreplicated Service Topology

Source: [../architecture/deep_2_unreplicated.yaml](../architecture/deep_2_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  deep1["deep1<br/>unreplicated"]
  deep2["deep2<br/>unreplicated"]

  client --> deep1
  deep1 --> deep2
```
