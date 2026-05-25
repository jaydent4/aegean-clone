# response_unreplicated Service Topology

Source: [../architecture/response_unreplicated.yaml](../architecture/response_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>unreplicated"]
  backend["backend<br/>unreplicated"]

  client --> middle
  middle --> backend
```
