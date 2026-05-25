# response_pbeo Service Topology

Source: [../architecture/response_pbeo.yaml](../architecture/response_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>pbeo"]
  backend["backend<br/>pbeo"]

  client --> middle
  middle --> backend
```
