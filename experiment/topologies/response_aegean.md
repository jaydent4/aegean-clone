# response_aegean Service Topology

Source: [../architecture/response_aegean.yaml](../architecture/response_aegean.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean"]
  backend["backend<br/>aegean"]

  client --> middle
  middle --> backend
```
