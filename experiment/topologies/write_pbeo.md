# write_pbeo Service Topology

Source: [../architecture/write_pbeo.yaml](../architecture/write_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>pbeo"]
  backend["backend<br/>pbeo"]

  client --> middle
  middle --> backend
```
