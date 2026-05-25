# write_aegean_eo Service Topology

Source: [../architecture/write_aegean_eo.yaml](../architecture/write_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  middle["middle<br/>aegean_eo"]
  backend["backend<br/>aegean_eo"]

  client --> middle
  middle --> backend
```
