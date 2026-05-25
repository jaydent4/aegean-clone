# social_compose_nonidem_graph_pbeo Service Topology

Source: [../architecture/social_compose_nonidem_graph_pbeo.yaml](../architecture/social_compose_nonidem_graph_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  compose_post["compose_post<br/>unreplicated"]
  post_storage["post_storage<br/>pbeo"]
  user_timeline["user_timeline<br/>pbeo"]
  home_timeline["home_timeline<br/>pbeo"]
  social_graph["social_graph<br/>unreplicated non-idem"]

  client --> compose_post
  compose_post --> post_storage
  compose_post --> user_timeline
  compose_post --> home_timeline
  home_timeline --> social_graph
```
