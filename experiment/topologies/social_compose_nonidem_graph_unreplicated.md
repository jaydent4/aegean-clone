# social_compose_nonidem_graph_unreplicated Service Topology

Source: [../architecture/social_compose_unreplicated.yaml](../architecture/social_compose_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  compose_post["compose_post<br/>unreplicated"]
  post_storage["post_storage<br/>unreplicated"]
  user_timeline["user_timeline<br/>unreplicated"]
  home_timeline["home_timeline<br/>unreplicated"]
  social_graph["social_graph<br/>unreplicated non-idem"]

  client --> compose_post
  compose_post --> post_storage
  compose_post --> user_timeline
  compose_post --> home_timeline
  home_timeline --> social_graph
```
