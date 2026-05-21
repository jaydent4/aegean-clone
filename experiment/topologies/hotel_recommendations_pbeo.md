# hotel_recommendations_pbeo Service Topology

Source: [../architecture/hotel_recommendations_pbeo.yaml](../architecture/hotel_recommendations_pbeo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  frontend["frontend<br/>pbeo"]
  profile["profile<br/>pbeo"]
  recommendation_1["recommendation_1<br/>pbeo"]
  recommendation_2["recommendation_2<br/>pbeo"]
  recommendation_3["recommendation_3<br/>pbeo"]

  client --> frontend
  frontend --> recommendation_1
  frontend --> recommendation_2
  frontend --> recommendation_3
  frontend --> profile
```
