# hotel_recommendations_aegean_eo Service Topology

Source: [../architecture/hotel_recommendations_aegean_eo.yaml](../architecture/hotel_recommendations_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  frontend["frontend<br/>aegean"]
  profile["profile<br/>aegean"]
  recommendation_1["recommendation_1<br/>aegean"]
  recommendation_2["recommendation_2<br/>aegean"]
  recommendation_3["recommendation_3<br/>aegean"]

  client --> frontend
  frontend --> recommendation_1
  frontend --> recommendation_2
  frontend --> recommendation_3
  frontend --> profile
```
