# hotel_hotels_aegean Service Topology

Source: [../architecture/hotel_hotels_aegean.yaml](../architecture/hotel_hotels_aegean.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  frontend["frontend<br/>unreplicated"]
  search["search<br/>aegean"]
  geo["geo<br/>aegean"]
  rate["rate<br/>aegean"]
  reservation["reservation<br/>aegean"]
  profile["profile<br/>aegean"]
  recommendation["recommendation<br/>aegean"]
  user["user<br/>aegean"]

  client --> frontend
  frontend --> search
  search --> geo
  search --> rate
  frontend --> reservation
  frontend --> profile
  frontend --> recommendation
  frontend --> user
```
