# hotel_hotels_unreplicated Service Topology

Source: [../architecture/hotel_hotels_unreplicated.yaml](../architecture/hotel_hotels_unreplicated.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  frontend["frontend<br/>unreplicated"]
  search["search<br/>unreplicated"]
  geo["geo<br/>unreplicated"]
  rate["rate<br/>unreplicated"]
  reservation["reservation<br/>unreplicated"]
  profile["profile<br/>unreplicated"]
  recommendation["recommendation<br/>unreplicated"]
  user["user<br/>unreplicated"]

  client --> frontend
  frontend --> search
  search --> geo
  search --> rate
  frontend --> reservation
  frontend --> profile
  frontend --> recommendation
  frontend --> user
```
