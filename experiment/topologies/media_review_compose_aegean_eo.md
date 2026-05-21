# media_review_compose_aegean_eo Service Topology

Source: [../architecture/media_review_compose_aegean_eo.yaml](../architecture/media_review_compose_aegean_eo.yaml)

```mermaid
flowchart LR
  client["client<br/>client"]
  review_compose_api["review_compose_api<br/>unreplicated"]
  user["user<br/>aegean"]
  movie_id["movie_id<br/>aegean"]
  text["text<br/>unreplicated"]
  unique_id["unique_id<br/>unreplicated"]
  rating["rating<br/>aegean"]
  compose_review["compose_review<br/>aegean"]
  review_storage["review_storage<br/>aegean"]
  user_review["user_review<br/>aegean"]
  movie_review["movie_review<br/>aegean"]

  client --> review_compose_api
  review_compose_api --> user
  review_compose_api --> movie_id
  review_compose_api --> text
  review_compose_api --> unique_id
  movie_id --> rating
  user --> compose_review
  movie_id --> compose_review
  text --> compose_review
  unique_id --> compose_review
  rating --> compose_review
  compose_review --> review_storage
  compose_review --> user_review
  compose_review --> movie_review
```
