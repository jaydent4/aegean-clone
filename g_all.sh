#!/usr/bin/env bash

python gather.py experiment/runs/hotel_hotels_aegean/ --boundary-qps 4207
python gather.py experiment/runs/hotel_hotels_aegean_eo/ --boundary-qps 3990
python gather.py experiment/runs/hotel_hotels_pbeo/ --boundary-qps 6975
python gather.py experiment/runs/hotel_hotels_unreplicated/ --boundary-qps 7942

python gather.py experiment/runs/hotel_recommendations_aegean/ --boundary-qps 1000
python gather.py experiment/runs/hotel_recommendations_aegean_eo/ --boundary-qps 1000
python gather.py experiment/runs/hotel_recommendations_pbeo/ --boundary-qps 1000
python gather.py experiment/runs/hotel_recommendations_unreplicated/ --boundary-qps 1000

python gather.py experiment/runs/media_review_compose_aegean/ --boundary-qps 1000
python gather.py experiment/runs/media_review_compose_aegean_eo/ --boundary-qps 1000
python gather.py experiment/runs/media_review_compose_pbeo/ --boundary-qps 1000
python gather.py experiment/runs/media_review_compose_unreplicated/ --boundary-qps 1000

python gather.py experiment/runs/social_compose_aegean/ --boundary-qps 1000
python gather.py experiment/runs/social_compose_aegean_eo/ --boundary-qps 1000
python gather.py experiment/runs/social_compose_pbeo/ --boundary-qps 1000
python gather.py experiment/runs/social_compose_unreplicated/ --boundary-qps 1000