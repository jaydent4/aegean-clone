#!/usr/bin/env bash

python gather.py experiment/runs/hotel_hotels_aegean/ --boundary-qps 4207
python gather.py experiment/runs/hotel_hotels_aegean_eo/ --boundary-qps 3990
python gather.py experiment/runs/hotel_hotels_pbeo/ --boundary-qps 6975
python gather.py experiment/runs/hotel_hotels_unreplicated/ --boundary-qps 7942

python gather.py experiment/runs/hotel_recommendations_aegean_eo/ --boundary-qps 2749
python gather.py experiment/runs/hotel_recommendations_pbeo/ --boundary-qps 5874
python gather.py experiment/runs/hotel_recommendations_unreplicated/ --boundary-qps 7000

python gather.py experiment/runs/media_review_compose_aegean/ --boundary-qps 750
python gather.py experiment/runs/media_review_compose_aegean_eo/ --boundary-qps 750
python gather.py experiment/runs/media_review_compose_pbeo/ --boundary-qps 1452
python gather.py experiment/runs/media_review_compose_unreplicated/ --boundary-qps 1985

python gather.py experiment/runs/social_compose_aegean/ --boundary-qps 3077 # inaccurate
python gather.py experiment/runs/social_compose_aegean_eo/ --boundary-qps 1000
python gather.py experiment/runs/social_compose_pbeo/ --boundary-qps 1000
python gather.py experiment/runs/social_compose_unreplicated/ --boundary-qps 1000

python gather.py experiment/runs/response_large_aegean/ --boundary-qps 1000
python gather.py experiment/runs/response_large_aegean_eo/ --boundary-qps 1000
python gather.py experiment/runs/response_large_pbeo/ --boundary-qps 1000
python gather.py experiment/runs/response_large_unreplicated/ --boundary-qps 1000

python gather.py experiment/runs/response_small_aegean/ --boundary-qps 6093
python gather.py experiment/runs/response_small_aegean_eo/ --boundary-qps 7351
python gather.py experiment/runs/response_small_pbeo/ --boundary-qps 6300
python gather.py experiment/runs/response_small_unreplicated/ --boundary-qps 9300

python gather.py experiment/runs/worker_2_aegean/ --boundary-qps 4374
python gather.py experiment/runs/worker_2_aegean_eo/ --boundary-qps 4374

python gather.py experiment/runs/worker_2_aegean/ --boundary-qps 4530
python gather.py experiment/runs/worker_2_aegean_eo/ --boundary-qps 4339

python gather.py experiment/runs/write_large_aegean/ --boundary-qps 2686
python gather.py experiment/runs/write_large_aegean_eo/ --boundary-qps 2499
python gather.py experiment/runs/write_large_pbeo/ --boundary-qps 609
python gather.py experiment/runs/write_large_unreplicated/ --boundary-qps 8119

python gather.py experiment/runs/write_small_aegean/ --boundary-qps 5937
python gather.py experiment/runs/write_small_aegean_eo/ --boundary-qps 5937
python gather.py experiment/runs/write_small_pbeo/ --boundary-qps 5799
python gather.py experiment/runs/write_small_unreplicated/ --boundary-qps 8514
