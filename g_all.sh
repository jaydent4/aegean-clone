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
python gather.py experiment/runs/social_compose_aegean_eo/ --boundary-qps 2209
python gather.py experiment/runs/social_compose_pbeo/ --boundary-qps 7375
python gather.py experiment/runs/social_compose_unreplicated/ --boundary-qps 7984

python gather.py experiment/runs/social_compose_nonidem_graph_aegean_eo/ --boundary-qps 1249
python gather.py experiment/runs/social_compose_nonidem_graph_pbeo/ --boundary-qps 8124
python gather.py experiment/runs/social_compose_nonidem_graph_unreplicated/ --boundary-qps 8124

python gather.py experiment/runs/response_large_aegean/ --boundary-qps 662
python gather.py experiment/runs/response_large_aegean_eo/ --boundary-qps 379
python gather.py experiment/runs/response_large_pbeo/ --boundary-qps 476
python gather.py experiment/runs/response_large_unreplicated/ --boundary-qps 1249

# python gather.py experiment/runs/response_medium_aegean/ --boundary-qps 0
# python gather.py experiment/runs/response_medium_aegean_eo/ --boundary-qps 0
# python gather.py experiment/runs/response_medium_pbeo/ --boundary-qps 0
# python gather.py experiment/runs/response_medium_unreplicated/ --boundary-qps 0

python gather.py experiment/runs/response_small_aegean/ --boundary-qps 6093
python gather.py experiment/runs/response_small_aegean_eo/ --boundary-qps 7351
python gather.py experiment/runs/response_small_pbeo/ --boundary-qps 6300
python gather.py experiment/runs/response_small_unreplicated/ --boundary-qps 9300

# python gather.py experiment/runs/worker_2_aegean/ --boundary-qps 0
# python gather.py experiment/runs/worker_2_aegean_eo/ --boundary-qps 0

# python gather.py experiment/runs/worker_4_aegean/ --boundary-qps 0
# python gather.py experiment/runs/worker_4_aegean_eo/ --boundary-qps 0

# python gather.py experiment/runs/worker_8_aegean/ --boundary-qps 0
# python gather.py experiment/runs/worker_8_aegean_eo/ --boundary-qps 0

python gather.py experiment/runs/write_large_aegean/ --boundary-qps 2686
python gather.py experiment/runs/write_large_aegean_eo/ --boundary-qps 2499
python gather.py experiment/runs/write_large_pbeo/ --boundary-qps 609
python gather.py experiment/runs/write_large_unreplicated/ --boundary-qps 8119

# python gather.py experiment/runs/write_medium_aegean/ --boundary-qps 0
# python gather.py experiment/runs/write_medium_aegean_eo/ --boundary-qps 0
# python gather.py experiment/runs/write_medium_pbeo/ --boundary-qps 0
# python gather.py experiment/runs/write_medium_unreplicated/ --boundary-qps 0

python gather.py experiment/runs/write_small_aegean/ --boundary-qps 5937
python gather.py experiment/runs/write_small_aegean_eo/ --boundary-qps 5937
python gather.py experiment/runs/write_small_pbeo/ --boundary-qps 5799
python gather.py experiment/runs/write_small_unreplicated/ --boundary-qps 8514

python gather.py experiment/runs/deep_2_aegean/ --boundary-qps 6289
python gather.py experiment/runs/deep_2_aegean_eo/ --boundary-qps 6347
python gather.py experiment/runs/deep_2_pbeo/ --boundary-qps 3907
python gather.py experiment/runs/deep_2_unreplicated/ --boundary-qps 8328

# python gather.py experiment/runs/deep_3_aegean/ --boundary-qps 0
# python gather.py experiment/runs/deep_3_aegean_eo/ --boundary-qps 0
# python gather.py experiment/runs/deep_3_pbeo/ --boundary-qps 0
# python gather.py experiment/runs/deep_3_unreplicated/ --boundary-qps 0

python gather.py experiment/runs/deep_4_aegean/ --boundary-qps 2718
python gather.py experiment/runs/deep_4_aegean_eo/ --boundary-qps 2577
python gather.py experiment/runs/deep_4_pbeo/ --boundary-qps 2811
python gather.py experiment/runs/deep_4_unreplicated/ --boundary-qps 7985

# python gather.py experiment/runs/wide_1_aegean/ --boundary-qps 0
# python gather.py experiment/runs/wide_1_aegean_eo/ --boundary-qps 0
# python gather.py experiment/runs/wide_1_pbeo/ --boundary-qps 0
# python gather.py experiment/runs/wide_1_unreplicated/ --boundary-qps 0

# python gather.py experiment/runs/wide_2_aegean/ --boundary-qps 0
# python gather.py experiment/runs/wide_2_aegean_eo/ --boundary-qps 0
# python gather.py experiment/runs/wide_2_pbeo/ --boundary-qps 0
# python gather.py experiment/runs/wide_2_unreplicated/ --boundary-qps 0

python gather.py experiment/runs/wide_3_aegean/ --boundary-qps 5156
python gather.py experiment/runs/wide_3_aegean_eo/ --boundary-qps 4374
python gather.py experiment/runs/wide_3_pbeo/ --boundary-qps 2499
python gather.py experiment/runs/wide_3_unreplicated/ --boundary-qps 7499
