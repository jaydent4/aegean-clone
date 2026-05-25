#!/usr/bin/env bash

# python find_boundary.py experiment/runs/hotel_hotels_aegean/ --lower 2000 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/hotel_hotels_aegean_eo/ --lower 2000 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/hotel_hotels_pbeo/ --lower 4000 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/hotel_hotels_unreplicated/ --lower 4000 --upper 10000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/hotel_recommendations_aegean_eo/ --lower 2000 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/hotel_recommendations_pbeo/ --lower 4000 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/hotel_recommendations_unreplicated/ --lower 4000 --upper 10000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/media_review_compose_aegean/ --lower 0 --upper 1000 --min-p90 0.6 --max-p90 0.8
# python find_boundary.py experiment/runs/media_review_compose_aegean_eo/ --lower 0 --upper 1000 --min-p90 0.6 --max-p90 0.8
# python find_boundary.py experiment/runs/media_review_compose_pbeo/ --lower 0 --upper 2000 --min-p90 0.6 --max-p90 0.8
# python find_boundary.py experiment/runs/media_review_compose_unreplicated/ --lower 0 --upper 2000 --min-p90 0.6 --max-p90 0.8

# python find_boundary.py experiment/runs/social_compose_aegean/ --lower 2000 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/social_compose_aegean_eo/ --lower 2000 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/social_compose_pbeo/ --lower 4000 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/social_compose_unreplicated/ --lower 4000 --upper 10000 --min-p90 0.2 --max-p90 0.4

python find_boundary.py experiment/runs/social_compose_nonidem_graph_aegean/ --lower 0 --upper 8000 --min-p90 0.2 --max-p90 0.4
python find_boundary.py experiment/runs/social_compose_nonidem_graph_aegean_eo/ --lower 0 --upper 8000 --min-p90 0.2 --max-p90 0.4
python find_boundary.py experiment/runs/social_compose_nonidem_graph_pbeo/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4
python find_boundary.py experiment/runs/social_compose_nonidem_graph_unreplicated/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/response_large_aegean/ --lower 0 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/response_large_aegean_eo/ --lower 0 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/response_large_pbeo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/response_large_unreplicated/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/response_small_aegean/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/response_small_aegean_eo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/response_small_pbeo/ --lower 0 --upper 30000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/response_small_unreplicated/ --lower 0 --upper 30000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/worker_2_aegean/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/worker_2_aegean_eo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/worker_8_aegean/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/worker_8_aegean_eo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/write_large_aegean/ --lower 0 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/write_large_aegean_eo/ --lower 0 --upper 8000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/write_large_pbeo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/write_large_unreplicated/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/write_small_aegean/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/write_small_aegean_eo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/write_small_pbeo/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/write_small_unreplicated/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/deep_2_aegean/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/deep_2_aegean_eo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/deep_2_pbeo/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/deep_2_unreplicated/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4

# python find_boundary.py experiment/runs/deep_4_aegean/ --lower 0 --upper 4000 --min-p90 0.6 --max-p90 0.8
# python find_boundary.py experiment/runs/deep_4_aegean_eo/ --lower 0 --upper 4000 --min-p90 0.6 --max-p90 0.8
# python find_boundary.py experiment/runs/deep_4_pbeo/ --lower 0 --upper 8000 --min-p90 0.6 --max-p90 0.8
# python find_boundary.py experiment/runs/deep_4_unreplicated/ --lower 0 --upper 8000 --min-p90 0.6 --max-p90 0.8

# python find_boundary.py experiment/runs/wide_3_aegean/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/wide_3_aegean_eo/ --lower 0 --upper 10000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/wide_3_pbeo/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4
# python find_boundary.py experiment/runs/wide_3_unreplicated/ --lower 0 --upper 20000 --min-p90 0.2 --max-p90 0.4
