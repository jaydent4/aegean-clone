#!/usr/bin/env bash

python find_boundary_and_gather.py experiment/runs/hotel_hotels_aegean/ --lower 2000 --upper 8000
python find_boundary_and_gather.py experiment/runs/hotel_hotels_aegean_eo/ --lower 2000 --upper 8000
python find_boundary_and_gather.py experiment/runs/hotel_hotels_pbeo/ --lower 4000 --upper 10000
python find_boundary_and_gather.py experiment/runs/hotel_hotels_unreplicated/ --lower 4000 --upper 10000

python find_boundary_and_gather.py experiment/runs/hotel_recommendations_aegean/ --lower 2000 --upper 8000
python find_boundary_and_gather.py experiment/runs/hotel_recommendations_aegean_eo/ --lower 2000 --upper 8000
python find_boundary_and_gather.py experiment/runs/hotel_recommendations_pbeo/ --lower 4000 --upper 10000
python find_boundary_and_gather.py experiment/runs/hotel_recommendations_unreplicated/ --lower 4000 --upper 10000

python find_boundary_and_gather.py experiment/runs/media_review_compose_aegean/ --lower 0 --upper 1000 --min-p90 0.8 --max-p90 1
python find_boundary_and_gather.py experiment/runs/media_review_compose_aegean_eo/ --lower 0 --upper 1000 --min-p90 0.8 --max-p90 1
python find_boundary_and_gather.py experiment/runs/media_review_compose_pbeo/ --lower 0 --upper 2000 --min-p90 0.8 --max-p90 1
python find_boundary_and_gather.py experiment/runs/media_review_compose_unreplicated/ --lower 0 --upper 2000 --min-p90 0.8 --max-p90 1

python find_boundary_and_gather.py experiment/runs/social_compose_aegean/ --lower 2000 --upper 8000
python find_boundary_and_gather.py experiment/runs/social_compose_aegean_eo/ --lower 2000 --upper 8000
python find_boundary_and_gather.py experiment/runs/social_compose_pbeo/ --lower 4000 --upper 10000
python find_boundary_and_gather.py experiment/runs/social_compose_unreplicated/ --lower 4000 --upper 10000
