#!/bin/bash

# Remove cached results
echo "Clearing test cache..."
docker compose exec node1 bash -c "go clean -testcache"

echo "Running Aegean tests..."
output=$(docker compose exec node1 bash -c "cd /app/src && go test ./... -v 2>&1")

# Run each package
echo "$output" | grep -E "^(ok|FAIL|---)" | while read -r line; do
  if [[ "$line" == ok* ]]; then
    pkg=$(echo "$line" | awk '{print $2}')
    duration=$(echo "$line" | awk '{print $3}')
    printf "  OK: %-45s %s\n" "$pkg" "$duration"
  elif [[ "$line" == FAIL* ]]; then
    pkg=$(echo "$line" | awk '{print $2}')
    printf "  ERROR: %-45s FAILED\n" "$pkg"
  fi
done

total=$(echo "$output" | grep -c "^--- PASS")
failed=$(echo "$output" | grep -c "^--- FAIL")
skipped=$(echo "$output" | grep -c "^\?")

echo ""
echo "--------------------------------"
printf "  Passed:  %d\n" "$total"
printf "  Failed:  %d\n" "$failed"
printf "  Skipped: %d (no test files)\n" "$skipped"
echo "--------------------------------"
echo ""

if [ "$failed" -gt 0 ]; then
  echo "FAILED TESTS:"
  echo "$output" | grep -A 5 "^--- FAIL"
  exit 1
fi