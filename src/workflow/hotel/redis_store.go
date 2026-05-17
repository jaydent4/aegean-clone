package hotelworkflow

import (
	"aegean/common"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	hotelRedisMu     sync.Mutex
	hotelRedisClient *redis.Client
	hotelRedisAddrID string
)

func hotelRedisEnabled(runConfig map[string]any) bool {
	if enabled, ok := os.LookupEnv("HOTEL_REDIS_ENABLE"); ok {
		return enabled == "1"
	}
	return common.BoolOrDefault(runConfig, "hotel_redis_enable", false)
}

func hotelRedisAddr(runConfig map[string]any) string {
	if addr := os.Getenv("HOTEL_REDIS_ADDR"); addr != "" {
		return addr
	}
	if configured, ok := runConfig["hotel_redis_addr"]; ok {
		addr, ok := configured.(string)
		if !ok {
			panic("run config field \"hotel_redis_addr\" must be a string")
		}
		if addr != "" {
			return addr
		}
	}
	return "127.0.0.1:6379"
}

func getHotelRedisClient(runConfig map[string]any) (*redis.Client, error) {
	if !hotelRedisEnabled(runConfig) {
		return nil, nil
	}
	addr := hotelRedisAddr(runConfig)

	hotelRedisMu.Lock()
	defer hotelRedisMu.Unlock()

	if hotelRedisClient != nil && hotelRedisAddrID == addr {
		return hotelRedisClient, nil
	}
	if hotelRedisClient != nil {
		_ = hotelRedisClient.Close()
		hotelRedisClient = nil
		hotelRedisAddrID = ""
	}

	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	hotelRedisClient = client
	hotelRedisAddrID = addr
	return hotelRedisClient, nil
}

func hotelReadKV(e workflowRuntime, key string) string {
	if value := e.ReadKV(key); value != "" {
		return value
	}
	client, err := getHotelRedisClient(e.GetRunConfig())
	if err != nil {
		log.Printf("hotel redis read unavailable for %s: %v", key, err)
		return ""
	}
	if client == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := client.Get(ctx, key).Result()
	switch err {
	case nil:
		return value
	case redis.Nil:
		return ""
	default:
		log.Printf("hotel redis read failed for %s: %v", key, err)
		return ""
	}
}

func hotelWriteKV(e workflowRuntime, key, value string) {
	e.WriteKV(key, value)
	client, err := getHotelRedisClient(e.GetRunConfig())
	if err != nil {
		log.Printf("hotel redis write unavailable for %s: %v", key, err)
		return
	}
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Set(ctx, key, value, 0).Err(); err != nil {
		log.Printf("hotel redis write failed for %s: %v", key, err)
	}
}

func hotelLoadOrSeedState(e workflowRuntime, serviceName string, seed map[string]string) map[string]string {
	state := common.CopyStringMap(seed)
	client, err := getHotelRedisClient(e.GetRunConfig())
	if err != nil {
		log.Printf("hotel redis hydrate unavailable for %s: %v", serviceName, err)
		return state
	}
	if client == nil {
		return state
	}

	prefixes := hotelServiceStatePrefixes(e, serviceName)
	if len(prefixes) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		persisted, loadErr := hotelLoadStateByPrefixes(ctx, client, prefixes)
		if loadErr != nil {
			log.Printf("hotel redis hydrate failed for %s: %v", serviceName, loadErr)
		} else {
			for key, value := range persisted {
				state[key] = value
			}
		}
		if seedErr := hotelSeedMissingState(ctx, client, seed); seedErr != nil {
			log.Printf("hotel redis seed failed for %s: %v", serviceName, seedErr)
		}
	}

	return state
}

func hotelServiceStatePrefixes(e workflowRuntime, serviceName string) []string {
	switch serviceName {
	case "geo":
		return []string{"hotel:geo:"}
	case "search":
		return []string{"hotel:search:"}
	case "profile":
		return []string{"hotel:profile:"}
	case "rate":
		return []string{"hotel:rate:"}
	case "recommendation":
		return []string{hotelRecommendationPoolPrefix(hotelRecommendationPoolFromConfig(e.GetRunConfig()))}
	case "user":
		return []string{"hotel:user:"}
	case "reservation":
		return []string{
			"hotel:reservation:capacity:",
			"hotel:reservation:count:",
			"hotel:reservation:record:",
		}
	default:
		if hotelIsRecommendationService(serviceName) {
			return []string{hotelRecommendationPoolPrefix(hotelRecommendationPoolFromConfig(e.GetRunConfig()))}
		}
		return nil
	}
}

func hotelLoadStateByPrefixes(ctx context.Context, client *redis.Client, prefixes []string) (map[string]string, error) {
	state := make(map[string]string)
	for _, prefix := range prefixes {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 128).Result()
			if err != nil {
				return nil, err
			}
			if len(keys) > 0 {
				values, err := client.MGet(ctx, keys...).Result()
				if err != nil {
					return nil, err
				}
				for idx, key := range keys {
					if idx >= len(values) || values[idx] == nil {
						continue
					}
					state[key] = fmt.Sprint(values[idx])
				}
			}
			if next == 0 {
				break
			}
			cursor = next
		}
	}
	return state, nil
}

func hotelSeedMissingState(ctx context.Context, client *redis.Client, seed map[string]string) error {
	if len(seed) == 0 {
		return nil
	}

	keys := make([]string, 0, len(seed))
	for key := range seed {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	pipe := client.Pipeline()
	for _, key := range keys {
		pipe.SetNX(ctx, key, seed[key], 0)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}
