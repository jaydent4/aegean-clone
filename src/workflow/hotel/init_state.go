package hotelworkflow

import (
	"aegean/common"
	"strconv"
	"strings"
)

func InitState(e workflowRuntime) map[string]string {
	serviceName := common.MustString(e.GetRunConfig(), "service_name")
	var state map[string]string
	switch serviceName {
	case "search":
		state = map[string]string{}
	case "geo":
		state = initHotelGeoState(e)
	case "profile":
		state = initHotelProfileState(e)
	case "rate":
		state = initHotelRateState(e)
	case "recommendation":
		state = initHotelRecommendationState(e)
	case "user":
		state = initHotelUserState(e)
	case "reservation":
		state = initHotelReservationState(e)
	default:
		if hotelIsRecommendationService(serviceName) {
			state = initHotelRecommendationState(e)
		} else {
			state = map[string]string{}
		}
	}
	if !hotelServiceHasPersistentState(serviceName) {
		return state
	}
	return hotelLoadOrSeedState(e, serviceName, state)
}

func hotelServiceHasPersistentState(serviceName string) bool {
	switch serviceName {
	case "geo", "profile", "rate", "recommendation", "reservation", "search", "user":
		return true
	default:
		return hotelIsRecommendationService(serviceName)
	}
}

func hotelIsRecommendationService(serviceName string) bool {
	return strings.HasPrefix(serviceName, "recommendation_")
}

func initHotelGeoState(e workflowRuntime) map[string]string {
	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	state := make(map[string]string, hotelCount)
	for _, point := range hotelSeedGeoPoints(hotelCount) {
		state[hotelGeoKey(point.HotelID)] = encodeJSON(point)
	}
	return state
}

func initHotelProfileState(e workflowRuntime) map[string]string {
	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	state := make(map[string]string, hotelCount)
	for _, profile := range hotelSeedProfiles(hotelCount) {
		state[hotelProfileKey(profile.ID)] = encodeJSON(profile)
	}
	return state
}

func initHotelRateState(e workflowRuntime) map[string]string {
	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	state := make(map[string]string, hotelCount)
	for _, plan := range hotelSeedRatePlans(hotelCount) {
		state[hotelRateKey(plan.HotelID)] = encodeJSON(plan)
	}
	return state
}

func initHotelRecommendationState(e workflowRuntime) map[string]string {
	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	pool := hotelRecommendationPoolFromConfig(e.GetRunConfig())
	state := make(map[string]string, hotelCount)
	for _, hotel := range hotelSeedRecommendationsForPool(hotelCount, pool) {
		state[hotelRecommendationKeyForPool(pool, hotel.HotelID)] = encodeJSON(hotel)
	}
	return state
}

func initHotelUserState(e workflowRuntime) map[string]string {
	userCount := common.MustInt(e.GetRunConfig(), "hotel_user_count")
	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		state[hotelUserKey(hotelSeedUsername(userIdx))] = hotelHashPassword(hotelSeedPassword(userIdx))
	}
	return state
}

func initHotelReservationState(e workflowRuntime) map[string]string {
	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	capacities := hotelSeedReservationCapacities(hotelCount)
	counts := hotelSeedReservationCounts(hotelCount)
	state := make(map[string]string, len(capacities)+len(counts)+1)
	for hotelID, capacity := range capacities {
		state[hotelReservationCapacityKey(hotelID)] = strconv.Itoa(capacity)
	}
	for key, count := range counts {
		state[key] = strconv.Itoa(count)
	}
	seedRecord := HotelReservationRecord{
		RequestID:       "seed-alice",
		HotelID:         "4",
		CustomerName:    "Alice",
		InDate:          "2015-04-09",
		OutDate:         "2015-04-10",
		RoomNumber:      1,
		CreatedAtMicros: 0,
	}
	state[hotelReservationRecordKey(seedRecord.RequestID)] = encodeJSON(seedRecord)
	return state
}
