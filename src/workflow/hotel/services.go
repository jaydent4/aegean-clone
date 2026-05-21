package hotelworkflow

import (
	"aegean/common"
	"sort"
	"strconv"
)

func ExecuteRequestGeo(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "nearby" {
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}

	payload := hotelPayload(request)
	lat, ok := hotelPayloadFloat64(payload, "lat")
	if !ok {
		return hotelErrorResponse(requestID, "missing lat")
	}
	lon, ok := hotelPayloadFloat64(payload, "lon")
	if !ok {
		return hotelErrorResponse(requestID, "missing lon")
	}

	type hotelDistance struct {
		hotelID  string
		distance float64
	}

	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	searchRadiusKm := hotelRunConfigFloatOrDefault(e.GetRunConfig(), "hotel_search_radius_km", hotelSearchRadiusDefaultKm)
	maxResults := common.IntOrDefault(e.GetRunConfig(), "hotel_search_max_results", hotelSearchMaxResultsDefault)

	candidates := make([]hotelDistance, 0, hotelCount)
	for hotelIdx := 1; hotelIdx <= hotelCount; hotelIdx++ {
		hotelID := strconv.Itoa(hotelIdx)
		point, ok := decodeHotelGeoPoint(e.ReadKV(hotelGeoKey(hotelID)))
		if !ok {
			continue
		}
		distance := hotelDistanceKm(lat, lon, point.Lat, point.Lon)
		if distance > searchRadiusKm {
			continue
		}
		candidates = append(candidates, hotelDistance{hotelID: hotelID, distance: distance})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance == candidates[j].distance {
			return candidates[i].hotelID < candidates[j].hotelID
		}
		return candidates[i].distance < candidates[j].distance
	})

	if len(candidates) > maxResults {
		candidates = candidates[:maxResults]
	}

	hotelIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		hotelIDs = append(hotelIDs, candidate.hotelID)
	}

	return hotelAttachParentRequestID(request, map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"hotel_ids":  hotelIDs,
	})
}

func ExecuteRequestRate(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "get_rates" {
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}

	payload := hotelPayload(request)
	inDate := hotelPayloadString(payload, "in_date")
	outDate := hotelPayloadString(payload, "out_date")
	if inDate == "" || outDate == "" {
		return hotelErrorResponse(requestID, "missing in_date or out_date")
	}

	plans := make([]HotelRatePlan, 0)
	for _, hotelID := range hotelUniqueStable(hotelPayloadStringSlice(payload, "hotel_ids")) {
		plan, ok := decodeHotelRatePlan(e.ReadKV(hotelRateKey(hotelID)))
		if !ok {
			continue
		}
		if !hotelDateWithinRange(inDate, outDate, plan.InDate, plan.OutDate) {
			continue
		}
		plans = append(plans, plan)
	}

	sort.Slice(plans, func(i, j int) bool {
		if plans[i].RoomType.TotalRate == plans[j].RoomType.TotalRate {
			return plans[i].HotelID < plans[j].HotelID
		}
		return plans[i].RoomType.TotalRate > plans[j].RoomType.TotalRate
	})

	hotelIDs := make([]string, 0, len(plans))
	for _, plan := range plans {
		hotelIDs = append(hotelIDs, plan.HotelID)
	}

	return hotelAttachParentRequestID(request, map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"hotel_ids":  hotelIDs,
	})
}

func ExecuteRequestProfile(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "get_profiles" {
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}

	payload := hotelPayload(request)
	profilesByID := make(map[string]HotelProfile)
	for _, hotelID := range hotelUniqueStable(hotelPayloadStringSlice(payload, "hotel_ids")) {
		profile, ok := decodeHotelProfile(e.ReadKV(hotelProfileKey(hotelID)))
		if !ok {
			continue
		}
		profilesByID[hotelID] = profile
	}
	profiles := hotelSortProfilesToRequestOrder(hotelPayloadStringSlice(payload, "hotel_ids"), profilesByID)
	responseProfiles := make([]map[string]any, 0, len(profiles))
	for _, profile := range profiles {
		responseProfiles = append(responseProfiles, hotelProfileToPayload(profile))
	}

	return hotelAttachParentRequestID(request, map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"profiles":   responseProfiles,
	})
}

func ExecuteRequestRecommendation(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "get_recommendations" {
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}

	payload := hotelPayload(request)
	requirement := hotelPayloadString(payload, "require")
	lat, ok := hotelPayloadFloat64(payload, "lat")
	if !ok {
		return hotelErrorResponse(requestID, "missing lat")
	}
	lon, ok := hotelPayloadFloat64(payload, "lon")
	if !ok {
		return hotelErrorResponse(requestID, "missing lon")
	}

	hotelCount := common.MustInt(e.GetRunConfig(), "hotel_hotel_count")
	hotels := make([]HotelRecommendation, 0, hotelCount)
	for hotelIdx := 1; hotelIdx <= hotelCount; hotelIdx++ {
		hotelID := strconv.Itoa(hotelIdx)
		hotel, ok := decodeHotelRecommendation(e.ReadKV(hotelRecommendationKeyForRuntime(e, hotelID)))
		if !ok {
			continue
		}
		hotels = append(hotels, hotel)
	}

	hotelIDs, ok := hotelBestRecommendationIDs(hotels, requirement, lat, lon)
	if !ok {
		return hotelErrorResponse(requestID, "invalid require")
	}

	sort.Strings(hotelIDs)
	return hotelAttachParentRequestID(request, map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"hotel_ids":  hotelIDs,
	})
}

func hotelBestRecommendationIDs(hotels []HotelRecommendation, requirement string, lat, lon float64) ([]string, bool) {
	score := func(hotel HotelRecommendation) float64 {
		switch requirement {
		case "dis":
			return hotelDistanceKm(lat, lon, hotel.Lat, hotel.Lon)
		case "rate":
			return hotel.Rate
		case "price":
			return hotel.Price
		default:
			return 0
		}
	}

	switch requirement {
	case "dis", "price":
		sort.Slice(hotels, func(i, j int) bool {
			left := score(hotels[i])
			right := score(hotels[j])
			if left == right {
				return hotels[i].HotelID < hotels[j].HotelID
			}
			return left < right
		})
	case "rate":
		sort.Slice(hotels, func(i, j int) bool {
			left := score(hotels[i])
			right := score(hotels[j])
			if left == right {
				return hotels[i].HotelID < hotels[j].HotelID
			}
			return left > right
		})
	default:
		return nil, false
	}

	if len(hotels) == 0 {
		return nil, true
	}

	bestScore := score(hotels[0])
	hotelIDs := []string{}
	for _, hotel := range hotels {
		if score(hotel) == bestScore {
			hotelIDs = append(hotelIDs, hotel.HotelID)
			continue
		}
		if len(hotelIDs) > 0 {
			break
		}
	}
	return hotelIDs, true
}

func ExecuteRequestUser(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "check_user" {
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}

	payload := hotelPayload(request)
	username := hotelPayloadString(payload, "username")
	password := hotelPayloadString(payload, "password")
	if username == "" || password == "" {
		return hotelErrorResponse(requestID, "missing username or password")
	}

	expectedHash := e.ReadKV(hotelUserKey(username))
	correct := expectedHash != "" && expectedHash == hotelHashPassword(password)
	return hotelAttachParentRequestID(request, map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"correct":    correct,
	})
}

func ExecuteRequestReservation(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	payload := hotelPayload(request)

	switch op {
	case "check_availability":
		inDate := hotelPayloadString(payload, "in_date")
		outDate := hotelPayloadString(payload, "out_date")
		stayDates, ok := hotelMustDateRange(inDate, outDate)
		if !ok {
			return hotelErrorResponse(requestID, "invalid date range")
		}
		roomNumber, ok := hotelPayloadInt(payload, "room_number")
		if !ok || roomNumber <= 0 {
			return hotelErrorResponse(requestID, "room_number must be positive")
		}
		availableHotelIDs := make([]string, 0)
		for _, hotelID := range hotelPayloadStringSlice(payload, "hotel_ids") {
			if hotelReservationAvailable(e, hotelID, stayDates, roomNumber) {
				availableHotelIDs = append(availableHotelIDs, hotelID)
			}
		}
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"hotel_ids":  hotelUniqueStable(availableHotelIDs),
		})

	case "make_reservation":
		hotelID := hotelPayloadString(payload, "hotel_id")
		inDate := hotelPayloadString(payload, "in_date")
		outDate := hotelPayloadString(payload, "out_date")
		customerName := hotelPayloadString(payload, "customer_name")
		roomNumber, ok := hotelPayloadInt(payload, "room_number")
		if hotelID == "" || customerName == "" || !ok || roomNumber <= 0 {
			return hotelErrorResponse(requestID, "missing reservation payload")
		}
		stayDates, ok := hotelMustDateRange(inDate, outDate)
		if !ok {
			return hotelErrorResponse(requestID, "invalid date range")
		}
		if !hotelReservationAvailable(e, hotelID, stayDates, roomNumber) {
			return hotelAttachParentRequestID(request, map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"reserved":   false,
				"hotel_id":   hotelID,
			})
		}

		for _, stayDate := range stayDates {
			currentCount := hotelReservationCount(e, hotelID, stayDate)
			e.WriteKV(hotelReservationCountKey(hotelID, stayDate), strconv.Itoa(currentCount+roomNumber))
		}
		record := HotelReservationRecord{
			RequestID:       hotelRequestIDString(requestID),
			HotelID:         hotelID,
			CustomerName:    customerName,
			InDate:          inDate,
			OutDate:         outDate,
			RoomNumber:      roomNumber,
			CreatedAtMicros: hotelTimestampMicros(ndTimestamp),
		}
		e.WriteKV(hotelReservationRecordKey(record.RequestID), encodeJSON(record))
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"reserved":   true,
			"hotel_id":   hotelID,
		})
	default:
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}
}

func hotelReservationAvailable(e workflowRuntime, hotelID string, stayDates []string, roomNumber int) bool {
	if hotelID == "" || roomNumber <= 0 {
		return false
	}
	capacity := hotelReservationCapacity(e, hotelID)
	if capacity <= 0 {
		return false
	}
	for _, stayDate := range stayDates {
		if hotelReservationCount(e, hotelID, stayDate)+roomNumber > capacity {
			return false
		}
	}
	return true
}

func hotelReservationCapacity(e workflowRuntime, hotelID string) int {
	raw := e.ReadKV(hotelReservationCapacityKey(hotelID))
	value, _ := strconv.Atoi(raw)
	return value
}

func hotelReservationCount(e workflowRuntime, hotelID, stayDate string) int {
	raw := e.ReadKV(hotelReservationCountKey(hotelID, stayDate))
	value, _ := strconv.Atoi(raw)
	return value
}

func hotelProfileToPayload(profile HotelProfile) map[string]any {
	return map[string]any{
		"id":           profile.ID,
		"name":         profile.Name,
		"phone_number": profile.PhoneNumber,
		"description":  profile.Description,
		"address": map[string]any{
			"street_number": profile.Address.StreetNumber,
			"street_name":   profile.Address.StreetName,
			"city":          profile.Address.City,
			"state":         profile.Address.State,
			"country":       profile.Address.Country,
			"postal_code":   profile.Address.PostalCode,
			"lat":           profile.Address.Lat,
			"lon":           profile.Address.Lon,
		},
	}
}

func hotelProfilesFromPayload(raw any) []HotelProfile {
	switch typed := raw.(type) {
	case []HotelProfile:
		return append([]HotelProfile{}, typed...)
	case []any:
		out := make([]HotelProfile, 0, len(typed))
		for _, item := range typed {
			entry, ok := item.(map[string]any)
			if !ok {
				continue
			}
			profile, ok := hotelProfileFromPayload(entry)
			if !ok {
				continue
			}
			out = append(out, profile)
		}
		return out
	case []map[string]any:
		out := make([]HotelProfile, 0, len(typed))
		for _, item := range typed {
			profile, ok := hotelProfileFromPayload(item)
			if ok {
				out = append(out, profile)
			}
		}
		return out
	default:
		return nil
	}
}

func hotelProfileFromPayload(payload map[string]any) (HotelProfile, bool) {
	addressRaw, _ := payload["address"].(map[string]any)
	if addressRaw == nil {
		return HotelProfile{}, false
	}
	lat, ok := hotelPayloadFloat64(addressRaw, "lat")
	if !ok {
		return HotelProfile{}, false
	}
	lon, ok := hotelPayloadFloat64(addressRaw, "lon")
	if !ok {
		return HotelProfile{}, false
	}
	return HotelProfile{
		ID:          hotelPayloadString(payload, "id"),
		Name:        hotelPayloadString(payload, "name"),
		PhoneNumber: hotelPayloadString(payload, "phone_number"),
		Description: hotelPayloadString(payload, "description"),
		Address: HotelAddress{
			StreetNumber: hotelPayloadString(addressRaw, "street_number"),
			StreetName:   hotelPayloadString(addressRaw, "street_name"),
			City:         hotelPayloadString(addressRaw, "city"),
			State:        hotelPayloadString(addressRaw, "state"),
			Country:      hotelPayloadString(addressRaw, "country"),
			PostalCode:   hotelPayloadString(addressRaw, "postal_code"),
			Lat:          lat,
			Lon:          lon,
		},
	}, true
}
