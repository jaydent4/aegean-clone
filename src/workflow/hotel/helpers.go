package hotelworkflow

import (
	"aegean/common"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	hotelDateLayout               = "2006-01-02"
	hotelSearchRadiusDefaultKm    = 10.0
	hotelSearchMaxResultsDefault  = 5
	hotelSeedReservationStartDate = "2015-04-09"
	hotelSeedReservationEndDate   = "2015-04-28"
)

func hotelGeoKey(hotelID string) string {
	return "hotel:geo:" + hotelID
}

func hotelProfileKey(hotelID string) string {
	return "hotel:profile:" + hotelID
}

func hotelRateKey(hotelID string) string {
	return "hotel:rate:" + hotelID
}

func hotelRecommendationKey(hotelID string) string {
	return "hotel:recommendation:" + hotelID
}

func hotelRecommendationPoolFromConfig(config map[string]any) string {
	return strings.TrimSpace(common.StringOrDefault(config, "hotel_recommendation_pool", ""))
}

func hotelRecommendationPoolPrefix(pool string) string {
	pool = strings.TrimSpace(pool)
	if pool == "" || pool == "default" {
		return "hotel:recommendation:"
	}
	return "hotel:recommendation:" + pool + ":"
}

func hotelRecommendationKeyForPool(pool, hotelID string) string {
	return hotelRecommendationPoolPrefix(pool) + hotelID
}

func hotelRecommendationKeyForRuntime(e workflowRuntime, hotelID string) string {
	return hotelRecommendationKeyForPool(hotelRecommendationPoolFromConfig(e.GetRunConfig()), hotelID)
}

func hotelSearchLedgerKey(payload map[string]any) string {
	lat, _ := hotelPayloadFloat64(payload, "lat")
	lon, _ := hotelPayloadFloat64(payload, "lon")
	roomNumber := hotelSearchRoomNumber(payload)
	return strings.Join([]string{
		"hotel:search",
		strconv.FormatFloat(lat, 'f', 6, 64),
		strconv.FormatFloat(lon, 'f', 6, 64),
		hotelPayloadString(payload, "in_date"),
		hotelPayloadString(payload, "out_date"),
		strconv.Itoa(roomNumber),
	}, ":")
}

func hotelUserKey(username string) string {
	return "hotel:user:" + username
}

func hotelReservationCapacityKey(hotelID string) string {
	return "hotel:reservation:capacity:" + hotelID
}

func hotelReservationCountKey(hotelID, stayDate string) string {
	return "hotel:reservation:count:" + hotelID + ":" + stayDate
}

func hotelReservationRecordKey(requestID string) string {
	return "hotel:reservation:record:" + requestID
}

func hotelPayload(request map[string]any) map[string]any {
	opPayload, _ := request["op_payload"].(map[string]any)
	if opPayload == nil {
		return map[string]any{}
	}
	return opPayload
}

func hotelPayloadString(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func hotelPayloadStringSlice(payload map[string]any, key string) []string {
	switch typed := payload[key].(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, raw := range typed {
			if value, ok := raw.(string); ok && value != "" {
				values = append(values, value)
			}
		}
		return values
	default:
		return nil
	}
}

func hotelPayloadInt(payload map[string]any, key string) (int, bool) {
	switch typed := payload[key].(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		value, err := strconv.Atoi(typed)
		return value, err == nil
	default:
		return 0, false
	}
}

func hotelPayloadFloat64(payload map[string]any, key string) (float64, bool) {
	switch typed := payload[key].(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case string:
		value, err := strconv.ParseFloat(typed, 64)
		return value, err == nil
	default:
		return 0, false
	}
}

func hotelRunConfigFloatOrDefault(config map[string]any, key string, defaultValue float64) float64 {
	value, ok := config[key]
	if !ok {
		return defaultValue
	}
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		panic(fmt.Sprintf("run config field %q must be a float", key))
	}
}

func hotelRunConfigStringSlice(config map[string]any, key string) []string {
	value, ok := config[key]
	if !ok {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, raw := range typed {
			value, ok := raw.(string)
			if ok && value != "" {
				values = append(values, value)
			}
		}
		return values
	case string:
		parts := strings.Split(typed, ",")
		values := make([]string, 0, len(parts))
		for _, part := range parts {
			value := strings.TrimSpace(part)
			if value != "" {
				values = append(values, value)
			}
		}
		return values
	default:
		return nil
	}
}

func hotelNestedRequestID(parentRequestID any, serviceName string) string {
	return fmt.Sprintf("%v/%s", parentRequestID, serviceName)
}

func hotelNestedTargets(runConfig map[string]any, replicas []string) []string {
	if len(replicas) == 0 {
		return nil
	}
	if !common.BoolOrDefault(runConfig, "hotel_nested_send_all_replicas", false) {
		return []string{replicas[0]}
	}
	return append([]string{}, replicas...)
}

func hotelServiceTargets(e workflowRuntime, serviceName string, fallback []string) []string {
	return common.ServiceNodesOrDefault(e.GetRunConfig(), serviceName, fallback)
}

func hotelDispatchNestedRequest(e workflowRuntime, sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	selectedTargets := hotelNestedTargets(e.GetRunConfig(), targets)
	if common.BoolOrDefault(e.GetRunConfig(), "hotel_nested_use_eo", false) {
		e.DispatchNestedRequestEO(sourceRequest, selectedTargets, outgoing)
		return
	}
	e.DispatchNestedRequestDirect(sourceRequest, selectedTargets, outgoing)
}

func hotelNewNestedRequest(parentRequestID any, childName string, ndTimestamp float64, op string, opPayload map[string]any) map[string]any {
	return map[string]any{
		"type":              "request",
		"request_id":        hotelNestedRequestID(parentRequestID, childName),
		"parent_request_id": parentRequestID,
		"timestamp":         ndTimestamp,
		"op":                op,
		"op_payload":        opPayload,
	}
}

func hotelSelectedNestedResponse(nestedResponses []map[string]any, parentRequestID any, serviceName string) map[string]any {
	expectedRequestID := hotelNestedRequestID(parentRequestID, serviceName)
	for _, nested := range nestedResponses {
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			continue
		}
		requestID, _ := nested["request_id"].(string)
		if requestID == expectedRequestID {
			return nested
		}
	}
	return nil
}

type hotelNestedResponseLookupRuntime interface {
	GetNestedResponseByRequestID(requestID any, nestedRequestID string) (map[string]any, bool)
}

func hotelSelectedNestedResponseFromRuntime(e workflowRuntime, parentRequestID any, serviceName string) (map[string]any, int) {
	expectedRequestID := hotelNestedRequestID(parentRequestID, serviceName)
	if lookup, ok := e.(hotelNestedResponseLookupRuntime); ok {
		if nested, ok := lookup.GetNestedResponseByRequestID(parentRequestID, expectedRequestID); ok {
			return nested, 1
		}
		return nil, 0
	}
	nestedResponses, ok := e.GetNestedResponses(parentRequestID)
	if !ok {
		return nil, 0
	}
	return hotelSelectedNestedResponse(nestedResponses, parentRequestID, serviceName), len(nestedResponses)
}

func hotelFirstReadyNestedResponse(nestedResponses []map[string]any, parentRequestID any, serviceNames []string) (map[string]any, string) {
	expected := make(map[string]string, len(serviceNames))
	for _, serviceName := range serviceNames {
		if serviceName == "" {
			continue
		}
		expected[hotelNestedRequestID(parentRequestID, serviceName)] = serviceName
	}
	for _, nested := range nestedResponses {
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			continue
		}
		requestID, _ := nested["request_id"].(string)
		if serviceName, ok := expected[requestID]; ok {
			return nested, serviceName
		}
	}
	return nil, ""
}

func hotelNestedResponsesReady(nestedResponses []map[string]any, parentRequestID any, serviceNames ...string) bool {
	for _, serviceName := range serviceNames {
		if hotelSelectedNestedResponse(nestedResponses, parentRequestID, serviceName) == nil {
			return false
		}
	}
	return true
}

func hotelNestedResponsePayload(nested map[string]any) map[string]any {
	response, _ := nested["response"].(map[string]any)
	if response == nil {
		return map[string]any{}
	}
	return response
}

func hotelBlockedForNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func hotelErrorResponse(requestID any, message string) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "error",
		"error":      message,
	}
}

func hotelAttachParentRequestID(request map[string]any, response map[string]any) map[string]any {
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func hotelNestedOKResponse(request map[string]any) map[string]any {
	return hotelAttachParentRequestID(request, map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	})
}

func hotelTimestampMicros(ndTimestamp float64) int64 {
	if ndTimestamp == 0 {
		return 0
	}
	return int64(ndTimestamp * 1_000_000)
}

func encodeJSON(value any) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func decodeHotelGeoPoint(raw string) (HotelGeoPoint, bool) {
	var point HotelGeoPoint
	if raw == "" {
		return point, false
	}
	if err := json.Unmarshal([]byte(raw), &point); err != nil {
		return HotelGeoPoint{}, false
	}
	return point, true
}

func decodeHotelProfile(raw string) (HotelProfile, bool) {
	var profile HotelProfile
	if raw == "" {
		return profile, false
	}
	if err := json.Unmarshal([]byte(raw), &profile); err != nil {
		return HotelProfile{}, false
	}
	return profile, true
}

func decodeHotelRatePlan(raw string) (HotelRatePlan, bool) {
	var plan HotelRatePlan
	if raw == "" {
		return plan, false
	}
	if err := json.Unmarshal([]byte(raw), &plan); err != nil {
		return HotelRatePlan{}, false
	}
	return plan, true
}

func decodeHotelRecommendation(raw string) (HotelRecommendation, bool) {
	var hotel HotelRecommendation
	if raw == "" {
		return hotel, false
	}
	if err := json.Unmarshal([]byte(raw), &hotel); err != nil {
		return HotelRecommendation{}, false
	}
	return hotel, true
}

func decodeHotelSearchLedger(raw string) (HotelSearchLedger, bool) {
	var ledger HotelSearchLedger
	if raw == "" {
		return ledger, false
	}
	if err := json.Unmarshal([]byte(raw), &ledger); err != nil {
		return HotelSearchLedger{}, false
	}
	return ledger, true
}

func decodeHotelReservationRecord(raw string) (HotelReservationRecord, bool) {
	var record HotelReservationRecord
	if raw == "" {
		return record, false
	}
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return HotelReservationRecord{}, false
	}
	return record, true
}

func hotelHashPassword(password string) string {
	sum := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", sum)
}

func hotelMustDateRange(inDate, outDate string) ([]string, bool) {
	start, ok := hotelParseDate(inDate)
	if !ok {
		return nil, false
	}
	end, ok := hotelParseDate(outDate)
	if !ok || !start.Before(end) {
		return nil, false
	}
	dates := []string{}
	for current := start; current.Before(end); current = current.AddDate(0, 0, 1) {
		dates = append(dates, current.Format(hotelDateLayout))
	}
	return dates, true
}

func hotelParseDate(raw string) (time.Time, bool) {
	parsed, err := time.ParseInLocation(hotelDateLayout, raw, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func hotelDateWithinRange(requestIn, requestOut, planIn, planOut string) bool {
	requestStart, ok := hotelParseDate(requestIn)
	if !ok {
		return false
	}
	requestEnd, ok := hotelParseDate(requestOut)
	if !ok {
		return false
	}
	planStart, ok := hotelParseDate(planIn)
	if !ok {
		return false
	}
	planEnd, ok := hotelParseDate(planOut)
	if !ok {
		return false
	}
	return !requestStart.Before(planStart) && !requestEnd.After(planEnd)
}

func hotelDistanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	latDelta := (lat2 - lat1) * math.Pi / 180
	lonDelta := (lon2 - lon1) * math.Pi / 180

	sinLat := math.Sin(latDelta / 2)
	sinLon := math.Sin(lonDelta / 2)
	a := sinLat*sinLat + math.Cos(lat1Rad)*math.Cos(lat2Rad)*sinLon*sinLon
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}

func hotelGeoJSONResponse(profiles []HotelProfile) map[string]any {
	features := make([]map[string]any, 0, len(profiles))
	for _, profile := range profiles {
		features = append(features, map[string]any{
			"type": "Feature",
			"id":   profile.ID,
			"properties": map[string]any{
				"name":         profile.Name,
				"phone_number": profile.PhoneNumber,
			},
			"geometry": map[string]any{
				"type":        "Point",
				"coordinates": []float64{profile.Address.Lon, profile.Address.Lat},
			},
		})
	}
	return map[string]any{
		"type":     "FeatureCollection",
		"features": features,
	}
}

func hotelSeedUsername(userIdx int) string {
	return fmt.Sprintf("Cornell_%d", userIdx)
}

func hotelSeedPassword(userIdx int) string {
	suffix := strconv.Itoa(userIdx)
	return strings.Repeat(suffix, 10)
}

func hotelSeedGeoPoints(hotelCount int) []HotelGeoPoint {
	points := []HotelGeoPoint{
		{HotelID: "1", Lat: 37.7867, Lon: -122.4112},
		{HotelID: "2", Lat: 37.7854, Lon: -122.4005},
		{HotelID: "3", Lat: 37.7834, Lon: -122.4071},
		{HotelID: "4", Lat: 37.7936, Lon: -122.3930},
		{HotelID: "5", Lat: 37.7831, Lon: -122.4181},
		{HotelID: "6", Lat: 37.7863, Lon: -122.4015},
	}
	for i := 7; i <= hotelCount; i++ {
		hotelID := strconv.Itoa(i)
		points = append(points, HotelGeoPoint{
			HotelID: hotelID,
			Lat:     37.7835 + float64(i)/500.0*3,
			Lon:     -122.41 + float64(i)/500.0*4,
		})
	}
	return points
}

func hotelSeedProfiles(hotelCount int) []HotelProfile {
	profiles := []HotelProfile{
		{
			ID:          "1",
			Name:        "Clift Hotel",
			PhoneNumber: "(415) 775-4700",
			Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
			Address: HotelAddress{
				StreetNumber: "495", StreetName: "Geary St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94102", Lat: 37.7867, Lon: -122.4112,
			},
		},
		{
			ID:          "2",
			Name:        "W San Francisco",
			PhoneNumber: "(415) 777-5300",
			Description: "Less than a block from the Yerba Buena Center for the Arts, this trendy hotel is a 12-minute walk from Union Square.",
			Address: HotelAddress{
				StreetNumber: "181", StreetName: "3rd St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94103", Lat: 37.7854, Lon: -122.4005,
			},
		},
		{
			ID:          "3",
			Name:        "Hotel Zetta",
			PhoneNumber: "(415) 543-8555",
			Description: "A 3-minute walk from the Powell Street cable-car turnaround and BART rail station, this hip hotel 9 minutes from Union Square combines high-tech lodging with artsy touches.",
			Address: HotelAddress{
				StreetNumber: "55", StreetName: "5th St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94103", Lat: 37.7834, Lon: -122.4071,
			},
		},
		{
			ID:          "4",
			Name:        "Hotel Vitale",
			PhoneNumber: "(415) 278-3700",
			Description: "This waterfront hotel with Bay Bridge views is 3 blocks from the Financial District and a 4-minute walk from the Ferry Building.",
			Address: HotelAddress{
				StreetNumber: "8", StreetName: "Mission St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94105", Lat: 37.7936, Lon: -122.3930,
			},
		},
		{
			ID:          "5",
			Name:        "Phoenix Hotel",
			PhoneNumber: "(415) 776-1380",
			Description: "Located in the Tenderloin neighborhood, a 10-minute walk from a BART rail station, this retro motor lodge has hosted many rock musicians and other celebrities since the 1950s. It is a 4-minute walk from the historic Great American Music Hall nightclub.",
			Address: HotelAddress{
				StreetNumber: "601", StreetName: "Eddy St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94109", Lat: 37.7831, Lon: -122.4181,
			},
		},
		{
			ID:          "6",
			Name:        "St. Regis San Francisco",
			PhoneNumber: "(415) 284-4000",
			Description: "St. Regis Museum Tower is a 42-story skyscraper in the South of Market district of San Francisco, adjacent to Yerba Buena Gardens and the San Francisco Museum of Modern Art.",
			Address: HotelAddress{
				StreetNumber: "125", StreetName: "3rd St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94109", Lat: 37.7863, Lon: -122.4015,
			},
		},
	}
	for i := 7; i <= hotelCount; i++ {
		hotelID := strconv.Itoa(i)
		lat := 37.7835 + float64(i)/500.0*3
		lon := -122.41 + float64(i)/500.0*4
		profiles = append(profiles, HotelProfile{
			ID:          hotelID,
			Name:        "St. Regis San Francisco",
			PhoneNumber: fmt.Sprintf("(415) 284-40%s", hotelID),
			Description: "St. Regis Museum Tower is a 42-story skyscraper in the South of Market district of San Francisco, adjacent to Yerba Buena Gardens and the San Francisco Museum of Modern Art.",
			Address: HotelAddress{
				StreetNumber: "125", StreetName: "3rd St", City: "San Francisco", State: "CA", Country: "United States", PostalCode: "94109", Lat: lat, Lon: lon,
			},
		})
	}
	return profiles
}

func hotelSeedRatePlans(hotelCount int) []HotelRatePlan {
	plans := []HotelRatePlan{
		{
			HotelID: "1", Code: "RACK", InDate: "2015-04-09", OutDate: "2015-04-10",
			RoomType: HotelRoomType{BookableRate: 109, Code: "KNG", Description: "King sized bed", TotalRate: 109, TotalRateInclusive: 123.17},
		},
		{
			HotelID: "2", Code: "RACK", InDate: "2015-04-09", OutDate: "2015-04-10",
			RoomType: HotelRoomType{BookableRate: 139, Code: "QN", Description: "Queen sized bed", TotalRate: 139, TotalRateInclusive: 153.09},
		},
		{
			HotelID: "3", Code: "RACK", InDate: "2015-04-09", OutDate: "2015-04-10",
			RoomType: HotelRoomType{BookableRate: 109, Code: "KNG", Description: "King sized bed", TotalRate: 109, TotalRateInclusive: 123.17},
		},
	}
	for i := 7; i <= hotelCount; i++ {
		if i%3 != 0 {
			continue
		}
		rate := 109.0
		rateInclusive := 123.17
		switch i % 5 {
		case 1:
			rate = 120
			rateInclusive = 140
		case 2:
			rate = 124
			rateInclusive = 144
		case 3:
			rate = 132
			rateInclusive = 158
		case 4:
			rate = 232
			rateInclusive = 258
		}
		outDate := "2015-04-24"
		if i%2 == 0 {
			outDate = "2015-04-17"
		}
		plans = append(plans, HotelRatePlan{
			HotelID: strconv.Itoa(i),
			Code:    "RACK",
			InDate:  "2015-04-09",
			OutDate: outDate,
			RoomType: HotelRoomType{
				BookableRate: rate, Code: "KNG", Description: "King sized bed", TotalRate: rate, TotalRateInclusive: rateInclusive,
			},
		})
	}
	return plans
}

func hotelSeedRecommendations(hotelCount int) []HotelRecommendation {
	hotels := []HotelRecommendation{
		{HotelID: "1", Lat: 37.7867, Lon: -122.4112, Rate: 109, Price: 150},
		{HotelID: "2", Lat: 37.7854, Lon: -122.4005, Rate: 139, Price: 120},
		{HotelID: "3", Lat: 37.7834, Lon: -122.4071, Rate: 109, Price: 190},
		{HotelID: "4", Lat: 37.7936, Lon: -122.3930, Rate: 129, Price: 160},
		{HotelID: "5", Lat: 37.7831, Lon: -122.4181, Rate: 119, Price: 140},
		{HotelID: "6", Lat: 37.7863, Lon: -122.4015, Rate: 149, Price: 200},
	}
	for i := 7; i <= hotelCount; i++ {
		rate := 135.0
		price := 179.0
		if i%3 == 0 {
			switch i % 5 {
			case 1:
				rate = 120
				price = 140
			case 2:
				rate = 124
				price = 144
			case 3:
				rate = 132
				price = 158
			case 4:
				rate = 232
				price = 258
			default:
				rate = 109
				price = 123.17
			}
		}
		hotels = append(hotels, HotelRecommendation{
			HotelID: strconv.Itoa(i),
			Lat:     37.7835 + float64(i)/500.0*3,
			Lon:     -122.41 + float64(i)/500.0*4,
			Rate:    rate,
			Price:   price,
		})
	}
	return hotels
}

func hotelSeedRecommendationsForPool(hotelCount int, pool string) []HotelRecommendation {
	hotels := hotelSeedRecommendations(hotelCount)
	pool = strings.TrimSpace(pool)
	if pool == "" || pool == "default" || pool == "pool_1" {
		return hotels
	}

	poolIndex := hotelRecommendationPoolIndex(pool)
	for idx := range hotels {
		hotelID, _ := strconv.Atoi(hotels[idx].HotelID)
		latStep := float64(((hotelID + poolIndex*2) % 9) - 4)
		lonStep := float64(((hotelID*2 + poolIndex*3) % 9) - 4)
		hotels[idx].Lat += latStep * 0.0015
		hotels[idx].Lon += lonStep * 0.0015
		hotels[idx].Rate = 90 + float64((hotelID*poolIndex*7+poolIndex*13)%160)
		hotels[idx].Price = 80 + float64((hotelID*(poolIndex+5)*11+poolIndex*19)%180)
	}
	return hotels
}

func hotelRecommendationPoolIndex(pool string) int {
	switch pool {
	case "pool_1":
		return 1
	case "pool_2":
		return 2
	case "pool_3":
		return 3
	default:
		total := 0
		for _, char := range pool {
			total += int(char)
		}
		return total%17 + 1
	}
}

func hotelSeedReservationCapacities(hotelCount int) map[string]int {
	capacities := map[string]int{
		"1": 200,
		"2": 200,
		"3": 200,
		"4": 200,
		"5": 200,
		"6": 200,
	}
	for i := 7; i <= hotelCount; i++ {
		capacity := 200
		switch i % 3 {
		case 1:
			capacity = 300
		case 2:
			capacity = 250
		}
		capacities[strconv.Itoa(i)] = capacity
	}
	return capacities
}

func hotelSeedReservationCounts(hotelCount int) map[string]int {
	state := make(map[string]int)
	start, _ := hotelParseDate(hotelSeedReservationStartDate)
	end, _ := hotelParseDate(hotelSeedReservationEndDate)
	for i := 1; i <= hotelCount; i++ {
		hotelID := strconv.Itoa(i)
		for current := start; current.Before(end); current = current.AddDate(0, 0, 1) {
			state[hotelReservationCountKey(hotelID, current.Format(hotelDateLayout))] = 0
		}
	}
	state[hotelReservationCountKey("4", "2015-04-09")] = 1
	return state
}

func hotelSortProfilesToRequestOrder(requestIDs []string, profilesByID map[string]HotelProfile) []HotelProfile {
	profiles := make([]HotelProfile, 0, len(requestIDs))
	for _, hotelID := range requestIDs {
		profile, ok := profilesByID[hotelID]
		if !ok {
			continue
		}
		profiles = append(profiles, profile)
	}
	return profiles
}

func hotelUniqueStable(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func hotelRequestIDString(requestID any) string {
	return fmt.Sprintf("%v", requestID)
}
