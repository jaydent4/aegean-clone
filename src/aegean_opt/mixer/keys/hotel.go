package keys

import "time"

const hotelDateLayout = "2006-01-02"

func AddHotelWorkflowKeys(request map[string]any, payload map[string]any, readKeys map[string]struct{}, writeKeys map[string]struct{}) {
	op, _ := request["op"].(string)

	switch op {
	case "nearby":
		readKeys["hotel:geo:index"] = struct{}{}
	case "check_user":
		if username, ok := payload["username"].(string); ok && username != "" {
			readKeys["hotel:user:"+username] = struct{}{}
		}
	case "get_profiles":
		addHotelStringSliceKeys(payload["hotel_ids"], "hotel:profile:", readKeys)
	case "get_rates":
		addHotelStringSliceKeys(payload["hotel_ids"], "hotel:rate:", readKeys)
	case "get_recommendations":
		readKeys["hotel:recommendation:index"] = struct{}{}
	case "check_availability":
		hotelIDs := hotelStringSlice(payload["hotel_ids"])
		stayDates := hotelStayDates(payload["in_date"], payload["out_date"])
		for _, hotelID := range hotelIDs {
			if hotelID == "" {
				continue
			}
			readKeys["hotel:reservation:capacity:"+hotelID] = struct{}{}
			for _, stayDate := range stayDates {
				readKeys["hotel:reservation:count:"+hotelID+":"+stayDate] = struct{}{}
			}
		}
	case "make_reservation":
		hotelID, _ := payload["hotel_id"].(string)
		if hotelID == "" {
			return
		}
		stayDates := hotelStayDates(payload["in_date"], payload["out_date"])
		readKeys["hotel:reservation:capacity:"+hotelID] = struct{}{}
		for _, stayDate := range stayDates {
			key := "hotel:reservation:count:" + hotelID + ":" + stayDate
			readKeys[key] = struct{}{}
			writeKeys[key] = struct{}{}
		}
		if requestID := hotelRequestID(request["request_id"]); requestID != "" {
			writeKeys["hotel:reservation:record:"+requestID] = struct{}{}
		}
	}
}

func addHotelStringSliceKeys(raw any, prefix string, target map[string]struct{}) {
	for _, value := range hotelStringSlice(raw) {
		if value == "" {
			continue
		}
		target[prefix+value] = struct{}{}
	}
}

func hotelStringSlice(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if value, ok := item.(string); ok && value != "" {
				values = append(values, value)
			}
		}
		return values
	default:
		return nil
	}
}

func hotelStayDates(rawInDate any, rawOutDate any) []string {
	inDate, _ := rawInDate.(string)
	outDate, _ := rawOutDate.(string)
	start, err := time.ParseInLocation(hotelDateLayout, inDate, time.UTC)
	if err != nil {
		return nil
	}
	end, err := time.ParseInLocation(hotelDateLayout, outDate, time.UTC)
	if err != nil || !start.Before(end) {
		return nil
	}

	dates := []string{}
	for current := start; current.Before(end); current = current.AddDate(0, 0, 1) {
		dates = append(dates, current.Format(hotelDateLayout))
	}
	return dates
}

func hotelRequestID(raw any) string {
	switch typed := raw.(type) {
	case string:
		return typed
	case int:
		return itoaHotelKey(int64(typed))
	case int64:
		return itoaHotelKey(typed)
	case float64:
		return itoaHotelKey(int64(typed))
	default:
		return ""
	}
}

func itoaHotelKey(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
