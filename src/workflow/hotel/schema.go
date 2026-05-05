package hotelworkflow

type HotelGeoPoint struct {
	HotelID string  `json:"hotel_id"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
}

type HotelAddress struct {
	StreetNumber string  `json:"street_number"`
	StreetName   string  `json:"street_name"`
	City         string  `json:"city"`
	State        string  `json:"state"`
	Country      string  `json:"country"`
	PostalCode   string  `json:"postal_code"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
}

type HotelProfile struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	PhoneNumber string       `json:"phone_number"`
	Description string       `json:"description"`
	Address     HotelAddress `json:"address"`
}

type HotelRoomType struct {
	BookableRate       float64 `json:"bookable_rate"`
	Code               string  `json:"code"`
	Description        string  `json:"description"`
	TotalRate          float64 `json:"total_rate"`
	TotalRateInclusive float64 `json:"total_rate_inclusive"`
}

type HotelRatePlan struct {
	HotelID  string        `json:"hotel_id"`
	Code     string        `json:"code"`
	InDate   string        `json:"in_date"`
	OutDate  string        `json:"out_date"`
	RoomType HotelRoomType `json:"room_type"`
}

type HotelRecommendation struct {
	HotelID string  `json:"hotel_id"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Rate    float64 `json:"rate"`
	Price   float64 `json:"price"`
}

type HotelSearchLedger struct {
	Count          int      `json:"count"`
	LastSeenMicros int64    `json:"last_seen_micros"`
	LastHotelIDs   []string `json:"last_hotel_ids"`
}

type HotelReservationRecord struct {
	RequestID       string `json:"request_id"`
	HotelID         string `json:"hotel_id"`
	CustomerName    string `json:"customer_name"`
	InDate          string `json:"in_date"`
	OutDate         string `json:"out_date"`
	RoomNumber      int    `json:"room_number"`
	CreatedAtMicros int64  `json:"created_at_micros"`
}
