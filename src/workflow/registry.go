package workflow

import (
	"aegean/aegean/exec"
	"aegean/aegean/unreplicated"
	"aegean/nodes"
	"aegean/pbeo"
	aegeanworkflow "aegean/workflow/aegean"
	externalsrvworkflow "aegean/workflow/external_srv"
	hotelworkflow "aegean/workflow/hotel"
	mediaworkflow "aegean/workflow/media"
	reqraceworkflow "aegean/workflow/req_race"
	responseworkflow "aegean/workflow/response"
	socialworkflow "aegean/workflow/social"
	supersimpleworkflow "aegean/workflow/supersimple"
	writeworkflow "aegean/workflow/write"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"aegean_oha_client":                          aegeanworkflow.OhaClientRequestLogic,
	"aegean_k6_client":                           aegeanworkflow.K6OpenClientRequestLogic,
	"aegean_k6_open_client":                      aegeanworkflow.K6OpenClientRequestLogic,
	"aegean_k6_closed_client":                    aegeanworkflow.K6ClosedClientRequestLogic,
	"external_srv_oha_client":                    externalsrvworkflow.OhaClientRequestLogic,
	"external_srv_k6_open_client":                externalsrvworkflow.K6OpenClientRequestLogic,
	"external_srv_k6_closed_client":              externalsrvworkflow.K6ClosedClientRequestLogic,
	"hotel_k6_closed_client":                     hotelworkflow.K6ClosedClientRequestLogic,
	"hotel_k6_closed_hotels_client":              hotelworkflow.K6ClosedHotelsClientRequestLogic,
	"hotel_k6_closed_recommendations_client":     hotelworkflow.K6ClosedRecommendationsClientRequestLogic,
	"hotel_k6_open_hotels_client":                hotelworkflow.K6OpenHotelsClientRequestLogic,
	"hotel_k6_open_recommendations_client":       hotelworkflow.K6OpenRecommendationsClientRequestLogic,
	"hotel_k6_open_reservation_client":           hotelworkflow.K6OpenReservationClientRequestLogic,
	"media_k6_closed_client":                     mediaworkflow.K6ClosedReviewComposeClientRequestLogic,
	"media_k6_closed_review_compose_client":      mediaworkflow.K6ClosedReviewComposeClientRequestLogic,
	"media_k6_open_review_compose_client":        mediaworkflow.K6OpenReviewComposeClientRequestLogic,
	"req_race_oha_client":                        reqraceworkflow.OhaClientRequestLogic,
	"req_race_k6_open_client":                    reqraceworkflow.K6OpenClientRequestLogic,
	"req_race_k6_closed_client":                  reqraceworkflow.K6ClosedClientRequestLogic,
	"response_k6_open_client":                    responseworkflow.K6OpenClientRequestLogic,
	"social_k6_closed_client":                    socialworkflow.K6ClosedClientRequestLogic,
	"social_k6_closed_read_home_timeline_client": socialworkflow.K6ClosedReadHomeTimelineClientRequestLogic,
	"social_k6_closed_read_user_timeline_client": socialworkflow.K6ClosedReadUserTimelineClientRequestLogic,
	"social_k6_open_client":                      socialworkflow.K6OpenClientRequestLogic,
	"social_k6_open_read_home_timeline_client":   socialworkflow.K6OpenReadHomeTimelineClientRequestLogic,
	"social_k6_open_read_user_timeline_client":   socialworkflow.K6OpenReadUserTimelineClientRequestLogic,
	"supersimple_oha_client":                     supersimpleworkflow.OhaClientRequestLogic,
	"supersimple_k6_closed_client":               supersimpleworkflow.K6ClosedClientRequestLogic,
	"write_k6_open_client":                       writeworkflow.K6OpenClientRequestLogic,
}

var ExecWorkflows = map[string]exec.ExecuteRequestFunc{
	"aegean_backend":           aegeanworkflow.ExecuteRequestBackend,
	"aegean_backend_diverge_1": aegeanworkflow.ExecuteRequestBackendDivergeOneNode,
	"aegean_backend_diverge_2": aegeanworkflow.ExecuteRequestBackendDivergeTwoNode,
	"aegean_backend_diverge_3": aegeanworkflow.ExecuteRequestBackendDivergeThreeNode,
	"aegean_middle":            aegeanworkflow.ExecuteRequestMiddle,
	"aegean_middle_diverge_1":  aegeanworkflow.ExecuteRequestMiddleDivergeOneNode,
	"aegean_middle_diverge_2":  aegeanworkflow.ExecuteRequestMiddleDivergeTwoNode,
	"aegean_middle_diverge_3":  aegeanworkflow.ExecuteRequestMiddleDivergeThreeNode,
	"external_srv_server":      externalsrvworkflow.ExecuteRequestServer,
	"hotel_frontend": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestFrontend(e, request, ndSeed, ndTimestamp)
	},
	"hotel_search": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestSearch(e, request, ndSeed, ndTimestamp)
	},
	"hotel_geo": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestGeo(e, request, ndSeed, ndTimestamp)
	},
	"hotel_rate": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestRate(e, request, ndSeed, ndTimestamp)
	},
	"hotel_profile": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestProfile(e, request, ndSeed, ndTimestamp)
	},
	"hotel_recommendation": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestRecommendation(e, request, ndSeed, ndTimestamp)
	},
	"hotel_user": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestUser(e, request, ndSeed, ndTimestamp)
	},
	"hotel_reservation": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return hotelworkflow.ExecuteRequestReservation(e, request, ndSeed, ndTimestamp)
	},
	"media_review_compose_api": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestReviewComposeAPI(e, request, ndSeed, ndTimestamp)
	},
	"media_user": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestUser(e, request, ndSeed, ndTimestamp)
	},
	"media_movie_id": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestMovieID(e, request, ndSeed, ndTimestamp)
	},
	"media_text": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestText(e, request, ndSeed, ndTimestamp)
	},
	"media_unique_id": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestUniqueID(e, request, ndSeed, ndTimestamp)
	},
	"media_rating": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestRating(e, request, ndSeed, ndTimestamp)
	},
	"media_compose_review": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestComposeReview(e, request, ndSeed, ndTimestamp)
	},
	"media_review_storage": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestReviewStorage(e, request, ndSeed, ndTimestamp)
	},
	"media_user_review": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestUserReview(e, request, ndSeed, ndTimestamp)
	},
	"media_movie_review": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return mediaworkflow.ExecuteRequestMovieReview(e, request, ndSeed, ndTimestamp)
	},
	"req_race_middle":    reqraceworkflow.ExecuteRequestMiddle,
	"req_race_backend_1": reqraceworkflow.ExecuteRequestBackend1,
	"req_race_backend_2": reqraceworkflow.ExecuteRequestBackend2,
	"req_race_backend_3": reqraceworkflow.ExecuteRequestBackend3,
	"response_middle":    responseworkflow.ExecuteRequestMiddle,
	"response_backend":   responseworkflow.ExecuteRequestBackend,
	"social_compose_post": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return socialworkflow.ExecuteRequestComposePost(e, request, ndSeed, ndTimestamp)
	},
	"social_post_storage": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return socialworkflow.ExecuteRequestPostStorage(e, request, ndSeed, ndTimestamp)
	},
	"social_user_timeline": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return socialworkflow.ExecuteRequestUserTimeline(e, request, ndSeed, ndTimestamp)
	},
	"social_home_timeline": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return socialworkflow.ExecuteRequestHomeTimeline(e, request, ndSeed, ndTimestamp)
	},
	"social_social_graph": func(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
		return socialworkflow.ExecuteRequestSocialGraph(e, request, ndSeed, ndTimestamp)
	},
	"supersimple_middle":  supersimpleworkflow.ExecuteRequestMiddle,
	"supersimple_backend": supersimpleworkflow.ExecuteRequestServer,
	"write_middle":        writeworkflow.ExecuteRequestMiddle,
	"write_backend":       writeworkflow.ExecuteRequestBackend,
}

var InitStateWorkflows = map[string]exec.InitStateFunc{
	"default":              aegeanworkflow.InitState,
	"aegean_default":       aegeanworkflow.InitState,
	"external_srv_default": externalsrvworkflow.InitState,
	"hotel_default": func(e *exec.Exec) map[string]string {
		return hotelworkflow.InitState(e)
	},
	"media_default": func(e *exec.Exec) map[string]string {
		return mediaworkflow.InitState(e)
	},
	"req_race_default": reqraceworkflow.InitState,
	"response_default": responseworkflow.InitState,
	"social_default": func(e *exec.Exec) map[string]string {
		return socialworkflow.InitState(e)
	},
	"supersimple_default": supersimpleworkflow.InitState,
	"write_default":       writeworkflow.InitState,
}

var PBEOExecWorkflows = map[string]pbeo.ExecuteRequestFunc{
	"aegean_middle_pbeo":  aegeanworkflow.ExecuteRequestMiddlePBEO,
	"aegean_backend_pbeo": aegeanworkflow.ExecuteRequestBackendPBEO,
	"write_middle_pbeo":   writeworkflow.ExecuteRequestMiddlePBEO,
	"write_backend_pbeo":  writeworkflow.ExecuteRequestBackendPBEO,
}

var PBEOInitStateWorkflows = map[string]pbeo.InitStateFunc{
	"default":             aegeanworkflow.InitStatePBEO,
	"aegean_pbeo_default": aegeanworkflow.InitStatePBEO,
	"write_pbeo_default":  writeworkflow.InitStatePBEO,
}

var UnreplicatedWorkflows = map[string]unreplicated.WorkflowFunc{
	"aegean_backend":           aegeanworkflow.ExecuteRequestBackendDirect,
	"aegean_middle":            aegeanworkflow.ExecuteRequestMiddleDirect,
	"hotel_frontend":           hotelworkflow.ExecuteRequestFrontendDirect,
	"hotel_search":             hotelworkflow.ExecuteRequestSearchDirect,
	"hotel_geo":                hotelworkflow.ExecuteRequestGeoDirect,
	"hotel_rate":               hotelworkflow.ExecuteRequestRateDirect,
	"hotel_profile":            hotelworkflow.ExecuteRequestProfileDirect,
	"hotel_recommendation":     hotelworkflow.ExecuteRequestRecommendationDirect,
	"hotel_user":               hotelworkflow.ExecuteRequestUserDirect,
	"hotel_reservation":        hotelworkflow.ExecuteRequestReservationDirect,
	"social_compose_post":      socialworkflow.ExecuteRequestComposePostDirect,
	"social_post_storage":      socialworkflow.ExecuteRequestPostStorageDirect,
	"social_user_timeline":     socialworkflow.ExecuteRequestUserTimelineDirect,
	"social_home_timeline":     socialworkflow.ExecuteRequestHomeTimelineDirect,
	"social_social_graph":      socialworkflow.ExecuteRequestSocialGraphDirect,
	"media_review_compose_api": mediaworkflow.ExecuteRequestReviewComposeAPIDirect,
	"media_user":               mediaworkflow.ExecuteRequestUserDirect,
	"media_movie_id":           mediaworkflow.ExecuteRequestMovieIDDirect,
	"media_text":               mediaworkflow.ExecuteRequestTextDirect,
	"media_unique_id":          mediaworkflow.ExecuteRequestUniqueIDDirect,
	"media_rating":             mediaworkflow.ExecuteRequestRatingDirect,
	"media_compose_review":     mediaworkflow.ExecuteRequestComposeReviewDirect,
	"media_review_storage":     mediaworkflow.ExecuteRequestReviewStorageDirect,
	"media_user_review":        mediaworkflow.ExecuteRequestUserReviewDirect,
	"media_movie_review":       mediaworkflow.ExecuteRequestMovieReviewDirect,
	"response_middle":          responseworkflow.ExecuteRequestMiddleDirect,
	"response_backend":         responseworkflow.ExecuteRequestBackendDirect,
	"write_backend":            writeworkflow.ExecuteRequestBackendDirect,
	"write_middle":             writeworkflow.ExecuteRequestMiddleDirect,
}

var UnreplicatedInitStateWorkflows = map[string]unreplicated.InitStateFunc{
	"default":          aegeanworkflow.InitStateDirect,
	"aegean_default":   aegeanworkflow.InitStateDirect,
	"hotel_default":    hotelworkflow.InitStateDirect,
	"media_default":    mediaworkflow.InitStateDirect,
	"response_default": responseworkflow.InitStateDirect,
	"social_default":   socialworkflow.InitStateDirect,
	"write_default":    writeworkflow.InitStateDirect,
}

var ExternalServiceInitWorkflows = map[string]func(es *nodes.ExternalService){
	"default":              externalsrvworkflow.InitExternalService,
	"external_srv_default": externalsrvworkflow.InitExternalService,
}

var ExternalServiceWorkflows = map[string]func(es *nodes.ExternalService, payload map[string]any) map[string]any{
	"external_srv_external_service": externalsrvworkflow.ExternalServiceLogic,
}
