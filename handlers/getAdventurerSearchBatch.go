package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"bdo-rest-api/cache"
	"bdo-rest-api/models"
	"bdo-rest-api/scraper"
	"bdo-rest-api/utils"
	"bdo-rest-api/validators"
)

type adventurerSearchBatchRequest struct {
	Region      string   `json:"region"`
	SearchType  string   `json:"searchType"`
	Queries     []string `json:"queries"`
	BypassCache bool     `json:"bypassCache"`
}

type adventurerSearchBatchItem struct {
	Query      string           `json:"query"`
	Status     string           `json:"status"`
	HTTPStatus int              `json:"httpStatus"`
	Data       []models.Profile `json:"data,omitempty"`
	Error      string           `json:"error,omitempty"`
}

type adventurerSearchBatchResponse struct {
	Region     string                       `json:"region"`
	SearchType string                       `json:"searchType"`
	Results    []adventurerSearchBatchItem  `json:"results"`
	Stats      map[string]int               `json:"stats"`
}

func getAdventurerSearchBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req adventurerSearchBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		giveBadRequestResponse(w, "Invalid JSON body.")
		return
	}

	region, regionOk, regionValidationMessage := validators.ValidateRegionQueryParam([]string{req.Region})
	if !regionOk {
		giveBadRequestResponse(w, regionValidationMessage)
		return
	}

	searchType := validators.ValidateSearchTypeQueryParam([]string{req.SearchType})

	if len(req.Queries) == 0 {
		giveBadRequestResponse(w, "queries list cannot be empty.")
		return
	}

	if len(req.Queries) > 200 {
		giveBadRequestResponse(w, "queries list exceeds max size of 200.")
		return
	}

	if ok := giveMaintenanceResponse(w, region); ok {
		return
	}

	results := make([]adventurerSearchBatchItem, 0, len(req.Queries))
	stats := map[string]int{
		"cached":   0,
		"started":  0,
		"pending":  0,
		"rejected": 0,
		"invalid":  0,
		"error":    0,
	}

	bypassCache := req.BypassCache
	if bypassCache && !utils.CheckAdminToken(r) {
		bypassCache = false
	}

	for _, queryValue := range req.Queries {
		query, queryOk, queryValidationMessage := validators.ValidateAdventurerNameQueryParam([]string{queryValue}, region, searchType)
		if !queryOk {
			results = append(results, adventurerSearchBatchItem{
				Query:      queryValue,
				Status:     "invalid",
				HTTPStatus: http.StatusBadRequest,
				Error:      queryValidationMessage,
			})
			stats["invalid"]++
			continue
		}

		if !bypassCache {
			if data, status, _, _, ok := cache.ProfileSearch.GetRecord([]string{region, query, searchType}); ok {
				item := adventurerSearchBatchItem{
					Query:      query,
					HTTPStatus: status,
				}
				if status == http.StatusOK {
					item.Status = "cached"
					item.Data = data
					stats["cached"]++
				} else {
					item.Status = "error"
					item.Error = "cached non-200 response"
					stats["error"]++
				}
				results = append(results, item)
				continue
			}
		}

		ok, tasksExceeded, _ := scraper.EnqueueAdventurerSearch(r.Header.Get("CF-Connecting-IP"), region, query, searchType)
		if tasksExceeded {
			results = append(results, adventurerSearchBatchItem{
				Query:      query,
				Status:     "rejected",
				HTTPStatus: http.StatusTooManyRequests,
				Error:      "You have exceeded the maximum number of concurrent tasks.",
			})
			stats["rejected"]++
			continue
		}

		results = append(results, adventurerSearchBatchItem{
			Query:      query,
			Status:     map[bool]string{true: "started", false: "pending"}[ok],
			HTTPStatus: http.StatusAccepted,
		})
		stats[map[bool]string{true: "started", false: "pending"}[ok]]++
	}

	w.Header().Set("X-Batch-Size", strconv.Itoa(len(req.Queries)))
	json.NewEncoder(w).Encode(adventurerSearchBatchResponse{
		Region:     region,
		SearchType: map[string]string{"1": "characterName", "2": "familyName"}[searchType],
		Results:    results,
		Stats:      stats,
	})
}
