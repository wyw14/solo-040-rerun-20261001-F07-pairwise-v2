package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type acknowledgeInput struct {
	Note string `json:"note"`
}

func deviationsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := r.URL.Query().Get("chamber_id")
		if chamberID != "" {
			found := false
			for _, id := range runtime.Chambers {
				if id == chamberID {
					found = true
					break
				}
			}
			if !found {
				writeError(w, http.StatusNotFound, errors.New("chamber not found"))
				return
			}
		}
		writeJSON(w, http.StatusOK, runtime.Gates.Deviations().List(chamberID))
	}
}

func acknowledgeDeviationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		var input acknowledgeInput
		if r.ContentLength != 0 {
			if err := decodeJSON(r, &input); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
		}
		deviation, err := runtime.Gates.Deviations().Acknowledge(id, input.Note)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, deviation)
	}
}
