package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/runtime"
)

func TestAgentsCatalogIsOperatorOnly(t *testing.T) {
	srv := &Server{cfg: config.Config{CookieName: "sid"}, runtime: runtime.New(runtime.Config{})}
	productReq := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	productReq.AddCookie(&http.Cookie{Name: "sid", Value: srv.signCookie(labUserID)})
	productRec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(productRec, productReq)
	if productRec.Code != http.StatusNotFound {
		t.Fatalf("product /api/agents status=%d body=%s", productRec.Code, productRec.Body.String())
	}

	operatorReq := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	operatorRec := httptest.NewRecorder()
	srv.OperatorRoutes().ServeHTTP(operatorRec, operatorReq)
	if operatorRec.Code != http.StatusOK {
		t.Fatalf("operator /api/agents status=%d body=%s", operatorRec.Code, operatorRec.Body.String())
	}
	var payload struct {
		Drivers []struct {
			DriverID string `json:"driver_id"`
		} `json:"drivers"`
	}
	if err := json.Unmarshal(operatorRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode operator catalog: %v", err)
	}
	if len(payload.Drivers) != 3 ||
		payload.Drivers[0].DriverID != "claude_code" ||
		payload.Drivers[1].DriverID != "pi" ||
		payload.Drivers[2].DriverID != "sh" {
		t.Fatalf("unexpected operator catalog: %+v", payload.Drivers)
	}
}
