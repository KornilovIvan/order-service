package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposesERPCounters(t *testing.T) {
	rec := httptest.NewRecorder()
	NewOrders(nil).Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"erp_payload_prepare_errors_total",
		"erp_submit_errors_total",
	} {
		if !strings.Contains(body, name) {
			t.Fatalf("/metrics missing %s:\n%s", name, body)
		}
	}
}
