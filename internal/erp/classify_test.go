package erp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSubmitClassifiesStatus(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		wantID string
		perm   bool
		temp   bool
	}{
		{name: "200", status: http.StatusOK, body: `{"erp_order_id":"ERP-1"}`, wantID: "ERP-1"},
		{name: "409", status: http.StatusConflict, body: `{"erp_order_id":"ERP-1"}`, wantID: "ERP-1"},
		{name: "422", status: http.StatusUnprocessableEntity, body: `{"error":"rejected"}`, perm: true},
		{name: "400", status: http.StatusBadRequest, body: `{"error":"bad"}`, perm: true},
		{name: "503", status: http.StatusServiceUnavailable, body: `down`, temp: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Idempotency-Key") == "" {
					t.Errorf("missing Idempotency-Key")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			id, err := New(srv.URL, time.Second).Submit(context.Background(), "key-1", SubmitRequest{
				OrderID: 1, CustomerID: 1, Items: []Item{{ProductID: "1", Qty: 1}},
			})
			if id != tc.wantID {
				t.Fatalf("erp_order_id = %q, want %q", id, tc.wantID)
			}
			if tc.perm && !errors.Is(err, ErrPermanent) {
				t.Fatalf("error = %v, want permanent", err)
			}
			if tc.temp && !errors.Is(err, ErrTemporary) {
				t.Fatalf("error = %v, want temporary", err)
			}
			if !tc.perm && !tc.temp && err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
		})
	}
}
