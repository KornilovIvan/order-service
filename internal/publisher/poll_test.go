package publisher

import (
	"encoding/json"
	"testing"
)

func TestEventJSONHasJobID(t *testing.T) {
	body, err := json.Marshal(Event{
		Event: EventOrderCreated, OrderID: 1, JobID: 10, CorrelationID: "c-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["event"] != "order.created" || got["order_id"] != float64(1) || got["job_id"] != float64(10) {
		t.Fatalf("event = %s", body)
	}
	if got["correlation_id"] != "c-1" {
		t.Fatalf("correlation_id = %v", got["correlation_id"])
	}
}
