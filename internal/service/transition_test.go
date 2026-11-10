package service

import (
	"testing"

	"order-service/internal/model"
)

func TestTransitions(t *testing.T) {
	cases := []struct {
		status string
		act    action
		ok     bool
	}{
		{model.StatusCreated, actionConfirm, true},
		{model.StatusCreated, actionCancel, true},
		{model.StatusCreated, actionRetry, false},
		{model.StatusConfirmed, actionCancel, true},
		{model.StatusConfirmed, actionConfirm, false},
		{model.StatusFailed, actionRetry, true},
		{model.StatusFailed, actionCancel, true},
		{model.StatusFailed, actionConfirm, false},
		{model.StatusSubmittedToERP, actionCancel, false},
		{model.StatusCompleted, actionCancel, false},
		{model.StatusCancelled, actionCancel, false},
		{model.StatusCancelled, actionConfirm, false},
	}

	for _, tc := range cases {
		if got := allow(tc.status, tc.act); got != tc.ok {
			t.Errorf("allow(%s, %d) = %v, want %v", tc.status, tc.act, got, tc.ok)
		}
	}
}
