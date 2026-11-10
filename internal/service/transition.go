package service

import "order-service/internal/model"

// action — команда, которую клиент просит выполнить над заказом.
type action int

const (
	actionConfirm action = iota
	actionCancel
	actionRetry
	actionResend
)

// allowed — таблица переходов. Статуса нет в карте или команды нет в статусе:
// переход запрещён, UPDATE не выполняется.
//
//	created          → confirm, cancel
//	confirmed        → cancel, resend
//	failed           → retry, cancel
//	submitted_to_erp → ничего
//	completed        → ничего
//	cancelled        → ничего
var allowed = map[string]map[action]struct{}{
	model.StatusCreated: {
		actionConfirm: {},
		actionCancel:  {},
	},
	model.StatusConfirmed: {
		actionCancel: {},
		actionResend: {},
	},
	model.StatusFailed: {
		actionRetry:  {},
		actionCancel: {},
	},
}

func allow(status string, act action) bool {
	_, ok := allowed[status][act]
	return ok
}
