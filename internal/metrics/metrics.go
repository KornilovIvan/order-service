package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	ErrorInvalidDeliveryDate = "invalid_delivery_date"
	ErrorPermanent           = "permanent"
	ErrorTemporary           = "temporary"
)

var (
	// PayloadPrepareErrors — Submit не вызывался: тело не прошло проверку.
	PayloadPrepareErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "erp_payload_prepare_errors_total",
		Help: "ERP payload failed validation before Submit",
	}, []string{"order_type", "error_type"})

	// SubmitErrors — Submit вызывался и вернул ошибку.
	SubmitErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "erp_submit_errors_total",
		Help: "ERP Submit returned an error",
	}, []string{"order_type", "error_type"})
)

func init() {
	PayloadPrepareErrors.WithLabelValues("preorder", ErrorInvalidDeliveryDate)
	SubmitErrors.WithLabelValues("preorder", ErrorPermanent)
	SubmitErrors.WithLabelValues("preorder", ErrorTemporary)
	SubmitErrors.WithLabelValues("regular", ErrorPermanent)
	SubmitErrors.WithLabelValues("regular", ErrorTemporary)
}

func PrepareError(orderType, errorType string) {
	PayloadPrepareErrors.WithLabelValues(orderType, errorType).Inc()
}

func SubmitError(orderType, errorType string) {
	SubmitErrors.WithLabelValues(orderType, errorType).Inc()
}
