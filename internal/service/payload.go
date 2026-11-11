package service

import (
	"errors"
	"strings"
	"time"

	"order-service/internal/erp"
	"order-service/internal/model"
)

const deliveryDateLayout = "2006-01-02"

var errInvalidDeliveryDate = errors.New("invalid delivery_date")

func buildSubmit(order model.Order) erp.SubmitRequest {
	req := erp.SubmitRequest{
		OrderID:   order.ID,
		OrderType: order.OrderType,
	}
	if order.OrderType == model.OrderTypePreorder && order.DeliveryDate != nil {
		d := order.DeliveryDate.Format(deliveryDateLayout)
		req.DeliveryDate = &d
	}
	for _, item := range order.Items {
		req.Items = append(req.Items, erp.Item{
			ProductID:    erp.ProductID(item.ProductID),
			Qty:          item.Qty,
			PriceAtOrder: item.PriceAtOrder,
		})
	}
	return req
}

// validateERPPayload — проверка тела до Submit. Дата preorder: YYYY-MM-DD без T.
func validateERPPayload(req erp.SubmitRequest) error {
	if req.OrderType != model.OrderTypePreorder {
		return nil
	}
	if req.DeliveryDate == nil {
		return errInvalidDeliveryDate
	}
	date := strings.TrimSpace(*req.DeliveryDate)
	if date == "" || strings.Contains(date, "T") {
		return errInvalidDeliveryDate
	}
	parsed, err := time.Parse(deliveryDateLayout, date)
	if err != nil || parsed.Format(deliveryDateLayout) != date {
		return errInvalidDeliveryDate
	}
	return nil
}
