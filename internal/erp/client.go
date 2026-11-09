// Package erp — клиент внешней ERP-системы.
package erp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	// ErrPermanent — ERP отвергла заказ (400/422): повторять бессмысленно.
	ErrPermanent = errors.New("erp: permanent failure")
	// ErrTemporary — временный сбой (сеть, таймаут, 408, 429, 5xx): можно повторить.
	ErrTemporary = errors.New("erp: temporary failure")
)

type Item struct {
	ProductID string `json:"product_id"`
	Qty       int    `json:"qty"`
}

// SubmitRequest — тело POST /orders в ERP.
type SubmitRequest struct {
	OrderID    int64  `json:"order_id"`
	CustomerID int64  `json:"customer_id"`
	Items      []Item `json:"items"`
}

// Client — HTTP-клиент ERP.
type Client struct {
	baseURL string
	http    *http.Client
}

// New создаёт клиента. timeout ограничивает весь запрос целиком.
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// Submit отправляет заказ в ERP.
//
//	200 — успех, возвращается номер заказа в ERP;
//	409 — дубль по Idempotency-Key, считается успехом (номер может быть пустым);
//	400/422 — ErrPermanent; остальное (408/429/5xx/сеть/таймаут) — ErrTemporary.
func (c *Client) Submit(ctx context.Context, idempotencyKey string, req SubmitRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("erp: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/orders", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("erp: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTemporary, err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))

	switch resp.StatusCode {
	case http.StatusOK:
		var ok struct {
			ERPOrderID string `json:"erp_order_id"`
		}
		if err := json.Unmarshal(payload, &ok); err != nil {
			return "", fmt.Errorf("%w: invalid response body: %v", ErrTemporary, err)
		}
		return ok.ERPOrderID, nil

	case http.StatusConflict:
		// Заказ уже принят ранее с этим ключом — повтор безопасен.
		var dup struct {
			ERPOrderID string `json:"erp_order_id"`
		}
		_ = json.Unmarshal(payload, &dup)
		return dup.ERPOrderID, nil

	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "", fmt.Errorf("%w: status %d: %s", ErrPermanent, resp.StatusCode, strings.TrimSpace(string(payload)))

	default:
		return "", fmt.Errorf("%w: status %d: %s", ErrTemporary, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
}
