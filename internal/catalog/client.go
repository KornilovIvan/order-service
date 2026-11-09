// Package catalog — клиент сервиса каталога товаров.
package catalog

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	catalogv1 "order-service/internal/gen/catalog/v1"
)

var ErrNotFound = errors.New("catalog: product not found")

// Client — gRPC-клиент каталога.
type Client struct {
	conn *grpc.ClientConn
	api  catalogv1.CatalogClient
}

// New создаёт клиента. Соединение устанавливается лениво, при первом вызове.
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("catalog: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, api: catalogv1.NewCatalogClient(conn)}, nil
}

// GetPrice возвращает цену товара десятичной строкой ("100.00").
func (c *Client) GetPrice(ctx context.Context, productID string) (string, error) {
	resp, err := c.api.GetProduct(ctx, &catalogv1.GetProductRequest{Id: productID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("catalog: get product %q: %w", productID, err)
	}
	return resp.GetProduct().GetPrice(), nil
}

// Close закрывает соединение с каталогом.
func (c *Client) Close() error {
	return c.conn.Close()
}
