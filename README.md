# Circuit Breaker

A small Go circuit breaker.

## Example

```go
func main() {
	cb := circuitbreaker.New(3, time.Second, time.Minute)

	err := cb.Do(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen) {
		...
	}
}
```
