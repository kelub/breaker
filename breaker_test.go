package Breaker

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type Req struct {
	runTime time.Duration
	e       bool
}

func handleFunc(ctx context.Context, req interface{}) (interface{}, error) {
	r, ok := req.(*Req)
	if !ok {
		return nil, fmt.Errorf("error")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:

	}
	time.Sleep(r.runTime)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:

	}
	if r.e {
		return nil, fmt.Errorf("headle error")
	}
	return req, nil
}

func TestBreaker_Run(t *testing.T) {
	b := NewBreaker(handleFunc)
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	defer b.Close()
	_, err := b.Run(ctx, &Req{100 * time.Millisecond, false})
	fmt.Println(b)
	assert.Nil(t, err)
	_, err = b.Run(ctx, &Req{100 * time.Millisecond, false})
	fmt.Println(b)
	assert.Nil(t, err)

	for i := 0; i < 3; i++ {
		_, err = b.Run(context.TODO(), &Req{100 * time.Millisecond, true})
		fmt.Println(b)
		assert.NotNil(t, err)
	}
	_, err = b.Run(context.TODO(), &Req{100 * time.Millisecond, true})
	fmt.Println(b)
	assert.Equal(t, BreakerOpenErr, err)
}
