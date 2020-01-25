package Breaker

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"sync/atomic"
	"time"
)

// 最小原则实现
// 例子 有个数据上报给第三方平台服务

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

type State int32

const (
	CLOSE int32 = 0
	HALFOPEN
	OPEN
)

var BreakerOpenErr = errors.New("Breaker is open!")
var BreakerErr = errors.New("Breaker state error!")

type HeadlerFunc func(ctx context.Context, req interface{}) (interface{}, error)

type Breaker struct {
	state     int32
	failuers  int32
	successes int32

	MaxFailures  int32
	MaxSuccesses int32

	headlerFunc     HeadlerFunc
	pollingTimer    *time.Timer
	TimeOutInterval time.Duration

	cancel context.CancelFunc
}

func NewBreaker(f HeadlerFunc) *Breaker {
	TimeOutInterval := 600 * time.Millisecond
	b := &Breaker{
		MaxFailures:     3,
		MaxSuccesses:    3,
		headlerFunc:     f,
		TimeOutInterval: TimeOutInterval,
		pollingTimer:    time.NewTimer(TimeOutInterval),
	}
	return b
}

func (b *Breaker) Run(ctx context.Context, req interface{}) (interface{}, error) {
	v := atomic.LoadInt32(&b.state)
	switch v {
	case 0:
		return b.CloseState(ctx, req)
	case 1:
		return b.HalfOpenState(ctx, req)
	case 2:
		return b.OpenState(ctx, req)
	default:
		logrus.Errorf(BreakerErr.Error())
		return nil, BreakerErr
	}
}

func (b *Breaker) CloseState(ctx context.Context, req interface{}) (interface{}, error) {
	resp, err := b.headlerFunc(ctx, req)
	if err != nil {
		for {
			f := atomic.LoadInt32(&b.failuers)
			if atomic.CompareAndSwapInt32(&b.failuers, f, f+1) {
				break
			}
		}
		if atomic.LoadInt32(&b.failuers) >= b.MaxFailures {
			// CLOSE to OPEN
			atomic.StoreInt32(&b.state, 2)
			b.pollingTimer.Reset(b.TimeOutInterval)
			logrus.Debugln("CLOSE to OPEN")
		}
	} else {
		// 重置失败次数
		for {
			f := atomic.LoadInt32(&b.failuers)
			if atomic.CompareAndSwapInt32(&b.failuers, f, 0) {
				break
			}
		}
	}
	return resp, err
}

func (b *Breaker) HalfOpenState(ctx context.Context, req interface{}) (interface{}, error) {
	resp, err := b.headlerFunc(ctx, req)
	if err != nil {
		// HALFOPEN to OPEN
		atomic.StoreInt32(&b.state, 1)
		b.pollingTimer.Reset(b.TimeOutInterval)
		logrus.Debugln("HALFOPEN to OPEN")
	} else {
		for {
			s := atomic.LoadInt32(&b.successes)
			if atomic.CompareAndSwapInt32(&b.successes, s, s+1) {
				break
			}
		}
		if atomic.LoadInt32(&b.successes) >= b.MaxSuccesses {
			// HALFOPEN to CLOSE
			atomic.StoreInt32(&b.state, 0)
			logrus.Debugln("HALFOPEN to CLOSE")
		}
	}
	return resp, err
}

func (b *Breaker) OpenState(ctx context.Context, req interface{}) (interface{}, error) {
	select {
	case <-b.pollingTimer.C:
		// OPEN to HALFOPEN
		atomic.StoreInt32(&b.state, 1)
		logrus.Debugln("OPEN to HALFOPEN")
		return b.HalfOpenState(ctx, req)
	default:
		return nil, BreakerOpenErr
	}
}
