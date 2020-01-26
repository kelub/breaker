package Breaker

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

// 1.最小原则实现
// 2.优化可拓展性 包括拆分出配置结构体，提取公共部分，提高扩展性,可测试性
// 3.添加 Close Breaker
// 例子 有个数据上报给第三方平台服务

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

type State int32

const (
	CLOSE int32 = iota
	HALFOPEN
	OPEN
)

var BreakerOpenErr = errors.New("Breaker is open!")
var BreakerErr = errors.New("Breaker state error!")

type HeadlerFunc func(ctx context.Context, req interface{}) (interface{}, error)

type Options struct {
	// Default 3
	MaxFailures int32
	// Default 3
	MaxSuccesses int32

	HeadlerFunc HeadlerFunc
	// Default 600 ms
	PollingTimer *time.Timer
	// Default 600 ms
	TimeOutInterval time.Duration
}

type Option func(options *Options)

func SetMaxFailures(maxFailures int32) Option {
	return func(options *Options) {
		options.MaxFailures = maxFailures
	}
}

func SetMaxSuccesses(maxSuccesses int32) Option {
	return func(options *Options) {
		options.MaxSuccesses = maxSuccesses
	}
}

func SetHeadlerFunc(headlerFunc HeadlerFunc) Option {
	return func(options *Options) {
		options.HeadlerFunc = headlerFunc
	}
}

func SetPollingTimer(pollingTimer *time.Timer) Option {
	return func(options *Options) {
		options.PollingTimer = pollingTimer
	}
}

func SetTimeOutInterval(timeOutInterval time.Duration) Option {
	return func(options *Options) {
		options.TimeOutInterval = timeOutInterval
	}
}

type Breaker struct {
	Options *Options

	state     int32
	failuers  int32
	successes int32

	// closeMu protects closed field
	closeMu sync.Mutex
	closed  bool
	// cancel
	cancel context.CancelFunc
}

func NewBreaker(f HeadlerFunc, setters ...Option) *Breaker {
	TimeOutInterval := 600 * time.Millisecond
	options := &Options{
		MaxFailures:     3,
		MaxSuccesses:    3,
		HeadlerFunc:     f,
		PollingTimer:    time.NewTimer(TimeOutInterval),
		TimeOutInterval: TimeOutInterval,
	}
	for _, stetter := range setters {
		stetter(options)
	}
	if options.HeadlerFunc == nil {
		return nil
	}
	b := &Breaker{
		Options: options,
	}
	return b
}

func (b *Breaker) Run(ctx context.Context, req interface{}) (interface{}, error) {
	v := atomic.LoadInt32(&b.state)
	switch v {
	case CLOSE:
		return b.CloseState(ctx, req)
	case HALFOPEN:
		return b.HalfOpenState(ctx, req)
	case OPEN:
		return b.OpenState(ctx, req)
	default:
		logrus.Errorf(BreakerErr.Error())
		return nil, BreakerErr
	}
}

func (b *Breaker) CloseState(ctx context.Context, req interface{}) (interface{}, error) {
	resp, err := b.Options.HeadlerFunc(ctx, req)
	if err != nil {
		for {
			f := atomic.LoadInt32(&b.failuers)
			if atomic.CompareAndSwapInt32(&b.failuers, f, f+1) {
				break
			}
		}
		if atomic.LoadInt32(&b.failuers) >= b.Options.MaxFailures {
			// CLOSE to OPEN
			b.setState(OPEN)
			b.Options.PollingTimer.Reset(b.Options.TimeOutInterval)
			logrus.Debugln("CLOSE to OPEN")
		}
	} else {
		// 重置失败次数
		for {
			f := atomic.LoadInt32(&b.failuers)
			if atomic.CompareAndSwapInt32(&b.failuers, f, CLOSE) {
				break
			}
		}
	}
	return resp, err
}

func (b *Breaker) HalfOpenState(ctx context.Context, req interface{}) (interface{}, error) {
	resp, err := b.Options.HeadlerFunc(ctx, req)
	if err != nil {
		// HALFOPEN to OPEN
		b.setState(HALFOPEN)
		b.Options.PollingTimer.Reset(b.Options.TimeOutInterval)
		logrus.Debugln("HALFOPEN to OPEN")
	} else {
		for {
			s := atomic.LoadInt32(&b.successes)
			if atomic.CompareAndSwapInt32(&b.successes, s, s+1) {
				break
			}
		}
		if atomic.LoadInt32(&b.successes) >= b.Options.MaxSuccesses {
			// HALFOPEN to CLOSE
			b.setState(CLOSE)
			logrus.Debugln("HALFOPEN to CLOSE")
		}
	}
	return resp, err
}

func (b *Breaker) OpenState(ctx context.Context, req interface{}) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.Options.PollingTimer.C:
		// OPEN to HALFOPEN
		b.setState(HALFOPEN)
		logrus.Debugln("OPEN to HALFOPEN")
		return b.HalfOpenState(ctx, req)
	default:
		return nil, BreakerOpenErr
	}
}

func (b *Breaker) Close() {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if b.closed {
		return
	} else {
		if b.cancel != nil {
			b.cancel()
		}
		b.closed = true
	}
}

func (b *Breaker) setState(state int32) {
	atomic.StoreInt32(&b.state, state)
}
