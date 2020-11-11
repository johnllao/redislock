package redismutex

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

type RedisMutex struct {
	conn     redis.Conn
	ext      int32
	rediskey string
	name     string
	uid      string
}

type LockState struct {
	Status string
	Err    error
}

const (
	KeyPrefix =     "STATIC:REDISLOCK:"
	Expiry =        1000
	SleepInterval = 100
)

var (
	DeleteScript = redis.NewScript(1, `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end`)

	TouchScript = redis.NewScript(1, `if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end`)
)

func NewRedisMutex(addr, name string) (*RedisMutex, error) {
	var err error
	var conn redis.Conn
	conn, err = redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	var uid = uniqueid()

	var k = KeyPrefix + name

	var m = &RedisMutex {
		conn:     conn,
		ext:      0,
		rediskey: k,
		name:     name,
		uid:      uid,
	}

	return m, nil
}

func(m *RedisMutex) RLock() error {
	var c = make(chan LockState)

	go func() {
		var err error
		var reply string

		reply, err = redis.String(m.conn.Do("GET", m.rediskey))
		for err == nil && reply != "" {
			time.Sleep(SleepInterval * time.Millisecond)
			reply, err = redis.String(m.conn.Do("GET", m.rediskey))
		}
		if err != redis.ErrNil {
			c <- LockState {
				Status: "FAIL",
				Err:    err,
			}
		}
		c <- LockState {
			Status: "OK",
			Err:    nil,
		}
	}()

	var s = <- c

	if s.Err != nil {
		return s.Err
	}

	if s.Status != "OK" {
		return errors.New("failed to acquire read lock. redis reply is not 'OK'")
	}

	return nil
}

func (m *RedisMutex) Lock() error {

	var c = make(chan LockState)

	go func() {
		var err error
		var reply string

		reply, err = redis.String(m.conn.Do("SET", m.rediskey, m.uid, "NX", "PX", Expiry))
		for err == redis.ErrNil {
			time.Sleep(SleepInterval * time.Millisecond)
			reply, err = redis.String(m.conn.Do("SET", m.rediskey, m.uid, "NX", "PX", Expiry))
		}
		if err != nil {
			c <- LockState {
				Status: "FAIL",
				Err:    err,
			}
		}
		c <- LockState {
			Status: reply,
			Err:    nil,
		}
	}()

	var s = <- c

	if s.Err != nil {
		return s.Err
	}

	if s.Status != "OK" {
		return errors.New("failed to acquire lock. redis reply is not 'OK'")
	}

	atomic.StoreInt32(&m.ext, 1)
	go func() {
		for {
			if ext := atomic.LoadInt32(&m.ext); ext == 0 {
				break
			}
			m.Extend()
			time.Sleep(SleepInterval * time.Millisecond)
		}
	}()

	return nil
}

func (m *RedisMutex) UnLock() error {
	atomic.StoreInt32(&m.ext, 0)
	r, err := redis.Int(DeleteScript.Do(m.conn, m.rediskey, m.uid, Expiry))
	if err != nil {
		return err
	}
	if r == 0 {
		return errors.New("unable to remove the lock")
	}
	return nil
}

func (m *RedisMutex) Extend() error {
	r, err := redis.Int(TouchScript.Do(m.conn, m.rediskey, m.uid, Expiry))
	if err != nil {
		return err
	}
	if r == 0 {
		return errors.New("timeout not set")
	}
	return nil
}

func (m *RedisMutex) Close() {
	if m.conn != nil {
		m.conn.Close()
	}
}

func uniqueid() string {
	var ts = make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(ts, time.Now().UnixNano())

	var r = make([]byte, 16)
	rand.Read(r)

	return hex.EncodeToString(append(ts, r...))
}
