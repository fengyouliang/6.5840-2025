package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l        string // lock name
	clientID string // unique client identifier

}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		l:        l,
		clientID: kvtest.RandValue(8),
	}
	return lk

}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		currentOwner, version, err := lk.ck.Get(lk.l)

		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.l, lk.clientID, 0)
			if err == rpc.OK {
				//
				return
			}
			// 在当前client调用get之后put之前，已经有其他client put成功，重试
			continue
		} else if err == rpc.OK {
			if currentOwner == lk.clientID {
				// re entrant
				return
			} else if currentOwner == "" {
				// KVServer没有delete，用空串表示已解锁
				err = lk.ck.Put(lk.l, lk.clientID, version)
				if err == rpc.OK {
					// success
					return
				}
				// 当前client put之前已经有其他client put成功了
				continue
			}
			// 被其他client锁住了
			continue
		} else {
			// 目前的实现KVServer指挥返回上面两中状态
		}
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		currentOwner, version, err := lk.ck.Get(lk.l)

		if err == rpc.ErrNoKey {
			return
		} else if err == rpc.OK {
			if currentOwner == lk.clientID {
				err = lk.ck.Put(lk.l, "", version) // unlock
				if err == rpc.OK {
					return
				}
				continue
			} else {
				return
			}
		}
	}

}
