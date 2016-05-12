package gpool

const (
	HookDone = 1 << iota
	HookError
	HookStart
	HookAdd
)

type HookFn func(PoolJob)
