package gpool

const (
	HookDone = 1 << iota
	HookError
	HookStart
	HookAdd
)

// HookFn is a function to be called when a Job state triggers a set Pool Hook
type HookFn func(PoolJob)
