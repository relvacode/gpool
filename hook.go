package gpool

const (
	// Job completed
	HookDone = 1 << iota
	// Job has an error
	HookError
	// Job started
	HookStart
	// Job added
	HookAdd
)

// HookFn is a function to be called when a Job state triggers a set Pool Hook
type HookFn func(PoolJob)
