package gpool

const (
	// Job completed
	HookDone = 1 << iota
	// Job started
	HookStart
	// Job added
	HookAdd
)

// HookFn is a function to be called when a Job state triggers a set Pool Hook
type HookFn func(Job)
