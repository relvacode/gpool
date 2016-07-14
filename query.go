package gpool

type jqMatcher func(j *JobState) bool

func stateMatcher(Status string) jqMatcher {
	return func(j *JobState) bool {
		if Status == "" || j.State == Status {
			return true
		}
		return false
	}
}
