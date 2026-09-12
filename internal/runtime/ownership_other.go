//go:build !linux

package runtime

import "errors"

var ErrAlreadyRunning = errors.New("already_running")
var errUnsupported = errors.New("runtime ownership requires Linux")

type Ownership struct{}

func Acquire(string) (*Ownership, error) { return nil, errUnsupported }
func (*Ownership) Path() string          { return "" }
func (*Ownership) Close() error          { return errUnsupported }
