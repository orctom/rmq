package queue

import "fmt"

type EOFError struct {
	name string
}

func NewEOFError(name string) *EOFError {
	return &EOFError{name: name}
}

func (e *EOFError) Error() string {
	return fmt.Sprintf("EOF: %s", e.name)
}

func IsEOFError(err error) bool {
	_, ok := err.(*EOFError)
	return ok
}
