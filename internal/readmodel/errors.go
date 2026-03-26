package readmodel

import "errors"

var (
	// ErrTableNotFound indicates a requested fed_data table does not exist.
	ErrTableNotFound = errors.New("readmodel: table not found")
	// ErrColumnNotFound indicates a requested table column does not exist.
	ErrColumnNotFound = errors.New("readmodel: column not found")
	// ErrRowNotFound indicates a requested row does not exist.
	ErrRowNotFound = errors.New("readmodel: row not found")
)
