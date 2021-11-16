package entity_test

import "time"

type Test2 struct {
	String string    `spanner:"String" pk:"1"`
	Bytes  time.Time `spanner:"Bytes"`
}
