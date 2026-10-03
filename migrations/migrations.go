package migrations

import "embed"

// Files contains the ordered SQL migrations compiled into the API binary.
//
//go:embed *.sql
var Files embed.FS
