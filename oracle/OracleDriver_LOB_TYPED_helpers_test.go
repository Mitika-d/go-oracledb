/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"bytes"
	"context"
	"io"
	"testing"
)

type typedLOBReader interface {
	ReadContext(context.Context, []byte) (int, error)
}

func readTypedLOBContents(t *testing.T, ctx context.Context, value typedLOBReader) []byte {
	t.Helper()
	var output bytes.Buffer
	buffer := make([]byte, 17)
	for {
		n, err := value.ReadContext(ctx, buffer)
		if n > 0 {
			_, _ = output.Write(buffer[:n])
		}
		if err == io.EOF {
			return output.Bytes()
		}
		if err != nil {
			t.Fatalf("read typed LOB: %v", err)
		}
	}
}
