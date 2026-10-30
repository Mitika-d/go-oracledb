/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** See the LICENSE file for license terms.
 */

package oracle

import internallob "github.com/oracle/go-oracledb/v26/internal/lob"

// BindBlob marks a byte slice as a BLOB bind. A plain []byte keeps the driver's
// normal binary bind behavior. BindBlob shares its backing array, so callers
// must not modify the slice until ExecContext or QueryContext returns.
type BindBlob = internallob.BindBlob

// BindClob marks a string as a CLOB bind. The string must contain valid UTF-8;
// the driver validates it while preparing the streamed bind.
type BindClob = internallob.BindClob

// BindNClob marks a string as an NCLOB bind. The string must contain valid
// UTF-8; the driver validates it while preparing the streamed bind.
type BindNClob = internallob.BindNClob
