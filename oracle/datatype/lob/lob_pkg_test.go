/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and under any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below) to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the Software
** and the Larger Work(s), and to sublicense the foregoing rights on either these
** or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or substantial
** portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package lob

import (
	"fmt"
	"os"
	"testing"

	oracleTest "github.com/oracle/go-oracledb/v26/internal/tests"
)

func TestMain(m *testing.M) {
	if err := oracleTest.InitConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "InitConfig failed: %v\n", err)
		os.Exit(1)
	}
	TestEnvironement = oracleTest.TestEnvironement
	TestingConfig = oracleTest.TestingConfig
	DefaultTestConfig = oracleTest.DefaultTestConfig
	TestCategories = oracleTest.TestCategories
	os.Exit(m.Run())
}

type Version = oracleTest.Version
type TestConfig = oracleTest.TestConfig
type TestingEnvironment = oracleTest.TestingEnvironment

var (
	DefaultTestConfig *TestConfig
	TestEnvironement  TestingEnvironment
	TestingConfig     *TestConfig
	TestCategories    oracleTest.TestCategoryList
)

var testCases = []oracleTest.CategorizedTestCase{
	{Name: "TestBlobScanAndReadAllWithPrefetch", Categories: "unitary", Fn: TestBlobScanAndReadAllWithPrefetch},
	{Name: "TestTypedScannerRejectsWrongKindAndSource", Categories: "unitary", Fn: TestTypedScannerRejectsWrongKindAndSource},
	{Name: "TestTypedScannerNullAndExpiredLease", Categories: "unitary", Fn: TestTypedScannerNullAndExpiredLease},
	{Name: "TestPositionalReadsLeaveSequentialCursorUnchanged", Categories: "unitary", Fn: TestPositionalReadsLeaveSequentialCursorUnchanged},
	{Name: "TestLOBMetadataAndServerLifecycle", Categories: "unitary", Fn: TestLOBMetadataAndServerLifecycle},
	{Name: "TestReadAtShortReadAndWriteTo", Categories: "unitary", Fn: TestReadAtShortReadAndWriteTo},
	{Name: "TestWriteToRejectsNilWriter", Categories: "unitary", Fn: TestWriteToRejectsNilWriter},
	{Name: "TestClobReadAllAndWriteString", Categories: "unitary", Fn: TestClobReadAllAndWriteString},
	{Name: "TestNClobTextHelpersAndTemporaryConstructors", Categories: "unitary", Fn: TestNClobTextHelpersAndTemporaryConstructors},
	{Name: "TestUTF16PrefixByteAccounting", Categories: "unitary", Fn: TestUTF16PrefixByteAccounting},
	{Name: "TestCreateTemporaryBlobWithDedicatedConn", Categories: "unitary", Fn: TestCreateTemporaryBlobWithDedicatedConn},
	{Name: "TestDatabaseSQLRowsScanPublicBlob", Categories: "unitary", Fn: TestDatabaseSQLRowsScanPublicBlob},
	{Name: "TestLOBScan_BytesReadsAndCloses", Categories: "unitary", Fn: TestLOBScan_BytesReadsAndCloses},
	{Name: "TestLOBScan_TextReadsCharacterKindsAndCloses", Categories: "unitary", Fn: TestLOBScan_TextReadsCharacterKindsAndCloses},
	{Name: "TestLOBScan_RejectsNullAndWrongKind", Categories: "unitary", Fn: TestLOBScan_RejectsNullAndWrongKind},
	{Name: "TestLOBScan_WrongKindReturnsCloseError", Categories: "unitary", Fn: TestLOBScan_WrongKindReturnsCloseError},
	{Name: "TestLOBScan_RejectsTypedNilSource", Categories: "unitary", Fn: TestLOBScan_RejectsTypedNilSource},
	{Name: "TestLOBScan_ScanPropagatesReadErrors", Categories: "unitary", Fn: TestLOBScan_ScanPropagatesReadErrors},
	{Name: "TestLOBScan_ReadAllSizeErrors", Categories: "unitary", Fn: TestLOBScan_ReadAllSizeErrors},
	{Name: "TestLOBScan_ReadAllErrors", Categories: "unitary", Fn: TestLOBScan_ReadAllErrors},
}

func TestCategoryExecutor(t *testing.T) {
	oracleTest.RunCategoryExecutor(t, TestCategories, testCases)
}
