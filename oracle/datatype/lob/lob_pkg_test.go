/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
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
