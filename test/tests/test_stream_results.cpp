#include "odbc_test_common.h"

#include <cstdint>
#include <vector>

using namespace odbc_test;

static const int64_t LARGE_ROW_COUNT = 20000000;
// About 8 TB of data, cannot be buffered in memory
static const std::string HUGE_QUERY = "SELECT i FROM range(1000000000000) t(i)";

static void ConnectWithParams(SQLHANDLE &env, SQLHANDLE &dbc, const std::string &params) {
	DRIVER_CONNECT_TO_DATABASE(env, dbc, "DSN=duckdbmemory;" + params);
}

TEST_CASE("Test streaming a large result to the end", "[odbc][stream]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	ConnectWithParams(env, dbc, "odbc_stream_results=true;");
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	const SQLULEN array_size = 1000;
	std::vector<SQLBIGINT> values(array_size);
	std::vector<SQLLEN> indicators(array_size);
	SQLULEN rows_fetched = 0;
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROW_ARRAY_SIZE)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
	                  ConvertToSQLPOINTER(array_size), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr (ROWS_FETCHED_PTR)", hstmt, SQLSetStmtAttr, hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
	                  &rows_fetched, 0);
	EXECUTE_AND_CHECK("SQLBindCol", hstmt, SQLBindCol, hstmt, 1, SQL_C_SBIGINT, values.data(), 0, indicators.data());

	// Array size does not divide the chunk size, so the rowsets cross the chunk boundaries
	const std::string query = "SELECT i FROM range(" + std::to_string(LARGE_ROW_COUNT) + ") t(i)";
	EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);

	int64_t expected = 0;
	bool all_match = true;
	SQLRETURN ret;
	while ((ret = SQLFetch(hstmt)) != SQL_NO_DATA) {
		ODBC_CHECK(ret, "SQLFetch", hstmt);
		for (SQLULEN i = 0; i < rows_fetched; i++) {
			all_match = all_match && values[i] == expected;
			expected++;
		}
	}
	REQUIRE(all_match);
	REQUIRE(expected == LARGE_ROW_COUNT);

	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test streaming the head of a huge result", "[odbc][stream]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	ConnectWithParams(env, dbc, "odbc_stream_results=true;");
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	SQLBIGINT value = -1;
	EXECUTE_AND_CHECK("SQLBindCol", hstmt, SQLBindCol, hstmt, 1, SQL_C_SBIGINT, &value, 0, nullptr);

	for (int run = 0; run < 2; run++) {
		// Only the fetched part of the result is computed
		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(HUGE_QUERY), SQL_NTS);
		for (int64_t i = 0; i < 100000; i++) {
			EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
			REQUIRE(value == i);
		}
		// Closing the cursor before reaching the end of the result aborts the query
		EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", hstmt, SQLFreeStmt, hstmt, SQL_CLOSE);
	}

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test streaming is lazy and can be disabled", "[odbc][stream]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt = SQL_NULL_HSTMT;

	// The error is raised only when the tail of the result is computed
	const std::string query =
	    "SELECT CASE WHEN i < 10000000 THEN i ELSE error('end of stream') END FROM range(20000000) t(i)";

	SECTION("Streaming enabled by default") {
		ConnectWithParams(env, dbc, "");
		EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

		EXECUTE_AND_CHECK("SQLExecDirect", hstmt, SQLExecDirect, hstmt, ConvertToSQLCHAR(query), SQL_NTS);
		// The head of the result is available
		EXECUTE_AND_CHECK("SQLFetch", hstmt, SQLFetch, hstmt);
		DATA_CHECK(hstmt, 1, "0");

		// The error is reported when fetching reaches the tail
		SQLRETURN ret;
		do {
			ret = SQLFetch(hstmt);
		} while (ret == SQL_SUCCESS);
		REQUIRE(ret == SQL_ERROR);
		std::string state;
		std::string message;
		ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
		REQUIRE(message.find("end of stream") != std::string::npos);
	}

	SECTION("Streaming disabled") {
		ConnectWithParams(env, dbc, "odbc_stream_results=false;");
		EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

		// The whole result is computed on execution
		SQLRETURN ret = SQLExecDirect(hstmt, ConvertToSQLCHAR(query), SQL_NTS);
		REQUIRE(ret == SQL_ERROR);
		std::string state;
		std::string message;
		ACCESS_DIAGNOSTIC(state, message, hstmt, SQL_HANDLE_STMT);
		REQUIRE(message.find("end of stream") != std::string::npos);
	}

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt, SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test streaming with multiple statements on the same connection", "[odbc][stream]") {
	SQLHANDLE env;
	SQLHANDLE dbc;
	HSTMT hstmt1 = SQL_NULL_HSTMT;
	HSTMT hstmt2 = SQL_NULL_HSTMT;

	ConnectWithParams(env, dbc, "odbc_stream_results=true;");
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt1, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt1);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", hstmt2, SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt2);

	const int64_t row_count = 100000;
	SQLBIGINT value1 = -1;
	SQLBIGINT value2 = -1;
	EXECUTE_AND_CHECK("SQLBindCol", hstmt1, SQLBindCol, hstmt1, 1, SQL_C_SBIGINT, &value1, 0, nullptr);
	EXECUTE_AND_CHECK("SQLBindCol", hstmt2, SQLBindCol, hstmt2, 1, SQL_C_SBIGINT, &value2, 0, nullptr);

	EXECUTE_AND_CHECK("SQLExecDirect", hstmt1, SQLExecDirect, hstmt1,
	                  ConvertToSQLCHAR("SELECT i FROM range(" + std::to_string(row_count) + ") t(i)"), SQL_NTS);
	for (int64_t i = 0; i < 10; i++) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt1, SQLFetch, hstmt1);
		REQUIRE(value1 == i);
	}

	// The open stream of the first statement must not prevent running queries on the second one
	EXECUTE_AND_CHECK("SQLPrepare", hstmt2, SQLPrepare, hstmt2,
	                  ConvertToSQLCHAR("SELECT i * 2 FROM range(" + std::to_string(row_count) + ") t(i)"), SQL_NTS);
	EXECUTE_AND_CHECK("SQLExecute", hstmt2, SQLExecute, hstmt2);
	for (int64_t i = 0; i < 10; i++) {
		EXECUTE_AND_CHECK("SQLFetch", hstmt2, SQLFetch, hstmt2);
		REQUIRE(value2 == i * 2);
	}

	// Both results can be read to the end
	int64_t expected1 = 10;
	while (SQLFetch(hstmt1) == SQL_SUCCESS) {
		REQUIRE(value1 == expected1);
		expected1++;
	}
	REQUIRE(expected1 == row_count);

	int64_t expected2 = 10;
	while (SQLFetch(hstmt2) == SQL_SUCCESS) {
		REQUIRE(value2 == expected2 * 2);
		expected2++;
	}
	REQUIRE(expected2 == row_count);

	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt1, SQLFreeHandle, SQL_HANDLE_STMT, hstmt1);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", hstmt2, SQLFreeHandle, SQL_HANDLE_STMT, hstmt2);
	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test invalid 'odbc_stream_results' option value", "[odbc][stream]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	EXECUTE_AND_CHECK("SQLAllocHandle (ENV)", nullptr, SQLAllocHandle, SQL_HANDLE_ENV, nullptr, &env);
	EXECUTE_AND_CHECK("SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)", nullptr, SQLSetEnvAttr, env, SQL_ATTR_ODBC_VERSION,
	                  ConvertToSQLPOINTER(SQL_OV_ODBC3), 0);
	EXECUTE_AND_CHECK("SQLAllocHandle (DBC)", nullptr, SQLAllocHandle, SQL_HANDLE_DBC, env, &dbc);

	SQLRETURN ret = SQLDriverConnect(dbc, nullptr, ConvertToSQLCHAR("DSN=duckdbmemory;odbc_stream_results=foo;"),
	                                 SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_COMPLETE);
	REQUIRE(ret == SQL_ERROR);

	SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	SQLFreeHandle(SQL_HANDLE_ENV, env);
}
