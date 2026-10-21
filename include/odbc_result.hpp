#ifndef ODBC_RESULT_HPP
#define ODBC_RESULT_HPP

#include "duckdb.hpp"
#include "duckdb/main/query_result_stream.hpp"

namespace duckdb {

//! Result of a statement execution, either materialized in memory or streamed from the engine.
//! An open stream holds the connection, no other query can run on it until the stream is
//! drained or closed, see Materialize().
class OdbcResult {
public:
	explicit OdbcResult(unique_ptr<QueryResult> result_p);
	explicit OdbcResult(unique_ptr<QueryResultStream<>> stream_p);

	//! Executes the prepared statement with the given values, streams the result when 'stream_results'
	//! is enabled and the statement is a read-only query
	static unique_ptr<OdbcResult> Execute(PreparedStatement &stmt, vector<Value> &values, bool stream_results);

	bool IsStreaming() const {
		return stream != nullptr;
	}

	//! Drains the rest of the stream into memory and releases the connection, the rows already
	//! fetched from the stream are not affected
	void Materialize();

	//! Returns full chunks of STANDARD_VECTOR_SIZE rows, except the last one, both for the streamed and
	//! materialized results. Returns nullptr at the end of the result or on error.
	unique_ptr<DataChunk> Fetch();

	bool HasError() const;
	const string &GetError() const;
	void SetError(ErrorData error);
	const vector<LogicalType> &GetTypes() const;
	const vector<Identifier> &GetNames() const;
	const StatementProperties &GetStatementProperties() const;

private:
	//! Copies up to 'count' rows not yet consumed from the stream chunk into the target chunk
	void ConsumeStreamChunk(DataChunk &target, idx_t count);

private:
	unique_ptr<QueryResult> result;
	unique_ptr<QueryResultStream<>> stream;
	// the chunk fetched from the stream that is not yet fully consumed, the chunks
	// coming from the stream can be of any size and are re-chunked
	unique_ptr<DataChunk> stream_chunk;
	idx_t stream_chunk_offset = 0;
};

} // namespace duckdb

#endif // ODBC_RESULT_HPP
