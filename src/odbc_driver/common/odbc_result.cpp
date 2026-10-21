#include "odbc_result.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using duckdb::OdbcResult;

OdbcResult::OdbcResult(unique_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	D_ASSERT(result);
}

OdbcResult::OdbcResult(unique_ptr<QueryResultStream<>> stream_p) : stream(std::move(stream_p)) {
	D_ASSERT(stream);
}

duckdb::unique_ptr<OdbcResult> OdbcResult::Execute(PreparedStatement &stmt, vector<Value> &values,
                                                   bool stream_results) {
	if (!stream_results) {
		return make_uniq<OdbcResult>(stmt.Execute(values));
	}
	try {
		auto result = stmt.Submit(values);
		// The client may close the cursor before reading all the rows, that rolls back what the
		// statement wrote, so only the read-only queries are streamed
		if (result->HasError() || !StreamsByDefault(stmt.GetStatementType(), *result, false)) {
			result->Complete();
			return make_uniq<OdbcResult>(std::move(result));
		}
		return make_uniq<OdbcResult>(make_uniq<QueryResultStream<>>(std::move(result)));
	} catch (std::exception &ex) {
		return make_uniq<OdbcResult>(make_uniq<QueryResult>(ErrorData(ex)));
	}
}

void OdbcResult::Materialize() {
	if (!stream) {
		return;
	}
	try {
		auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), stream->GetTypes());
		ColumnDataAppendState append_state;
		collection->InitializeAppend(append_state);
		while (auto chunk = Fetch()) {
			collection->Append(append_state, *chunk);
		}
		if (stream->HasError()) {
			result = make_uniq<QueryResult>(stream->GetErrorObject());
		} else {
			result = make_uniq<QueryResult>(stream->GetStatementType(), stream->GetStatementProperties(),
			                                stream->GetNames(), std::move(collection), stream->GetClientProperties());
		}
	} catch (std::exception &ex) {
		result = make_uniq<QueryResult>(ErrorData(ex));
	}
	stream_chunk.reset();
	stream.reset();
}

void OdbcResult::ConsumeStreamChunk(DataChunk &target, idx_t count) {
	auto copy_count = MinValue(count, stream_chunk->size() - stream_chunk_offset);
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		VectorOperations::Copy(stream_chunk->data[col_idx], target.data[col_idx], stream_chunk_offset + copy_count,
		                       stream_chunk_offset, target.size());
	}
	target.SetCardinality(target.size() + copy_count);
	stream_chunk_offset += copy_count;
}

duckdb::unique_ptr<duckdb::DataChunk> OdbcResult::Fetch() {
	if (!stream) {
		return result->Fetch();
	}
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), stream->GetTypes());
	while (chunk->size() < STANDARD_VECTOR_SIZE) {
		if (!stream_chunk || stream_chunk_offset >= stream_chunk->size()) {
			stream_chunk = stream->Fetch();
			stream_chunk_offset = 0;
			if (!stream_chunk || stream_chunk->size() == 0) {
				stream_chunk.reset();
				break;
			}
		}
		ConsumeStreamChunk(*chunk, STANDARD_VECTOR_SIZE - chunk->size());
	}
	if (stream->HasError() || chunk->size() == 0) {
		return nullptr;
	}
	return chunk;
}

bool OdbcResult::HasError() const {
	return stream ? stream->HasError() : result->HasError();
}

const std::string &OdbcResult::GetError() const {
	return stream ? stream->GetError() : result->GetError();
}

void OdbcResult::SetError(ErrorData error) {
	if (stream) {
		stream->SetError(std::move(error));
	} else {
		result->SetError(std::move(error));
	}
}

const duckdb::vector<duckdb::LogicalType> &OdbcResult::GetTypes() const {
	return stream ? stream->GetTypes() : result->GetTypes();
}

const duckdb::vector<duckdb::Identifier> &OdbcResult::GetNames() const {
	return stream ? stream->GetNames() : result->GetNames();
}

const duckdb::StatementProperties &OdbcResult::GetStatementProperties() const {
	return stream ? stream->GetStatementProperties() : result->GetStatementProperties();
}
