#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

static int64_t MismatchesScalarFunction(Vector &result, const string_t str, string_t tgt) {
  int64_t num_characters_str = LengthFun::Length<string_t, int64_t>(str);
  int64_t num_characters_tgt = LengthFun::Length<string_t, int64_t>(tgt);

  if (num_characters_str != num_characters_tgt) {
    return -1;  // Expected error code: both strings must be of equal length
  }

  if (num_characters_str == 0) {
    return -0;  
  }
  
  idx_t mismatches = 0;
  
  for (idx_t pos = 0; pos < (idx_t)str.GetSize(); ++pos) {
    if (str.GetString()[pos] != tgt.GetString()[pos]) {
      mismatches++;
    }
  }
  return mismatches;
}

static void MismatchesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
  auto &str_vec = args.data[0];
  auto &tgt_vec = args.data[1];
  
  BinaryExecutor::Execute<string_t, string_t, int64_t>(
      str_vec, tgt_vec, result, args.size(),
      [&](string_t str, string_t tgt) { return MismatchesScalarFunction(result, str, tgt); });
}

void MismatchesFun::RegisterFunction(BuiltinFunctions &set) {
  set.AddFunction(
    ScalarFunction("mismatches", {
      LogicalType::VARCHAR, 
      LogicalType::VARCHAR
    }, 
    LogicalType::BIGINT, MismatchesFunction)); // Pointer to function implementation
}

} // namespace duckdb
