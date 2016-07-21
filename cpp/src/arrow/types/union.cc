// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/types/union.h"

#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/type.h"

namespace arrow {

bool DenseUnionArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  if (null_count_ != arr->null_count()) { return false; }
  return RangeEquals(0, length_, 0, arr);
}

bool DenseUnionArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (Type::DENSE_UNION != arr->type_enum()) { return false; }
  const auto other = static_cast<DenseUnionArray*>(arr.get());

  int32_t i = start_idx;
  int32_t o_i = other_start_idx;
  for (size_t c = 0; c < other->children().size(); ++c) {
    for (int32_t e = 0; e < other->children()[c]->length(); ++e) {
      if (!children()[c]->RangeEquals(e, e + 1, e, other->children()[c])) { // FIXME(pcm): fix this
        return false;
      }
      i += 1;
      o_i += 1;
      if (i >= end_idx) {
        return true;
      }
    }
  }
  return false; // to make the compiler happy
}

Status DenseUnionArray::Validate() const {
  return Status::OK();
}

static inline std::string format_union(const std::vector<TypePtr>& child_types) {
  std::stringstream s;
  s << "union<";
  for (size_t i = 0; i < child_types.size(); ++i) {
    if (i) { s << ", "; }
    s << child_types[i]->ToString();
  }
  s << ">";
  return s.str();
}

std::string DenseUnionType::ToString() const {
  return format_union(child_types_);
}

std::string SparseUnionType::ToString() const {
  return format_union(child_types_);
}

}  // namespace arrow
