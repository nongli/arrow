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

// Exposes additional APIs for serialization related functions that are not part of
// the exported API.

#ifndef ARROW_IPC_WRITER_INTERNAL_H
#define ARROW_IPC_WRITER_INTERNAL_H

#include "arrow/ipc/writer.h"
#include "arrow/ipc/Message_generated.h"

namespace flatbuffers {
  class FlatBufferBuilder;
}

namespace flatbuf = org::apache::arrow::flatbuf;

namespace arrow {
namespace ipc {

/// \brief Write record batch to a flatbuf
///
/// \param[in] batch the record batch to write
/// \param[in] flatbuf builder
/// \param[out] Resulting offset for the serialized message header.
/// \param[out] Resulting offset for the serialized buffer data.
/// \return Status
Status SerializeRecordBatchStream(const RecordBatch& batch, MemoryPool* pool,
    flatbuffers::FlatBufferBuilder& fbb,
    ::flatbuffers::Offset<flatbuf::Message>* msg_out,
    ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>>* data_out);
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_WRITER_INTERNAL_H
