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

#ifndef ARROW_IPC_ARROW_GRPC_H
#define ARROW_IPC_ARROW_GRPC_H

#include <iostream>
#include <memory>
#include <string>
#include <sstream>

#include <grpc++/grpc++.h>

#include "arrow/status.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/writer.h"

#include "arrow/ipc/ArrowRpc.grpc.fb.h"

namespace flatbuf = org::apache::arrow::flatbuf;

typedef grpc::ClientReader<flatbuffers::grpc::Message<flatbuf::ScanReply>>
    GrpcStreamReader;
typedef grpc::ServerWriter<flatbuffers::grpc::Message<flatbuf::ScanReply>>
    GrpcStreamWriter;

namespace arrow {
namespace ipc {

/// \brief Utilities for gRPC
class GrpcUtil {
 public:
  static Status FromGrpcStatus(const grpc::Status& status) {
    // Inline fast path.
    if (status.ok()) return Status::OK();
    return FromGrpcStatusInternal(status);
  }

  static grpc::Status ToGrpcStatus(const Status& status) {
    // Inline fast path.
    if (status.ok()) return grpc::Status::OK;
    return ToGrpcStatusInternal(status);
  }

  // Starts up a producer server listening on host:port. This function does not
  // return unless there is an error.
  static Status RunServer(flatbuf::ArrowProducer::Service* service, int port,
      const char* host = "0.0.0.0");

 private:
  static Status FromGrpcStatusInternal(const grpc::Status& status);
  static grpc::Status ToGrpcStatusInternal(const Status& status);
};

/// \brief Implementation of MessageReader that reads messages via gRPC
class GrpcMessageReader : public MessageReader {
 public:
  explicit GrpcMessageReader(GrpcStreamReader* reader);
  virtual ~GrpcMessageReader() = default;
  Status ReadNextMessage(std::unique_ptr<Message>* message) override;

 private:
  GrpcStreamReader* reader_; // unowned
  flatbuffers::grpc::Message<flatbuf::ScanReply> response_msg_;
};

/// \brief Writer which outputs stream formatted message directly to the gRPC output
/// channel
class GrpcRecordBatchStreamWriter : public RecordBatchStreamWriter {
 public:
  virtual ~GrpcRecordBatchStreamWriter() = default;

  /// Sets the schema for this writer. Must be set before any of the other APIs are
  /// called.
  void SetSchema(const std::shared_ptr<Schema>& schema);

  /// Create a new writer from gRPC writer and schema.
  ///
  /// \param[in] gRPC writer to output to
  /// \param[out] out the created stream writer
  /// \return Status
  static Status Open(GrpcStreamWriter* writer,
                     std::shared_ptr<GrpcRecordBatchStreamWriter>* out);

  Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit = false) override;
  Status Close() override;
  void set_memory_pool(MemoryPool* pool) override;

 private:
  GrpcRecordBatchStreamWriter(GrpcStreamWriter* writer);

  /// Writes the schema to the output channel.
  Status WriteSchema();

  // Grpc channel to write to.
  GrpcStreamWriter* writer_;

  // True if schema is already written.
  bool schema_written_;

  std::shared_ptr<Schema> schema_;

  // Reused builder object for output messages.
  flatbuffers::grpc::MessageBuilder mb_;
};

/// \brief Base class for gRPC servers. This class handles some common connection
/// related logic.
class ArrowProducerServiceBase : public flatbuf::ArrowProducer::Service {
 public:
  virtual ~ArrowProducerServiceBase() = default;

  virtual grpc::Status Datasets(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<flatbuf::DatasetsRequest>* request_msg,
      flatbuffers::grpc::Message<flatbuf::DatasetsReply>* response_msg) override = 0;

  /// Subclasses should not need to override this but instead ScanImpl().
  virtual grpc::Status Scan(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<flatbuf::ScanRequest>* request_msg,
      GrpcStreamWriter* writer) override;

 protected:
  // Must be implmented by subclass. Implementation should process 'request' and generate
  // output to writer.
  virtual Status ScanImpl(const flatbuf::ScanRequest* request,
      GrpcRecordBatchStreamWriter* writer) = 0;

  flatbuffers::grpc::MessageBuilder mb_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_ARROW_GRPC_H
