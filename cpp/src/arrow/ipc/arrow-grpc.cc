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

#include "arrow/ipc/arrow-grpc.h"

#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer-internal.h"
#include "arrow/util/logging.h"

#include "arrow/ipc/ArrowRpc_generated.h"

namespace arrow {
namespace ipc {

Status GrpcUtil::FromGrpcStatusInternal(const grpc::Status& status) {
  DCHECK(!status.ok());
  std::stringstream ss;
  ss << "gRPC failed (error code: " << status.error_code() << "): "
      << status.error_message();
  // TODO: switch on status.error_code to translate that as well as we can.
  return Status(StatusCode::IOError, ss.str());
}

grpc::Status GrpcUtil::ToGrpcStatusInternal(const Status& status) {
  DCHECK(status.ok());
  // TODO: switch on status.error_code to translate that as well as we can.
  return grpc::Status(grpc::StatusCode::INTERNAL, status.message());
}

Status GrpcUtil::RunServer(flatbuf::ArrowProducer::Service* service, int port,
      const char* host) {
  std::string address = std::string(host)  + ":" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cerr << "Server listening on " << address << std::endl;
  server->Wait();
  return Status::OK();
}

GrpcMessageReader::GrpcMessageReader(GrpcStreamReader* reader) : reader_(reader) { }

Status GrpcMessageReader::ReadNextMessage(std::unique_ptr<Message>* message) {
  if (reader_->Read(&response_msg_)) {
    const flatbuf::ScanReply* response = response_msg_.GetRoot();
    std::shared_ptr<Buffer> body = nullptr;
    if (response->message()->bodyLength() != 0) {
      if (!response->data()) {
        // TODO: add more diagnostic info.
        return Status(StatusCode::IOError,
            "Invalid rpc from server. RPC missing body.");
      }
      body = std::make_shared<Buffer>(
          response->data()->data(), response->data()->size());
    }
    Status status =  Message::Open(response->message(), body, message);
    return status;
  }

  // All done. This can either be EOS or an error.
  *message = nullptr;
  return GrpcUtil::FromGrpcStatus(reader_->Finish());
}

Status GrpcRecordBatchStreamWriter::Open(GrpcStreamWriter* writer,
    std::shared_ptr<GrpcRecordBatchStreamWriter>* out) {
  auto result = std::shared_ptr<GrpcRecordBatchStreamWriter>(
      new GrpcRecordBatchStreamWriter(writer));
  *out = result;
  return Status::OK();
}

GrpcRecordBatchStreamWriter::GrpcRecordBatchStreamWriter(GrpcStreamWriter* writer)
  : writer_(writer), schema_written_(false) {
}

void GrpcRecordBatchStreamWriter::set_memory_pool(MemoryPool* pool) {
  // No-op
}

void GrpcRecordBatchStreamWriter::SetSchema(const std::shared_ptr<Schema>& schema) {
  DCHECK(schema_.get() == nullptr);
  schema_ = schema;
}

Status GrpcRecordBatchStreamWriter::WriteSchema() {
  DCHECK(schema_.get() != nullptr) << "Must call SetSchema() first.";
  DCHECK(!schema_written_);
  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(mb_, *schema_.get(), nullptr, &fb_schema));
  auto msg_offset = CreateMessage(mb_,
      kCurrentMetadataVersion, flatbuf::MessageHeader_Schema,
      fb_schema.Union(), 0);
  auto reply_offset = CreateScanReply(mb_, msg_offset, 0);
  mb_.Finish(reply_offset);
  writer_->Write(mb_.ReleaseMessage<flatbuf::ScanReply>());
  return Status::OK();
}

Status GrpcRecordBatchStreamWriter::Close() {
  DCHECK(schema_.get() != nullptr); // Handle this. Just return is probably okay.
  if (!schema_written_) {
    RETURN_NOT_OK(WriteSchema());
    schema_written_ = true;
  }
  // TODO: close writer
  return Status::OK();
}

Status GrpcRecordBatchStreamWriter::WriteRecordBatch(const RecordBatch& batch,
                                                     bool allow_64bit) {
  DCHECK(schema_.get() != nullptr) << "Must call SetSchema() first.";
  if (!schema_written_) {
    RETURN_NOT_OK(WriteSchema());
    schema_written_ = true;
  }

  flatbuffers::Offset<flatbuf::Message> msg_offset;
  flatbuffers::Offset<flatbuffers::Vector<uint8_t>> data_offset;

  RETURN_NOT_OK(SerializeRecordBatchStream(
        batch, nullptr, mb_, &msg_offset, &data_offset));

  auto reply_offset = CreateScanReply(mb_, msg_offset, data_offset);
  mb_.Finish(reply_offset);
  writer_->Write(mb_.ReleaseMessage<flatbuf::ScanReply>());

  return Status::OK();
}

grpc::Status ArrowProducerServiceBase::Scan(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<flatbuf::ScanRequest>* request_msg,
      GrpcStreamWriter* writer) {
  const flatbuf::ScanRequest* scan_request = request_msg->GetRoot();
  std::shared_ptr<GrpcRecordBatchStreamWriter> batch_writer;
  Status status = GrpcRecordBatchStreamWriter::Open(writer, &batch_writer);
  if (!status.ok()) return GrpcUtil::ToGrpcStatus(status);
  return GrpcUtil::ToGrpcStatus(ScanImpl(scan_request, batch_writer.get()));
}

}  // namespace ipc
}  // namespace arrow
