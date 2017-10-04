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

#include <iostream>
#include <memory>
#include <string>
#include <sstream>

#include <gflags/gflags.h>

#include "arrow/ipc/arrow-grpc.h"
#include "arrow/ipc/json.h"
#include "arrow/ipc/reader.h"

DEFINE_int32(port, 50051, "Port to connect to.");
DEFINE_bool(json, false, "If true, output the results as json.");
DEFINE_bool(datasets, false, "If true, just ask the server for the supported datasets.");

namespace arrow {
namespace ipc {

class ScannerClient {
 public:
  ScannerClient(const char* address)
    : ScannerClient(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())) {
  }

  ScannerClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(flatbuf::ArrowProducer::NewStub(channel)) {}

  Status Datasets(std::vector<std::string>* result) {
    flatbuffers::grpc::MessageBuilder mb;
    auto request_offset = flatbuf::CreateScanRequest(mb);
    mb.Finish(request_offset);
    auto request_msg = mb.ReleaseMessage<flatbuf::DatasetsRequest>();
    flatbuffers::grpc::Message<flatbuf::DatasetsReply> response_msg;

    grpc::ClientContext context;
    auto status = stub_->Datasets(&context, request_msg, &response_msg);
    if (status.ok()) {
      const flatbuf::DatasetsReply* response = response_msg.GetRoot();
      for (auto dataset: *response->datasets()) {
        result->push_back(dataset->str());
      }
    }
    return GrpcUtil::FromGrpcStatus(status);
  }

  Status Scan(const std::string& request) {
    flatbuffers::grpc::MessageBuilder mb;
    auto request_offset = mb.CreateString(request);
    auto scan_request_offset = flatbuf::CreateScanRequest(mb, request_offset);
    mb.Finish(scan_request_offset);
    auto request_msg = mb.ReleaseMessage<flatbuf::ScanRequest>();

    grpc::ClientContext context;
    std::unique_ptr<GrpcStreamReader> reader = stub_->Scan(&context, request_msg);
    std::unique_ptr<MessageReader> msg_reader(new GrpcMessageReader(reader.get()));

    std::shared_ptr<RecordBatchReader> batch_reader;
    RETURN_NOT_OK(RecordBatchStreamReader::Open(std::move(msg_reader), &batch_reader));
    std::cerr << "Schema: " << batch_reader->schema()->ToString() << std::endl;

    std::unique_ptr<JsonWriter> json_writer;
    RETURN_NOT_OK(JsonWriter::Open(batch_reader->schema(), &json_writer));

    while (true) {
      std::shared_ptr<RecordBatch> batch;
      RETURN_NOT_OK(batch_reader->ReadNext(&batch));
      if (batch == nullptr) {
        std::cerr << "EOS" << std::endl;
        break;
      }
      std::cerr << "Got batch:" << batch->num_rows() << std::endl;
      if (FLAGS_json) {
        RETURN_NOT_OK(json_writer->WriteRecordBatch(*batch.get()));
      }
    }

    if (FLAGS_json) {
      std::string json;
      RETURN_NOT_OK(json_writer->Finish(&json));
      std::cout << json << std::endl;
    }
    return Status::OK();
  }

 private:
  std::unique_ptr<flatbuf::ArrowProducer::Stub> stub_;
};

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string address = "localhost:" + std::to_string(FLAGS_port);
  arrow::ipc::ScannerClient scanner(address.c_str());

  arrow::Status status;
  if (FLAGS_datasets) {
    std::vector<std::string> datasets;
    status = scanner.Datasets(&datasets);
    std::cout << "Datasets: " << std::endl;
    for (const std::string& dataset: datasets) {
      std::cout << "    " << dataset << std::endl;
    }
  } else {
    // Example client can take a request to specify what to request from the server.
    status = scanner.Scan(argc > 1 ? argv[1] : "");
  }
  if (!status.ok()) {
    std::cerr << "Scan failed. " << status << std::endl;
    return 1;
  }
  return 0;
}

