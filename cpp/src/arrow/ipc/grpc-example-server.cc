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

#include <gflags/gflags.h>

#include "arrow/ipc/arrow-grpc.h"
#include "arrow/io/test-common.h"

DEFINE_int32(port, 50051, "Port to listen on.");

namespace flatbuf = org::apache::arrow::flatbuf;
namespace arrow {
namespace ipc {

static void MakeBatchArrays(const std::shared_ptr<Schema>& schema, const int num_rows,
                            std::vector<std::shared_ptr<Array>>* arrays) {
  std::vector<bool> is_valid;
  test::random_is_valid(num_rows, 0.25, &is_valid);

  std::vector<int8_t> v1_values;
  std::vector<int32_t> v2_values;

  test::randint<int8_t>(num_rows, 0, 100, &v1_values);
  test::randint<int32_t>(num_rows, 0, 100, &v2_values);

  std::shared_ptr<Array> v1;
  ArrayFromVector<Int8Type, int8_t>(is_valid, v1_values, &v1);

  std::shared_ptr<Array> v2;
  ArrayFromVector<Int32Type, int32_t>(is_valid, v2_values, &v2);

  static const int kBufferSize = 10;
  static uint8_t buffer[kBufferSize];
  static uint32_t seed = 0;
  StringBuilder string_builder;
  for (int i = 0; i < num_rows; ++i) {
    if (!is_valid[i]) {
      ASSERT_OK(string_builder.AppendNull());
    } else {
      test::random_ascii(kBufferSize, seed++, buffer);
      ASSERT_OK(string_builder.Append(buffer, kBufferSize));
    }
  }
  std::shared_ptr<Array> v3;
  ASSERT_OK(string_builder.Finish(&v3));

  arrays->emplace_back(v1);
  arrays->emplace_back(v2);
  arrays->emplace_back(v3);
}

static Status GeneratePrimitiveTypesSchema(GrpcRecordBatchStreamWriter* writer) {
  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false);
  auto f2 = field("f2", std::make_shared<Int32Type>());
  auto f3 = field("f3", std::make_shared<Int64Type>());
  auto f4 = field("f4", std::make_shared<UInt8Type>());
  auto f5 = field("f5", std::make_shared<UInt16Type>());
  auto f6 = field("f6", std::make_shared<UInt32Type>());
  auto f7 = field("f7", std::make_shared<UInt64Type>());
  auto f8 = field("f8", std::make_shared<FloatType>());
  auto f9 = field("f9", std::make_shared<DoubleType>(), false);
  auto f10 = field("f10", std::make_shared<BooleanType>());

  std::shared_ptr<Schema> schema(
      new Schema({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10}));
  writer->SetSchema(schema);
  return writer->Close();
}

static Status GenerateBasicData(GrpcRecordBatchStreamWriter* writer) {
  auto v1_type = int8();
  auto v2_type = int32();
  auto v3_type = utf8();
  auto schema = ::arrow::schema(
      {field("f1", v1_type), field("f2", v2_type), field("f3", v3_type)});
  writer->SetSchema(schema);

  const int nbatches = 3;
  std::vector<std::shared_ptr<RecordBatch>> batches;
  for (int i = 0; i < nbatches; ++i) {
    int num_rows = 5 + i * 5;
    std::vector<std::shared_ptr<Array>> arrays;

    MakeBatchArrays(schema, num_rows, &arrays);
    auto batch = std::make_shared<RecordBatch>(schema, num_rows, arrays);
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  return writer->Close();
}

class ExampleArrowService final : public ArrowProducerServiceBase {
 public:
  virtual grpc::Status Datasets(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<flatbuf::DatasetsRequest>* request_msg,
      flatbuffers::grpc::Message<flatbuf::DatasetsReply>* response_msg) override {
    std::vector<::flatbuffers::Offset<::flatbuffers::String>> offsets;
    offsets.push_back(mb_.CreateString("test-error"));
    offsets.push_back(mb_.CreateString("primitive-types"));
    offsets.push_back(mb_.CreateString("basic"));
    auto datasets_offset = flatbuf::CreateDatasetsReplyDirect(mb_, &offsets);

    mb_.Finish(datasets_offset);
    *response_msg = mb_.ReleaseMessage<flatbuf::DatasetsReply>();
    return grpc::Status::OK;
  }

  Status ScanImpl(const flatbuf::ScanRequest* scan_request,
                  GrpcRecordBatchStreamWriter* writer) override {
    const std::string& request = scan_request->request()->str();
    std::cerr << "request: " << request << std::endl;

    if (request == "test-error") {
      return Status(StatusCode::IOError, "Server intentionally returning error.");
    } else if (request == "primitive-types") {
      return GeneratePrimitiveTypesSchema(writer);
    } else if (request == "basic") {
      return GenerateBasicData(writer);
    }
    return GenerateBasicData(writer);
  }

 private:
  flatbuffers::grpc::MessageBuilder mb_;
};

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  arrow::ipc::ExampleArrowService service;
  arrow::Status status = arrow::ipc::GrpcUtil::RunServer(&service, FLAGS_port);
  if (status.ok()) return 0;
  std::cerr << "Could not start server. " << status << std::endl;
  return 1;
}
