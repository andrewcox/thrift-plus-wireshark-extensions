/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef THRIFT_TRANSPORT_THEADERTRANSPORT_H_
#define THRIFT_TRANSPORT_THEADERTRANSPORT_H_ 1

#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"
#include "thrift/lib/cpp/protocol/TProtocolTypes.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/transport/THttpServer.h"
#include "thrift/lib/cpp/transport/TTransport.h"
#include "thrift/lib/cpp/transport/TVirtualTransport.h"

#include <bitset>
#include "boost/scoped_array.hpp"

// Don't include the unknown client.
#define CLIENT_TYPES_LEN 4

enum CLIENT_TYPE {
  THRIFT_HEADER_CLIENT_TYPE = 0,
  THRIFT_FRAMED_DEPRECATED = 1,
  THRIFT_UNFRAMED_DEPRECATED = 2,
  THRIFT_HTTP_CLIENT_TYPE = 3,
  THRIFT_UNKNOWN_CLIENT_TYPE = 4,
};

namespace apache { namespace thrift { namespace transport {

using namespace apache::thrift::protocol;

class THttpHeaderServer : public THttpServer {
 public:

  THttpHeaderServer(boost::shared_ptr<TTransport> transport) :
      THttpServer(transport)
    {
    }

  void refill() {
    // POST - because we read this earlier, need to put it back.
    *((uint32_t*)httpBuf_) = 0x54534F50;
    httpBufLen_ += 4;
    THttpTransport::refill();
  }
};

/**
 * Header transport. All writes go into an in-memory buffer until flush is
 * called, at which point the transport writes the length of the entire
 * binary chunk followed by the data payload. This allows the receiver on the
 * other end to always do fixed-length reads.
 *
 * Subclass TFramedTransport because most of the read/write methods are similar
 * and need similar buffers.  Major changes are readFrame & flush.
 */
class THeaderTransport
    : public TVirtualTransport<THeaderTransport, TFramedTransport> {
 public:

  static const int DEFAULT_BUFFER_SIZE = 512u;

  /// Use default buffer sizes.
  THeaderTransport(const boost::shared_ptr<TTransport> transport)
    : transport_(transport)
    , protoId(T_COMPACT_PROTOCOL)
    , clientType(THRIFT_HEADER_CLIENT_TYPE)
    , httpTransport_(transport)
    , tBufSize_(wBufSize_ + DEFAULT_BUFFER_SIZE)
    , tBuf_(new uint8_t[tBufSize_])
  {
    initBuffers();
    initSupportedClients(NULL);
  }

  THeaderTransport(const boost::shared_ptr<TTransport> transport,
                   std::bitset<CLIENT_TYPES_LEN> const* clientTypes)
    : transport_(transport)
    , protoId(T_COMPACT_PROTOCOL)
    , clientType(THRIFT_HEADER_CLIENT_TYPE)
    , httpTransport_(transport)
    , tBufSize_(wBufSize_ + DEFAULT_BUFFER_SIZE)
    , tBuf_(new uint8_t[tBufSize_])
  {
    initBuffers();
    initSupportedClients(clientTypes);
  }

  THeaderTransport(const boost::shared_ptr<TTransport> transport, uint32_t sz,
                   std::bitset<CLIENT_TYPES_LEN> const* clientTypes)
    : transport_(transport)
    , protoId(T_COMPACT_PROTOCOL)
    , clientType(THRIFT_HEADER_CLIENT_TYPE)
    , httpTransport_(transport)
    , tBufSize_(wBufSize_ + DEFAULT_BUFFER_SIZE)
    , tBuf_(new uint8_t[tBufSize_])
  {
    initBuffers();
    initSupportedClients(clientTypes);
  }

  void open() {
    transport_->open();
  }

  bool isOpen() {
    return transport_->isOpen();
  }

  bool peek() {
    return (this->rBase_ < this->rBound_) || transport_->peek();
  }

  void close() {
    flush();
    transport_->close();
  }

  virtual uint32_t readSlow(uint8_t* buf, uint32_t len);
  virtual uint32_t readAll(uint8_t* buf, uint32_t len);

  virtual void flush();

  boost::shared_ptr<TTransport> getUnderlyingTransport() {
    return transport_;
  }

  /*
   * TVirtualTransport provides a default implementation of readAll().
   * We want to use the TBufferBase version instead.
   */
  using TBufferBase::readAll;

  uint16_t getProtocolId() const;
  void setProtocolId(uint16_t protoId) { this->protoId = protoId; }

  void resetProtocol();

  /**
   * We know we got a packet in header format here, try to parse the header
   *
   * @param headerSize size of the header portion
   * @param sz Size of the whole message, including header
   */
  void readHeaderFormat(uint16_t headerSize, uint32_t sz);

  /**
   * Untransform the data based on the recieved header flags
   * On conclusion of function, setReadBuffer is called with the
   * untransformed data.
   *
   * @param ptr ptr to data
   * @param size of data
   */
  void untransform(uint8_t* ptr, uint32_t sz);

  /**
   * Transform the data based on our write transform flags
   * At conclusion of function the write buffer is set to the
   * transformed data.
   *
   * @param ptr Ptr to data to transform
   * @param sz Size of data buffer
   */
  void transform(uint8_t* ptr, uint32_t sz);

  uint16_t getNumTransforms() const { return write_trans.size(); }

  void setTransform(uint16_t transId) { write_trans.push_back(transId); }

  enum TRANSFORMS {
    ZLIB_TRANSFORM = 0x01,
  };

 protected:

  std::bitset<CLIENT_TYPES_LEN> supported_clients;

  uint32_t readInt32(uint8_t** ptr, uint8_t* boundary);

  void initSupportedClients(std::bitset<CLIENT_TYPES_LEN> const*);

  /**
   * Reads a frame of input from the underlying stream.
   *
   * Returns true if a frame was read successfully, or false on EOF.
   * (Raises a TTransportException if EOF occurs after a partial frame.)
   */
  bool readFrame(uint32_t minFrameSize);

  void allocateReadBuffer(uint32_t sz);
  uint32_t getWriteBytes();

  void initBuffers() {
    setReadBuffer(NULL, 0);
    setWriteBuffer(wBuf_.get(), wBufSize_);
  }

  boost::shared_ptr<TTransport> transport_;

  // 0 and 16th bits must be 0 to differentiate from framed & unframed
  static const uint32_t HEADER_MAGIC = 0x0FFF0000;
  static const uint32_t HEADER_MASK = 0xFFFF0000;
  static const uint32_t HEADER_SIZE_MASK = 0x0000FFFF;
  static const uint32_t HTTP_MAGIC = 0x504F5354; // POST

  static const uint32_t MAX_FRAME_SIZE = 0x3FFFFFFF;

  int16_t protoId;
  uint16_t clientType;
  uint32_t seqId;

  std::vector<uint16_t> read_trans;
  std::vector<uint16_t> write_trans;

  THttpHeaderServer httpTransport_;

  // Buffers to use for transform processing
  uint32_t tBufSize_;
  boost::scoped_array<uint8_t> tBuf_;
};

/**
 * Wraps a transport into a header one.
 *
 */
class THeaderTransportFactory : public TTransportFactory {
 public:
  THeaderTransportFactory() {}

  virtual ~THeaderTransportFactory() {}

  /**
   * Wraps the transport into a header one.
   */
  virtual boost::shared_ptr<TTransport>
  getTransport(boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TTransport>(
      new THeaderTransport(trans, &clientTypes));
  }

private:
  std::bitset<CLIENT_TYPES_LEN> clientTypes;
};

}}} // apache::thrift::transport

#endif // #ifndef THRIFT_TRANSPORT_THEADERTRANSPORT_H_
