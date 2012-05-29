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

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TProtocolTypes.h>
#include "THeaderTransport.h"
#include <thrift/util/VarintUtils.h>

#include <algorithm>
#include <bitset>
#include <cassert>
#include <zlib.h>

using std::string;
using std::vector;

namespace apache { namespace thrift { namespace transport {

using namespace apache::thrift::protocol;
using namespace apache::thrift::util;
using apache::thrift::protocol::TBinaryProtocol;

uint32_t THeaderTransport::readInt32(uint8_t** ptr, uint8_t* boundary) {
  if (*ptr + 4 > boundary) {
    throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                  "Tried to read past header");
  }
  uint32_t ret = ntohl((*reinterpret_cast<uint32_t*>(*ptr)));
  (*ptr) += 4;
  return ret;
}

void THeaderTransport::initSupportedClients(std::bitset<CLIENT_TYPES_LEN>
                                            const* clients) {
  if (clients) {
    supported_clients = *clients;
  }

  // Let's always support Header
  supported_clients[THRIFT_HEADER_CLIENT_TYPE] = true;
}

uint32_t THeaderTransport::readAll(uint8_t* buf, uint32_t len) {
  if (clientType == THRIFT_HTTP_CLIENT_TYPE) {
    return httpTransport_.read(buf, len);
  }

  // We want to call TBufferBase's version here, because
  // TFramedTransport would try and call its own readFrame function
  return TBufferBase::readAll(buf, len);
}

uint32_t THeaderTransport::readSlow(uint8_t* buf, uint32_t len) {

  if (clientType == THRIFT_HTTP_CLIENT_TYPE) {
    return httpTransport_.read(buf, len);
  }

  if (clientType == THRIFT_UNFRAMED_DEPRECATED) {
    return transport_->read(buf, len);
  }

  return TFramedTransport::readSlow(buf, len);
}

uint16_t THeaderTransport::getProtocolId() const {
  if (clientType == THRIFT_HEADER_CLIENT_TYPE) {
    return protoId;
  } else {
    return T_BINARY_PROTOCOL; // Assume other transports use TBinary
  }
}

void THeaderTransport::allocateReadBuffer(uint32_t sz) {
  if (sz > rBufSize_) {
    rBuf_.reset(new uint8_t[sz]);
    rBufSize_ = sz;
  }
}

bool THeaderTransport::readFrame(uint32_t minFrameSize) {
  // szN is network byte order of sz
  uint32_t szN;
  uint32_t sz;

  // Read the size of the next frame.
  // We can't use readAll(&sz, sizeof(sz)), since that always throws an
  // exception on EOF.  We want to throw an exception only if EOF occurs after
  // partial size data.
  uint32_t sizeBytesRead = 0;
  while (sizeBytesRead < sizeof(szN)) {
    uint8_t* szp = reinterpret_cast<uint8_t*>(&szN) + sizeBytesRead;
    uint32_t bytesRead = transport_->read(szp, sizeof(szN) - sizeBytesRead);
    if (bytesRead == 0) {
      if (sizeBytesRead == 0) {
        // EOF before any data was read.
        return false;
      } else {
        // EOF after a partial frame header.  Raise an exception.
        throw TTransportException(TTransportException::END_OF_FILE,
                                  "No more data to read after "
                                  "partial frame header.");
      }
    }
    sizeBytesRead += bytesRead;
  }

  sz = ntohl(szN);

  allocateReadBuffer(minFrameSize + 4);

  if ((sz & TBinaryProtocol::VERSION_MASK) == TBinaryProtocol::VERSION_1) {
    // unframed
    clientType = THRIFT_UNFRAMED_DEPRECATED;
    *reinterpret_cast<uint32_t*>(rBuf_.get()) = szN;
    if (minFrameSize > 4) {
      transport_->readAll(rBuf_.get() + 4, minFrameSize - 4);
    }
    setReadBuffer(rBuf_.get(), minFrameSize);
  } else if (sz == HTTP_MAGIC) {
    clientType = THRIFT_HTTP_CLIENT_TYPE;
    *reinterpret_cast<uint32_t*>(rBuf_.get()) = szN;
    setReadBuffer(rBuf_.get(), 4);
  } else {
    // Could be header format or framed. Check next uint32
    uint32_t magic_n;
    uint32_t magic;

    allocateReadBuffer(sz);

    // We can use readAll here, because it would be an invalid frame otherwise
    transport_->readAll(reinterpret_cast<uint8_t*>(&magic_n), sizeof(magic_n));

    magic = ntohl(magic_n);
    if ((magic & TBinaryProtocol::VERSION_MASK) == TBinaryProtocol::VERSION_1) {
      // framed
      clientType = THRIFT_FRAMED_DEPRECATED;
      if (sz > MAX_FRAME_SIZE) {
        throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                  "Header transport frame was too large");
      }
      *reinterpret_cast<uint32_t*>(rBuf_.get()) = magic_n;
      transport_->readAll(rBuf_.get() + 4, sz - 4);
      setReadBuffer(rBuf_.get(), sz);
    } else if (HEADER_MAGIC == (magic & HEADER_MASK)) {
      // header format
      clientType = THRIFT_HEADER_CLIENT_TYPE;
      uint16_t headerSize = magic & HEADER_SIZE_MASK;
      if (sz > MAX_FRAME_SIZE) {
        throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                  "Header transport frame was too large");
      }
      transport_->readAll(rBuf_.get(), sz - 4);
      setReadBuffer(rBuf_.get(), sz - 4);
      readHeaderFormat(headerSize, sz - 4);
    } else {
      clientType = THRIFT_UNKNOWN_CLIENT_TYPE;
      throw TTransportException(TTransportException::BAD_ARGS,
                                "Could not detect client transport type");
    }
  }

  if (!supported_clients[clientType]) {
    throw TApplicationException(TApplicationException::UNSUPPORTED_CLIENT_TYPE,
                                "Transport does not support this client type");
  }

  return true;
}

void THeaderTransport::readHeaderFormat(uint16_t headerSize, uint32_t sz) {
  read_trans.clear(); // Clear out any previous transforms.
  uint8_t* ptr = reinterpret_cast<uint8_t*>(rBuf_.get());
  headerSize *= 4;
  uint8_t* headerBoundary = ptr + headerSize;
  if (headerSize > sz) {
    throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                              "Header size is larger than frame");
  }
  seqId = readInt32(&ptr, headerBoundary);
  ptr += readVarint16(ptr, &protoId, headerBoundary);
  int16_t headerCount;
  ptr += readVarint16(ptr, &headerCount, headerBoundary);

  // For now all transforms consist of only the ID, not data.
  for (int i = 0; i < headerCount; i++) {
    int32_t transId;
    ptr += readVarint32(ptr, &transId, headerBoundary);
    read_trans.push_back(transId);
  }

  // Skip over the Info headers.  No info headers currently.

  // Untransform the data section.  rBuf will contain result.
  untransform(rBuf_.get() + headerSize, sz - headerSize);

  if (protoId == T_JSON_PROTOCOL && clientType != THRIFT_HTTP_CLIENT_TYPE) {
    throw TApplicationException(TApplicationException::UNSUPPORTED_CLIENT_TYPE,
                                "Client is trying to send JSON without HTTP");
  }
}

void THeaderTransport::untransform(uint8_t* ptr, uint32_t sz) {
  for (vector<uint16_t>::const_iterator it = read_trans.begin();
       it != read_trans.end(); ++it) {
    const uint16_t transId = *it;

    if (transId == ZLIB_TRANSFORM) {
      z_stream stream;
      int err;

      stream.next_in = ptr;
      stream.avail_in = sz;

      // Setting these to 0 means use the default free/alloc functions
      stream.zalloc = (alloc_func)0;
      stream.zfree = (free_func)0;
      stream.opaque = (voidpf)0;
      err = inflateInit(&stream);
      if (err != Z_OK) {
        throw TApplicationException(TApplicationException::MISSING_RESULT,
                                    "Error while zlib deflateInit");
      }
      stream.next_out = tBuf_.get();
      stream.avail_out = tBufSize_;
      err = inflate(&stream, Z_FINISH);
      if (err != Z_STREAM_END || stream.avail_out == 0) {
        throw TApplicationException(TApplicationException::MISSING_RESULT,
                                    "Error while zlib deflate");
      }
      sz = stream.total_out;

      err = inflateEnd(&stream);
      if (err != Z_OK) {
        throw TApplicationException(TApplicationException::MISSING_RESULT,
                                    "Error while zlib deflateEnd");
      }

      memcpy(ptr, tBuf_.get(), sz);
    } else {
      throw TApplicationException(TApplicationException::MISSING_RESULT,
                                "Unknown transform");
    }
  }

  setReadBuffer(ptr, sz);
}

void THeaderTransport::transform(uint8_t* ptr, uint32_t sz) {
  // TODO(davejwatson) look at doing these as stream operations on write
  // instead of memory buffer operations.  Would save a memcpy.

  for (vector<uint16_t>::const_iterator it = write_trans.begin();
       it != write_trans.end(); ++it) {
    const uint16_t transId = *it;

    if (transId == ZLIB_TRANSFORM) {
      z_stream stream;
      int err;

      stream.next_in = ptr;
      stream.avail_in = sz;

      stream.zalloc = (alloc_func)0;
      stream.zfree = (free_func)0;
      stream.opaque = (voidpf)0;
      err = deflateInit(&stream, Z_DEFAULT_COMPRESSION);
      if (err != Z_OK) {
        throw TTransportException(TTransportException::CORRUPTED_DATA,
                                  "Error while zlib deflateInit");
      }
      stream.next_out = tBuf_.get();
      stream.avail_out = tBufSize_;
      err = deflate(&stream, Z_FINISH);
      if (err != Z_STREAM_END || stream.avail_out == 0) {
        throw TTransportException(TTransportException::CORRUPTED_DATA,
                                  "Error while zlib deflate");
      }
      sz = stream.total_out;

      err = deflateEnd(&stream);
      if (err != Z_OK) {
        throw TTransportException(TTransportException::CORRUPTED_DATA,
                                  "Error while zlib deflateEnd");
      }

      memcpy(ptr, tBuf_.get(), sz);
    } else {
      throw TTransportException(TTransportException::CORRUPTED_DATA,
                                "Unknown transform");
    }
  }

  wBase_ = wBuf_.get() + sz;
}

void THeaderTransport::resetProtocol() {
  // HTTP requires a response on one-way.
  if (clientType == THRIFT_HTTP_CLIENT_TYPE) {
    flush();
  }

  // Set to anything except HTTP type so we don't flush again
  clientType = THRIFT_HEADER_CLIENT_TYPE;

  // Read the header and decide which protocol to go with
  readFrame(0);
}

uint32_t THeaderTransport::getWriteBytes() {
  return wBase_ - wBuf_.get();
}

void THeaderTransport::flush()  {
  // Write out any data waiting in the write buffer.
  uint32_t haveBytes = getWriteBytes();
  uint8_t* pktStart = NULL;

  if (clientType == THRIFT_HEADER_CLIENT_TYPE) {
    transform(wBuf_.get(), haveBytes);
    haveBytes = getWriteBytes(); // transform may have changed the size
  }

  // Note that we reset wBase_ prior to the underlying write
  // to ensure we're in a sane state (i.e. internal buffer cleaned)
  // if the underlying write throws up an exception
  wBase_ = wBuf_.get();

  if (protoId == T_JSON_PROTOCOL && clientType != THRIFT_HTTP_CLIENT_TYPE) {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "Trying to send JSON without HTTP");
  }

  if (haveBytes > MAX_FRAME_SIZE ||
      haveBytes > tBufSize_) {
    throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                              "Attempting to send frame that is too large");
  }

  if (clientType == THRIFT_HEADER_CLIENT_TYPE) {
    // header size will need to be updated at the end because of varints.
    // Make it big enough here for max varint size, plus 4 for padding.
    int headerSize = 4 + (2 + getNumTransforms()) * 5 + 4;
    uint32_t maxSzHbo = headerSize + haveBytes + 4; // Pkt size
    uint8_t* pkt = tBuf_.get();
    uint8_t* headerPtr;
    pktStart = pkt;

    if (maxSzHbo > tBufSize_) {
      throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                "Attempting to header frame that is too large");
    }

    // Fixup szHbo later
    pkt += 4;
    *reinterpret_cast<uint16_t*>(pkt) = htons(HEADER_MAGIC >> 16);
    pkt += 2;
    headerPtr = pkt;
    // Fixup headerPtr later
    pkt += 2;
    *reinterpret_cast<uint32_t*>(pkt) = htonl(seqId);
    pkt += 4;

    pkt += writeVarint32(protoId, pkt);
    pkt += writeVarint32(getNumTransforms(), pkt);

    // For now, each transform is only the ID, no following data.
    for (vector<uint16_t>::const_iterator it = write_trans.begin();
         it != write_trans.end(); ++it) {
      pkt += writeVarint32(*it, pkt);
    }

    // TODO(davejwatson) optimize this for writing twice/memcopy to pkt buffer.
    // See code in TBufferTransports

    // Fixups after varint size calculations
    headerSize = (pkt - pktStart) - 8;
    uint8_t padding = 4 - (headerSize % 4);
    headerSize += padding;

    // Pad out pkt with 0x00
    for (int i = 0; i < padding; i++) {
      *(pkt++) = 0x00;
    }

    uint32_t szHbo = headerSize + haveBytes + 4; // Pkt size
    *reinterpret_cast<uint32_t*>(pktStart) = htonl(szHbo);
    *reinterpret_cast<uint16_t*>(headerPtr) = htons(headerSize / 4);

    transport_->write(pktStart, szHbo - haveBytes + 4);
    transport_->write(wBuf_.get(), haveBytes);
  } else if (clientType == THRIFT_FRAMED_DEPRECATED) {
    int32_t szHbo = haveBytes;
    uint8_t *pkt = tBuf_.get();
    pktStart = pkt;

    *reinterpret_cast<uint32_t*>(pkt) = (int32_t)htonl((uint32_t)szHbo);
    pkt += 4;

    transport_->write(pktStart, szHbo - haveBytes + 4);
    transport_->write(wBuf_.get(), haveBytes);
  } else if (clientType == THRIFT_UNFRAMED_DEPRECATED) {
    pktStart = wBuf_.get();
    transport_->write(pktStart, haveBytes);
  } else if (clientType == THRIFT_HTTP_CLIENT_TYPE) {
    httpTransport_.write(wBuf_.get(), haveBytes);
    httpTransport_.flush();
  } else {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "Unknown client type");
  }

  // Flush the underlying transport.
  transport_->flush();
}

}}} // apache::thrift::transport
