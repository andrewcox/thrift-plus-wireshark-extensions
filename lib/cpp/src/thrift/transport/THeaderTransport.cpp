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
#include <string>
#include <zlib.h>

using std::map;
using boost::shared_ptr;
using std::string;
using std::vector;

namespace apache { namespace thrift { namespace transport {

using namespace apache::thrift::protocol;
using namespace apache::thrift::util;
using apache::thrift::protocol::TBinaryProtocol;

// Gets username using getpwuid_r()
string getIdentity();

const string THeaderTransport::IDENTITY_HEADER = "identity";
const string THeaderTransport::ID_VERSION_HEADER = "id_version";
const string THeaderTransport::ID_VERSION = "1";
string THeaderTransport::s_identity = getIdentity();

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
    return httpTransport_->read(buf, len);
  }

  // We want to call TBufferBase's version here, because
  // TFramedTransport would try and call its own readFrame function
  return TBufferBase::readAll(buf, len);
}

uint32_t THeaderTransport::readSlow(uint8_t* buf, uint32_t len) {

  if (clientType == THRIFT_HTTP_CLIENT_TYPE) {
    return httpTransport_->read(buf, len);
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
    memcpy(rBuf_.get(), &szN, sizeof(szN));
    if (minFrameSize > 4) {
      transport_->readAll(rBuf_.get() + 4, minFrameSize - 4);
      setReadBuffer(rBuf_.get(), minFrameSize);
    } else {
      setReadBuffer(rBuf_.get(), 4);
    }
  } else if (sz == HTTP_MAGIC) {
    clientType = THRIFT_HTTP_CLIENT_TYPE;

    // Transports don't support readahead, so put back the szN we read
    // off the wire for httpTransport_ to read.  It was probably 'POST'.
    shared_ptr<TBufferedTransport> bufferedTrans(
      new TBufferedTransport(transport_));
    bufferedTrans->putBack(reinterpret_cast<uint8_t*>(&szN), sizeof(szN));
    httpTransport_ = shared_ptr<TTransport>(new THttpServer(bufferedTrans));
  } else {
    // Could be header format or framed. Check next uint32
    uint32_t magic_n;
    uint32_t magic;

    if (sz > MAX_FRAME_SIZE) {
      throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                "Header transport frame is too large");
    }

    allocateReadBuffer(sz);

    // We can use readAll here, because it would be an invalid frame otherwise
    transport_->readAll(reinterpret_cast<uint8_t*>(&magic_n), sizeof(magic_n));
    memcpy(rBuf_.get(), &magic_n, sizeof(magic_n));
    magic = ntohl(magic_n);

    if ((magic & TBinaryProtocol::VERSION_MASK) == TBinaryProtocol::VERSION_1) {
      // framed
      clientType = THRIFT_FRAMED_DEPRECATED;
      transport_->readAll(rBuf_.get() + 4, sz - 4);
      setReadBuffer(rBuf_.get(), sz);
    } else if (HEADER_MAGIC == (magic & HEADER_MASK)) {
      if (sz < 10) {
        throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                  "Header transport frame is too small");
      }

      transport_->readAll(rBuf_.get() + 4, sz - 4);

      // header format
      clientType = THRIFT_HEADER_CLIENT_TYPE;
      // flags
      flags = magic & FLAGS_MASK;
      // seqId
      uint32_t seqId_n;
      memcpy(&seqId_n, rBuf_.get() + 4, sizeof(seqId_n));
      seqId = ntohl(seqId_n);
      // header size
      uint16_t headerSize_n;
      memcpy(&headerSize_n, rBuf_.get() + 8, sizeof(headerSize_n));
      uint16_t headerSize = ntohs(headerSize_n);
      setReadBuffer(rBuf_.get(), sz);
      readHeaderFormat(headerSize, sz);
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


/**
 * Reads a string from ptr, taking care not to reach headerBoundary
 * Advances ptr on success
 *
 * @param   str                  output string
 * @throws  INVALID_FRAME_SIZE  if size of string exceeds boundary
 */
void readString(uint8_t* &ptr, /* out */ string &str,
                uint8_t const* headerBoundary) {
  int32_t strLen;

  uint32_t bytes = readVarint32(ptr, &strLen, headerBoundary);
  if (ptr + strLen > headerBoundary) {
    throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                              "Info header length exceeds header size");
  }
  ptr += bytes;
  str.assign(reinterpret_cast<const char*>(ptr), strLen);
  ptr += strLen;
}

void THeaderTransport::readHeaderFormat(uint16_t headerSize, uint32_t sz) {
  readTrans_.clear(); // Clear out any previous transforms.
  readHeaders_.clear(); // Clear out any previous headers.

  // skip over already processed magic(4), seqId(4), headerSize(2)
  uint8_t* ptr = reinterpret_cast<uint8_t*>(rBuf_.get() + 10);
  headerSize *= 4;
  const uint8_t* const headerBoundary = ptr + headerSize;
  if (headerSize > sz) {
    throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                              "Header size is larger than frame");
  }
  uint8_t* data = ptr + headerSize;
  ptr += readVarint16(ptr, &protoId, headerBoundary);
  int16_t numTransforms;
  ptr += readVarint16(ptr, &numTransforms, headerBoundary);

  uint16_t macSz = 0;

  // For now all transforms consist of only the ID, not data.
  for (int i = 0; i < numTransforms; i++) {
    int32_t transId;
    ptr += readVarint32(ptr, &transId, headerBoundary);

    if (transId == HMAC_TRANSFORM) {
      macSz = *ptr;
      *ptr = 0x00; // Blank to check mac.
      ++ptr;
    } else {
      readTrans_.push_back(transId);
    }
  }

  // Info headers
  while (ptr < headerBoundary) {
    uint32_t infoId;
    ptr += readVarint32(ptr, (int32_t*)&infoId, headerBoundary);

    if (infoId == 0) {
      // header padding
      break;
    }
    if (infoId >= infoIdType::END) {
      // cannot handle infoId
      break;
    }
    switch (infoId) {
      case infoIdType::KEYVALUE:
        // Process key-value headers
        uint32_t numKVHeaders;
        ptr += readVarint32(ptr, (int32_t*)&numKVHeaders, headerBoundary);
        // continue until we reach (padded) end of packet
        while (numKVHeaders-- && ptr < headerBoundary) {
          // format: key; value
          // both: length (varint32); value (string)
          string key, value;
          readString(ptr, key, headerBoundary);
          // value
          readString(ptr, value, headerBoundary);
          // save to headers
          readHeaders_[key] = value;
        }
        break;
    }
  }

  if (verifyCallback_) {
    string verify_data(reinterpret_cast<char*>(rBuf_.get()), sz - macSz);

    string mac(reinterpret_cast<char*>(rBuf_.get() + sz - macSz), macSz);
    ptr += macSz;
    if (!verifyCallback_(verify_data, mac)) {
      if (macSz > 0) {
        throw TTransportException(TTransportException::INVALID_STATE,
                                  "mac did not verify");
      } else {
        throw TTransportException(TTransportException::INVALID_STATE,
                                  "Client did not send a mac");
      }
    }
  }

  // Don't include the mac in the data to untransform
  sz -= macSz;

  // Untransform the data section.  rBuf will contain result.
  untransform(data, sz - (data - rBuf_.get())); // ignore header in size calc

  if (protoId == T_JSON_PROTOCOL && clientType != THRIFT_HTTP_CLIENT_TYPE) {
    throw TApplicationException(TApplicationException::UNSUPPORTED_CLIENT_TYPE,
                                "Client is trying to send JSON without HTTP");
  }
}

void THeaderTransport::untransform(uint8_t* ptr, uint32_t sz) {
  // Update the transform buffer size if needed
  resizeTransformBuffer();

  for (vector<uint16_t>::const_iterator it = readTrans_.begin();
       it != readTrans_.end(); ++it) {
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

/**
 * We may have updated the wBuf size, update the tBuf size to match.
 * Should be called in transform.
 *
 * The buffer should be slightly larger than write buffer size due to
 * compression transforms (that may slightly grow on small frame sizes)
 */
void THeaderTransport::resizeTransformBuffer() {
  if (tBufSize_ < wBufSize_ + DEFAULT_BUFFER_SIZE) {
    uint32_t new_size = wBufSize_ + DEFAULT_BUFFER_SIZE;
    uint8_t* new_buf = new uint8_t[new_size];
    tBuf_.reset(new_buf);
    tBufSize_ = new_size;
  }
}

void THeaderTransport::transform(uint8_t* ptr, uint32_t sz) {
  // TODO(davejwatson) look at doing these as stream operations on write
  // instead of memory buffer operations.  Would save a memcpy.

  // Update the transform buffer size if needed
  resizeTransformBuffer();

  for (vector<uint16_t>::const_iterator it = writeTrans_.begin();
       it != writeTrans_.end(); ++it) {
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
  // Set to anything except HTTP type so we don't flush again
  clientType = THRIFT_HEADER_CLIENT_TYPE;

  // Read the header and decide which protocol to go with
  readFrame(0);
}

uint32_t THeaderTransport::getWriteBytes() {
  return wBase_ - wBuf_.get();
}

/**
 * Writes a string to a byte buffer, as size (varint32) + string (non-null
 * terminated)
 * Automatically advances ptr to after the written portion
 */
void writeString(uint8_t* &ptr, const string& str) {
  uint32_t strLen = str.length();
  ptr += writeVarint32(strLen, ptr);
  memcpy(ptr, str.c_str(), strLen); // no need to write \0
  ptr += strLen;
}

void THeaderTransport::setHeader(const string& key, const string& value) {
  writeHeaders_[key] = value;
}

size_t THeaderTransport::getMaxWriteHeadersSize() const {
  size_t maxWriteHeadersSize = 0;
  THeaderTransport::StringToStringMap::const_iterator it;
  for (it = writeHeaders_.begin(); it != writeHeaders_.end(); ++it) {
    // add sizes of key and value to maxWriteHeadersSize
    // 2 varints32 + the strings themselves
    maxWriteHeadersSize += 5 + 5 + (it->first).length() +
      (it->second).length();
  }
  return maxWriteHeadersSize;
}

void THeaderTransport::clearHeaders() {
  writeHeaders_.clear();
}

string THeaderTransport::getPeerIdentity() {
  if (readHeaders_.find(IDENTITY_HEADER) != readHeaders_.end()) {
    if (readHeaders_[ID_VERSION_HEADER] == ID_VERSION) {
      return readHeaders_[IDENTITY_HEADER];
    }
  }
  return "";
}

void THeaderTransport::setIdentity(const string& identity) {
  this->identity = identity;
}

void THeaderTransport::flush()  {
  // Write out any data waiting in the write buffer.
  uint32_t haveBytes = getWriteBytes();

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

  if (haveBytes > MAX_FRAME_SIZE) {
    throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                              "Attempting to send frame that is too large");
  }

  if (clientType == THRIFT_HEADER_CLIENT_TYPE) {
    // header size will need to be updated at the end because of varints.
    // Make it big enough here for max varint size, plus 4 for padding.
    int headerSize = (2 + getNumTransforms()) * 5 + 4;
    // add approximate size of info headers
    headerSize += getMaxWriteHeadersSize();

    // Pkt size
    uint32_t maxSzHbo = headerSize + haveBytes // thrift header + payload
                        + 10;                  // common header section
    uint8_t* pkt = tBuf_.get();
    uint8_t* headerStart;
    uint8_t* headerSizePtr;
    uint8_t* pktStart = pkt;

    if (maxSzHbo > tBufSize_) {
      throw TTransportException(TTransportException::INVALID_FRAME_SIZE,
                                "Attempting to header frame that is too large");
    }

    uint32_t szHbo;
    uint32_t szNbo;
    uint16_t headerSizeN;

    // Fixup szHbo later
    pkt += sizeof(szNbo);
    uint16_t headerN = htons(HEADER_MAGIC >> 16);
    memcpy(pkt, &headerN, sizeof(headerN));
    pkt += sizeof(headerN);
    uint16_t flagsN = htons(flags);
    memcpy(pkt, &flagsN, sizeof(flagsN));
    pkt += sizeof(flagsN);
    uint32_t seqIdN = htonl(seqId);
    memcpy(pkt, &seqIdN, sizeof(seqIdN));
    pkt += sizeof(seqIdN);
    headerSizePtr = pkt;
    // Fixup headerSizeN later
    pkt += sizeof(headerSizeN);
    headerStart = pkt;

    pkt += writeVarint32(protoId, pkt);
    pkt += writeVarint32(getNumTransforms(), pkt);

    // For now, each transform is only the ID, no following data.
    for (vector<uint16_t>::const_iterator it = writeTrans_.begin();
         it != writeTrans_.end(); ++it) {
      pkt += writeVarint32(*it, pkt);
    }

    uint8_t* mac_loc = NULL;
    if (macCallback_) {
      pkt += writeVarint32(HMAC_TRANSFORM, pkt);
      mac_loc = pkt;
      *pkt = 0x00;
      pkt++;
    }

    // write info headers

    // Add in special flags
    if (identity.length() > 0) {
      writeHeaders_[IDENTITY_HEADER] = identity;
      writeHeaders_[ID_VERSION_HEADER] = ID_VERSION;
    }

    // for now only write kv-headers
    uint16_t headerCount = writeHeaders_.size();
    if (headerCount > 0) {
      pkt += writeVarint32(infoIdType::KEYVALUE, pkt);
      // Write key-value headers count
      pkt += writeVarint32(headerCount, pkt);
      // Write info headers
      map<string, string>::const_iterator it;
      for (it = writeHeaders_.begin(); it != writeHeaders_.end(); ++it) {
        writeString(pkt, it->first);  // key
        writeString(pkt, it->second); // value
      }
      writeHeaders_.clear();
    }

    // TODO(davejwatson) optimize this for writing twice/memcopy to pkt buffer.
    // See code in TBufferTransports

    // Fixups after varint size calculations
    headerSize = (pkt - headerStart);
    uint8_t padding = 4 - (headerSize % 4);
    headerSize += padding;

    // Pad out pkt with 0x00
    for (int i = 0; i < padding; i++) {
      *(pkt++) = 0x00;
    }

    // Pkt size
    szHbo = headerSize + haveBytes           // thrift header + payload
            + (headerStart - pktStart - 4);  // common header section
    headerSizeN = htons(headerSize / 4);
    memcpy(headerSizePtr, &headerSizeN, sizeof(headerSizeN));

    // hmac calculation should always be last.
    string hmac;
    if (macCallback_) {
      // TODO(davejwatson): refactor macCallback_ interface to take
      // several uint8_t buffers instead of string to avoid the extra copying.

      // Ignoring 4 bytes of framing.
      string headerData(reinterpret_cast<char*>(pktStart + 4),
                        szHbo - haveBytes);
      string data(reinterpret_cast<char*>(wBuf_.get()), haveBytes);
      hmac = macCallback_(headerData + data);
      *mac_loc = hmac.length(); // Set mac size.
      szHbo += hmac.length();
    }

    // Set framing size.
    szNbo = htonl(szHbo);
    memcpy(pktStart, &szNbo, sizeof(szNbo));

    outTransport_->write(pktStart, szHbo - haveBytes + 4 - hmac.length());
    outTransport_->write(wBuf_.get(), haveBytes);
    outTransport_->write(reinterpret_cast<const uint8_t*>(hmac.c_str()),
                         hmac.length());
  } else if (clientType == THRIFT_FRAMED_DEPRECATED) {
    uint32_t szHbo = (uint32_t)haveBytes;
    uint32_t szNbo = htonl(szHbo);

    outTransport_->write(reinterpret_cast<uint8_t*>(&szNbo), 4);
    outTransport_->write(wBuf_.get(), haveBytes);
  } else if (clientType == THRIFT_UNFRAMED_DEPRECATED) {
    outTransport_->write(wBuf_.get(), haveBytes);
  } else if (clientType == THRIFT_HTTP_CLIENT_TYPE) {
    httpTransport_->write(wBuf_.get(), haveBytes);
    httpTransport_->flush();
  } else {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "Unknown client type");
  }

  // Flush the underlying transport.
  outTransport_->flush();
}

string getIdentity() {
  struct passwd pwd;
  struct passwd *pwdbufp = NULL;
  long buflen = sysconf(_SC_GETPW_R_SIZE_MAX);
  boost::scoped_array<char> buf(new char[buflen]);
  int retval = getpwuid_r(getuid(), &pwd, buf.get(), buflen, &pwdbufp);
  if (pwdbufp) {
    return pwdbufp->pw_name;
  }
  return "";
}

}}} // apache::thrift::transport
