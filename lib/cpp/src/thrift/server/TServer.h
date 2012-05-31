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

#ifndef _THRIFT_SERVER_TSERVER_H_
#define _THRIFT_SERVER_TSERVER_H_ 1

#include <thrift/TProcessor.h>
#include <thrift/transport/TServerTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/concurrency/Thread.h>

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace server {

using apache::thrift::TProcessor;
using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TProtocolFactory;
using apache::thrift::protocol::TDuplexProtocolFactory;
using apache::thrift::protocol::TDualProtocolFactory;
using apache::thrift::protocol::TSingleProtocolFactory;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TTransportFactory;
using apache::thrift::transport::TDuplexTransportFactory;
using apache::thrift::transport::TDualTransportFactory;
using apache::thrift::transport::TSingleTransportFactory;

/**
 * Virtual interface class that can handle events from the server core. To
 * use this you should subclass it and implement the methods that you care
 * about. Your subclass can also store local data that you may care about,
 * such as additional "arguments" to these methods (stored in the object
 * instance's state).
 */
class TServerEventHandler {
 public:

  virtual ~TServerEventHandler() {}

  /**
   * Called before the server begins.
   */
  virtual void preServe() {}

  /**
   * Called when a new client has connected and is about to being processing.
   */
  virtual void* createContext(boost::shared_ptr<TProtocol> input,
                              boost::shared_ptr<TProtocol> output) {
    (void)input;
    (void)output;
    return NULL;
  }

  /**
   * Called when a client has finished request-handling to delete server
   * context.
   */
  virtual void deleteContext(void* serverContext,
                             boost::shared_ptr<TProtocol>input,
                             boost::shared_ptr<TProtocol>output) {
    (void)serverContext;
    (void)input;
    (void)output;
  }

  /**
   * Called when a client is about to call the processor.
   */
  virtual void processContext(void* serverContext,
                              boost::shared_ptr<TTransport> transport) {
    (void)serverContext;
    (void)transport;
}

 protected:

  /**
   * Prevent direct instantiation.
   */
  TServerEventHandler() {}

};

/**
 * Thrift server.
 *
 */
class TServer : public concurrency::Runnable {
 public:

  virtual ~TServer() {}

  virtual void serve() = 0;

  virtual void stop() {}

  // Allows running the server as a Runnable thread
  virtual void run() {
    serve();
  }

  boost::shared_ptr<TProcessorFactory> getProcessorFactory() {
    return processorFactory_;
  }

  boost::shared_ptr<TServerTransport> getServerTransport() {
    return serverTransport_;
  }

  boost::shared_ptr<TDuplexTransportFactory> getDuplexTransportFactory() {
    return duplexTransportFactory_;
  }

  boost::shared_ptr<TDuplexProtocolFactory> getDuplexProtocolFactory() {
    return duplexProtocolFactory_;
  }

  boost::shared_ptr<TServerEventHandler> getEventHandler() {
    return eventHandler_;
  }

protected:
  template<typename ProcessorFactory>
  TServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
          THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)):
    processorFactory_(processorFactory) {
    setTransportFactory(boost::shared_ptr<TTransportFactory>(
                          new TTransportFactory()));
    setProtocolFactory(boost::shared_ptr<TBinaryProtocolFactory>(
                         new TBinaryProtocolFactory()));
  }

  template<typename Processor>
  TServer(const boost::shared_ptr<Processor>& processor,
          THRIFT_OVERLOAD_IF(Processor, TProcessor)):
    processorFactory_(new TSingletonProcessorFactory(processor)) {
    setTransportFactory(boost::shared_ptr<TTransportFactory>(
                          new TTransportFactory()));
    setProtocolFactory(boost::shared_ptr<TBinaryProtocolFactory>(
                         new TBinaryProtocolFactory()));
  }

  template<typename ProcessorFactory>
  TServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
          const boost::shared_ptr<TServerTransport>& serverTransport,
          THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)):
    processorFactory_(processorFactory),
    serverTransport_(serverTransport) {
    setTransportFactory(boost::shared_ptr<TTransportFactory>(
                          new TTransportFactory()));
    setProtocolFactory(boost::shared_ptr<TBinaryProtocolFactory>(
                         new TBinaryProtocolFactory()));
  }

  template<typename Processor>
  TServer(const boost::shared_ptr<Processor>& processor,
          const boost::shared_ptr<TServerTransport>& serverTransport,
          THRIFT_OVERLOAD_IF(Processor, TProcessor)):
    processorFactory_(new TSingletonProcessorFactory(processor)),
    serverTransport_(serverTransport) {
    setTransportFactory(boost::shared_ptr<TTransportFactory>(
                          new TTransportFactory()));
    setProtocolFactory(boost::shared_ptr<TBinaryProtocolFactory>(
                         new TBinaryProtocolFactory()));
  }

  template<typename ProcessorFactory>
  TServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
          const boost::shared_ptr<TServerTransport>& serverTransport,
          const boost::shared_ptr<TTransportFactory>& transportFactory,
          const boost::shared_ptr<TProtocolFactory>& protocolFactory,
          THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)):
    processorFactory_(processorFactory),
    serverTransport_(serverTransport) {
    setTransportFactory(transportFactory);
    setProtocolFactory(protocolFactory);
  }

  template<typename Processor>
  TServer(const boost::shared_ptr<Processor>& processor,
          const boost::shared_ptr<TServerTransport>& serverTransport,
          const boost::shared_ptr<TTransportFactory>& transportFactory,
          const boost::shared_ptr<TProtocolFactory>& protocolFactory,
          THRIFT_OVERLOAD_IF(Processor, TProcessor)):
    processorFactory_(new TSingletonProcessorFactory(processor)),
    serverTransport_(serverTransport) {
    setTransportFactory(transportFactory);
    setProtocolFactory(protocolFactory);
  }

  template<typename ProcessorFactory>
  TServer(
    const boost::shared_ptr<ProcessorFactory>& processorFactory,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
    const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
    THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)) :
    processorFactory_(processorFactory),
    serverTransport_(serverTransport),
    duplexTransportFactory_(duplexTransportFactory),
    duplexProtocolFactory_(duplexProtocolFactory) {}

  template<typename Processor>
  TServer(
    const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
    const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
    THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    processorFactory_(new TSingletonProcessorFactory(processor)),
    serverTransport_(serverTransport),
    duplexTransportFactory_(duplexTransportFactory),
    duplexProtocolFactory_(duplexProtocolFactory) {}

  /**
   * Get a TProcessor to handle calls on a particular connection.
   *
   * This method should only be called once per connection (never once per
   * call).  This allows the TProcessorFactory to return a different processor
   * for each connection if it desires.
   */
  boost::shared_ptr<TProcessor> getProcessor(
      boost::shared_ptr<TProtocol> inputProtocol,
      boost::shared_ptr<TProtocol> outputProtocol,
      boost::shared_ptr<TTransport> transport) {
    TConnectionInfo connInfo;
    connInfo.input = inputProtocol;
    connInfo.output = outputProtocol;
    connInfo.transport = transport;
    return processorFactory_->getProcessor(connInfo);
  }

  // Class variables
  boost::shared_ptr<TProcessorFactory> processorFactory_;
  boost::shared_ptr<TServerTransport> serverTransport_;

  boost::shared_ptr<TDuplexTransportFactory> duplexTransportFactory_;
  boost::shared_ptr<TDuplexProtocolFactory> duplexProtocolFactory_;

  boost::shared_ptr<TServerEventHandler> eventHandler_;

public:
  void setProcessorFactory(
    boost::shared_ptr<TProcessorFactory> processorFactory) {
    processorFactory_ = processorFactory;
  }

  void setTransportFactory(
    boost::shared_ptr<TTransportFactory> transportFactory) {
    duplexTransportFactory_.reset(
      new TSingleTransportFactory<TTransportFactory>(transportFactory));
  }

  void setDuplexTransportFactory(
    boost::shared_ptr<TDuplexTransportFactory> duplexTransportFactory) {
    duplexTransportFactory_ = duplexTransportFactory;
  }

  void setProtocolFactory(boost::shared_ptr<TProtocolFactory> protocolFactory) {
    duplexProtocolFactory_.reset(
      new TSingleProtocolFactory<TProtocolFactory>(protocolFactory));
  }

  void setDuplexProtocolFactory(
    boost::shared_ptr<TDuplexProtocolFactory> duplexProtocolFactory) {
    duplexProtocolFactory_ = duplexProtocolFactory;
  }

  void setServerEventHandler(
    boost::shared_ptr<TServerEventHandler> eventHandler) {
    eventHandler_ = eventHandler;
  }

};

/**
 * Helper function to increase the max file descriptors limit
 * for the current process and all of its children.
 * By default, tries to increase it to as much as 2^24.
 */
 int increase_max_fds(int max_fds=(1<<24));


}}} // apache::thrift::server

#endif // #ifndef _THRIFT_SERVER_TSERVER_H_
