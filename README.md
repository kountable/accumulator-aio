# Accumulator AIO

Async Python client for Apache Accumulo's Thrift-proxy 
based on [thriftpy2](https://github.com/Thriftpy/thriftpy2) 
and ideas from [pyaccumulo](https://github.com/revelc/pyaccumulo).

Makes use of Python3 async/await mechanism and leverages async IO 
using the awesome async Thrift implementation from thriftpy2.

Under `/resources` you can find a Thrift schema for Accumulo 1.9.3 proxy `proxy.thrift` 
and a `docker-compose.yml` which starts up all the infrastructure for it:
Zookeeper, HDFS data-node, HDFS name-node, Accumulo (master, tserver, monitor) and Thrift Proxy.

For this client to work Accumulo Thrift Proxy is required to use
BinaryProtocol and FramedTransport (this seems to be hardcoded in proxy codebase).
This is the configuration that has to go to `$ACCUMULO_HOME/proxy/proxy.properties`:

```
protocolFactory=org.apache.thrift.protocol.TBinaryProtocol$$Factory
tokenClass=org.apache.accumulo.core.client.security.tokens.PasswordToken
```