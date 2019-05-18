# Accumulator AIO

Async Python client for Apache Accumulo's Thrift-proxy 
based on [thriftpy2](https://github.com/Thriftpy/thriftpy2) 
and ideas from [pyaccumulo](https://github.com/revelc/pyaccumulo).

Makes use of Python3 async/await mechanism and leverages async IO 
using the awesome async Thrift implementation from thriftpy2.

Under `/resources` you can find a Thrift schema for Accumulo 1.9.3 proxy 
and a `docker-compose.yml` which starts up all the infrastructure for it:
Zookeeper, HDFS data-node, HDFS name-node, Accumulo (master, tserver, monitor) and Thrift Proxy.

...