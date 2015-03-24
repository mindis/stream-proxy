# stream-proxy

Version 1.3 (initial public release)

A standalone streaming proxy service that provides dumps of line based data
to be served live or as a backlog. It was created as a local proxy for the
(Twitter streaming API)[https://dev.twitter.com/streaming/overview] to be
able to store data locally and re-run analytics for past days.

Does proxy streams written to disk by following the file end. Requires the
streams to be named after the current date they are written (i.e. 2015-01-11.json).
The stream files may be gzipped or plain.

# Running the proxy

The stream proxy builds an executable jar (````mvn package````) that contains all
dependencies and can be run using the following command line:

````
java -jar stream-proxy-1.3.jar <host> <port> <authfile> <dumpdir>
````

- ````host port```` - the hostname and port to listen for requests
- ````authfile```` - a json configuration file with users and passwords
- ````dumpdir```` - the directory with dump files as produced by [stream-crawler](https://github.com/streamdrill/stream-crawler)

The basic idea is that a dump file is one day of data with each line containing
a single JSON data structure. The data can then be retrieved using a streaming
HTTP connection using the resources:

# Resources

- ````/tweets.json```` - send the latest data as soon as it is stored on disk, waits for new data
- ````/tweets/range.json?start=20150101000000&end=20150102000000```` - send data from this renage

The ````start```` and ````end```` is in the format YYYYDDMMhhmmss. It is useful if a connection was lost
and previous data should be sent. If the system is fast enough it will catch up with the analysis. The
proxy sends the data as fast as possible.

LICENSE
=======

Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


