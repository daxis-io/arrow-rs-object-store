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

import http from "node:http";

const attempts = new Map();
const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Range, If-Range, User-Agent",
  "Access-Control-Expose-Headers": "Content-Length, Content-Range, ETag",
  "Access-Control-Allow-Private-Network": "true",
};

const server = http.createServer((request, response) => {
  process.stdout.write(
    `${request.method} ${request.url} range=${request.headers.range ?? "-"} if-range=${request.headers["if-range"] ?? "-"} requested=${request.headers["access-control-request-headers"] ?? "-"}\n`,
  );

  if (request.method === "OPTIONS") {
    response.writeHead(204, cors);
    response.end();
    return;
  }

  if (request.url === "/health") {
    response.writeHead(200, { ...cors, "Content-Length": "2" });
    response.end("ok");
    return;
  }

  if (request.url !== "/transient") {
    response.writeHead(404, { ...cors, "Content-Length": "0" });
    response.end();
    return;
  }

  const attempt = (attempts.get(request.url) ?? 0) + 1;
  attempts.set(request.url, attempt);

  if (attempt === 1) {
    response.writeHead(503, { ...cors, "Content-Length": "0" });
    response.end();
    return;
  }

  if (request.headers.range !== "bytes=0-4") {
    response.writeHead(416, { ...cors, "Content-Length": "0" });
    response.end();
    return;
  }

  attempts.set(request.url, 0);
  response.writeHead(206, {
    ...cors,
    "Content-Length": "5",
    "Content-Range": "bytes 0-4/5",
    ETag: '"v1"',
  });
  response.end("hello");
});

server.listen(18080, "127.0.0.1", () => {
  process.stdout.write("WASM HTTP test server listening on 127.0.0.1:18080\n");
});
