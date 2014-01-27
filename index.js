var assert = require('assert');
var path = require('path')
var qs = require('querystring')
var url = require('url')

var Promise = require('bluebird')
var bun = require('bun');
var es = require('event-stream');
var JSONStream = require('JSONStream');
var Readable = require('readable-stream');
var request = require('request');
var through = require('through2');

var promisedRequest = Promise.promisify(request);

var IronMQ = function (token, options) {
  options = options || {};

  this.headers = {
    'Authorization': 'OAuth ' + token,
    'Content-Type': 'application/json',
    'User-Agent': 'Node.js IronMQ Streaming Client',
  };

  var api_ver = options.ver || '1'
  this.baseUrl = url.format({
    protocol: options.protocol || 'https',
    hostname: options.host || 'mq-aws-us-east-1.iron.io',
    port: options.port || 443,
    pathname: '/' + api_ver + '/',
  });
};

IronMQ.prototype.project = function (projectId) {
  assert(projectId, 'Missing projectId');
  this.projectId = projectId;
  return this;
};

IronMQ.prototype.queue = function (queueName) {
  queueName = queueName || 'default';
  return new Queue(this, queueName);
};


var Queue = function (ironMQ, queueName) {
  this._configure(ironMQ);
  this.queueName = queueName;
};

Queue.prototype._configure = function (ironMQ) {
  this.projectId = ironMQ.projectId;
  this.headers = ironMQ.headers;
  this.baseUrl = ironMQ.baseUrl;
};

// Create and return http stream for GET /messages
Queue.prototype.read = function (params) {
  assert(this.projectId, 'Missing projectId');
  assert(this.queueName, 'Missing queue name');

  // TODO: Make it extend instead of replace
  params = params || { n: 100 };
  var readPath = path.join('projects', this.projectId, 'queues', this.queueName, 'messages');
  if (params) {
    readPath += '?' + qs.stringify(params);
  }

  var readUrl = url.resolve(this.baseUrl, readPath);
  return bun([ createRequestStream(readUrl, this.headers), JSONStream.parse('messages.*') ]);
};

// Create and return http stream for POST /messages
Queue.prototype.write = function (params) {
  assert(this.projectId, 'Missing projectId');
  assert(this.queueName, 'Missing queue name');

  var writePath = path.join('projects', this.projectId, 'queues', this.queueName, 'messages');
  if (params) {
    writePath += '?' + qs.stringify(params);
  }

  var writeUrl = url.resolve(this.baseUrl, writePath);
  return bun([ getPrepareWriteStream(), JSONStream.stringify('{\n"messages":\n[', '\n,\n', '\n]\n}\n'), createRequestStream(writeUrl, 'POST', this.headers) ]);
};

// Create and return http stream for DELETE /messages
Queue.prototype.delete = function (params) {
  assert(this.projectId, 'Missing projectId');
  assert(this.queueName, 'Missing queue name');

  var deletePath = path.join('projects', this.projectId, 'queues', this.queueName, 'messages');
  if (params) {
    deletePath += '?' + qs.stringify(params);
  }

  // var test = through.obj(function (chunk, enc, callback) {
  //   console.log(chunk, typeof(chunk))
  //   this.push(chunk)
  //   callback();
  // }, function () {
  //   this.emit('end')
  // });

  var deleteUrl = url.resolve(this.baseUrl, deletePath);
  return bun([ getPrepareDeleteStream(), JSONStream.stringify('{\n"ids":\n', '', '\n}\n'), createRequestStream(deleteUrl, 'DELETE', this.headers) ]);
};

// Non-stream helper method
Queue.prototype.clear = function () {
  assert(this.projectId, 'Missing projectId');
  assert(this.queueName, 'Missing queue name');

  var clearPath = path.join('projects', this.projectId, 'queues', this.queueName, 'clear');
  var clearUrl = url.resolve(this.baseUrl, clearPath);

  return createRequestPromise(clearUrl, 'POST', this.headers)
    .then(parseResponse)
    .then(function(body) {
      if (!body || body.msg !== 'Cleared') {
        throw new Error('Error clearing messages');
      }

      return body;
    });
}


// Expose function
module.exports = function connect(token, options) {
  return new IronMQ(token, options);
};

// Create request stream
function createRequestStream(url, method, headers) {
  if (typeof(method) === 'object') {
    headers = method;
    method = 'GET';
  }

  // return new Readable().wrap(request({
  //   url: url,
  //   headers: headers,
  //   method: method,
  // }));
  var req = request({
    url: url,
    headers: headers,
    method: method,
  });
  // req.on('response', function (resp) {
  //   console.log('Response', resp)
  // })
  req.on('pipe', function (resp) {
    console.log('Piped', resp)
  })
  req.on('unpipe', function (resp) {
    console.log('Unpiped', resp)
  })
  return req;
}

// Get stream that prepares messages to be posted to queue
function getPrepareWriteStream() {
  return through.obj(function (chunk, enc, callback) {
    if (typeof(chunk) === 'object') {
      chunk = JSON.stringify(chunk);
    }

    this.push({
      body: chunk
    });
    callback();
  }, function () {
    this.emit('end');
  })
}

// Get stream that prepares messages to be posted to queue
function getPrepareDeleteStream() {
  return through.obj(function (chunk, enc, callback) {
    this._idBuffer = this._idBuffer || [];

    if (typeof(chunk) !== 'string') {
      chunk = String(chunk);
    }

    this._idBuffer.push(chunk);
    callback();
  }, function () {
    this.push(this._idBuffer);
    this.emit('end');
  })
}

// Create request promise
function createRequestPromise(url, method, headers) {
  if (typeof(method) === 'object') {
    headers = method;
    method = 'GET';
  }
  return promisedRequest({
    url: url,
    headers: headers,
    method: method,
  });
}

// Parse response
function parseResponse(response) {
  var body;

  console.log(response)
  try {
    body = JSON.parse(response[1]);
  } catch (e) {
    throw new Error('Error parsing body');
  }
  return body;
}
