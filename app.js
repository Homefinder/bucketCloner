var AWS = require('aws-sdk');
var http = require('http');
var yargs = require('yargs');
var _ = require('underscore');

AWS.config.sslEnabled = false;
http.globalAgent.maxSockets = 100;

var s3 = new AWS.S3();
var objectsToCopy = [];
var maxThreads = 50;
var currentThreads = 0;

var nextMarker;
var listInProgress = false;

var progress = {
  succeeded: 0,
  failed: 0,
  bytesTransferred: 0
}

function startThread() {
  currentThreads++;
  var objectToCopy = objectsToCopy.pop();

  s3.copyObject({
    Bucket: argv.target,
    CopySource: encodeURIComponent(argv.source) + '/' + objectToCopy.Key,
    Key: objectToCopy.Key
  }, function (err, data) {
    if (err) {
      console.log('Error with ' + objectToCopy.Key, err);
      progress.failed++;
    } else {
      progress.succeeded++;
      progress.bytesTransferred += objectToCopy.Size
    }
    
    currentThreads--;
    setImmediate(startThreads);

  });
}

function startThreads() {
  if (!listInProgress && objectsToCopy.length < 10000 && nextMarker !== false) {
    console.log('listing objects', nextMarker);
    listInProgress = true;
    s3.listObjects({
      Bucket: argv.source,
      Marker: nextMarker
    }, function (err, data) {
      if (err) {
        console.log('Error listing objects', err)
      }
      objectsToCopy = objectsToCopy.concat(data.Contents);
      if (data.IsTruncated) {
        nextMarker = data.Contents[data.Contents.length-1].Key;
      } else {
        nextMarker = false;
      }
      listInProgress = false;
      startThreads();
    });
  }

  while (currentThreads <= maxThreads && objectsToCopy.length) {
    startThread();
  }
}

var argv = yargs
  .usage('Usage: $0 --source=[source bucket name] --target=[target bucket name]')
  .demand(['source','target'])
  .argv;

if (argv.threads) {
  maxThreads = argv.threads;
}

startThreads();

setInterval(function () {
  var uptime = process.uptime();
  console.log(_.extend(progress, {
    uptime: uptime,
    filesPerSecond: progress.succeeded / uptime,
    MBytesPerSecond: (progress.bytesTransferred/1024/1024) / uptime,
    listBufferSize: objectsToCopy.length,
    marker: nextMarker
  }));
}, 1000);