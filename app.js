var AWS = require('aws-sdk');
var http = require('http');
var yargs = require('yargs');

AWS.config.sslEnabled = false;

var s3 = new AWS.S3();
var objectsToCopy = [];
var maxRequests;
var currentRequests = 0;

var nextMarker = null;
var listInProgress = false;

var progress = {
  succeeded: 0,
  failed: 0,
  bytesTransferred: 0
}

function startRequest() {
  currentRequests++;
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
    
    currentRequests--;
    setImmediate(startRequests);

  });
}

function startRequests() {
  if (!listInProgress && objectsToCopy.length < 10000 && nextMarker !== false) {
    listInProgress = true;
    s3.listObjects({
      Bucket: argv.source,
      Marker: nextMarker
    }, function (err, data) {
      if (err) {
        console.log('Error listing objects', err)
      }
      
      if (data) {
        objectsToCopy = objectsToCopy.concat(data.Contents);
        if (data.IsTruncated) {
          nextMarker = data.Contents[data.Contents.length-1].Key;
        } else {
          nextMarker = false;
        }  
      }
      listInProgress = false;
      startRequests();
    });
  }

  while (currentRequests <= maxRequests && objectsToCopy.length) {
    startRequest();
  }
}

var argv = yargs
  .usage('Usage: $0 --source=[source bucket name] --target=[target bucket name]')
  .demand(['source','target'])
  .argv;

maxRequests = argv.maxRequests || 500;
http.globalAgent.maxSockets = argv.maxSockets || 100;
nextMarker = argv.marker || null;

startRequests();

setInterval(function () {
  var uptime = process.uptime();
  console.log({
    uptime: uptime,
    listBufferSize: objectsToCopy.length,
    succeeded: progress.succeeded,
    failed: progress.failed,
    gigabytesTransferred: progress.bytesTransferred/1024/1024/1024,
    megabitsPerSecond: ((progress.bytesTransferred/1024/1024) * 8) / uptime,
    objectsPerSecond: progress.succeeded / uptime,
    marker: nextMarker
  });

}, 1000);