var AWS = require('aws-sdk');
var Promise = require("bluebird");
var yargs = require('yargs');
var _ = require('underscore');

var s3 = new AWS.S3();
var objectsToCopy = [];
var maxThreads = 50;
var currentThreads = 0;

var progress = {
  succeeded: 0,
  failed: 0,
  bytesTransferred: 0
}

function getFullDirectoryList (bucket, progressCallback) {
  progressCallback = progressCallback || _.noop;
  
  return new Promise(function (resolve, reject) {
    var tempObjectList = [];
    
    function listObjects (marker) {
      var listOpts = {
        Bucket: bucket
      };
      
      if (marker) {
        listOpts.Marker = marker;
      }
      s3.listObjects(listOpts, function (err, data) {
        if (err) { // If we couldn't even list objects, fail completely
          reject(err);
        }
        
        if (data) {
          var marker = data.Contents[data.Contents.length-1].Key;
          tempObjectList = tempObjectList.concat(data.Contents);
          progressCallback({
            objects: data.Contents,
            objectCount: tempObjectList.length,
            marker: marker
          });
          
          if (data.IsTruncated) {
            setImmediate(function () {
              listObjects(marker);
            });
          } else {
            resolve(tempObjectList);
          }
        }
      });
    }

    listObjects();
  });
}

function startThread() {
  currentThreads++;
  var objectToCopy = objectsToCopy.pop();
  
  //console.log('Copying ', objectToCopy.Key);

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

getFullDirectoryList(argv.source, function (progress) {
  //console.log(_.pick(progress, 'objectCount', 'marker'));
  objectsToCopy = objectsToCopy.concat(progress.objects);
  //console.log('Objects left to copy: ', objectsToCopy.length);
  startThreads();
});

setInterval(function () {
  var uptime = process.uptime();
  console.log(_.extend(progress, {
    uptime: uptime,
    filesPerSecond: progress.succeeded / uptime,
    MBytesPerSecond: (progress.bytesTransferred/1024/1024) / uptime,
    objectsToCopy: objectsToCopy.length
  }));
}, 500);