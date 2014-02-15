/**
 * Auxiliary promise interface to EC2 meta-data service
 */
var request       = require('superagent');
var Promise       = require('promise');

/** Auxiliary function to get text from URL */
var getUrlAsText = function(url) {
  return new Promise(function(accept, reject) {
    request
      .get(url)
      .end(function(res) {
        if (res.ok) {
          accept(res.text);
        } else {
          reject(res.status);
        }
      });
  });
};

/** Get AMI image identifier */
exports.getImageId = function() {
  return getUrlAsText('http://169.254.169.254/2012-01-12/meta-data/ami-id');
};

/** Get instance type */
exports.getInstanceType = function() {
  return getUrlAsText('http://169.254.169.254/2012-01-12/meta-data/instance-type');
};

/** Get availability zone */
exports.getAvailabilityZone = function() {
  return getUrlAsText('http://169.254.169.254/2012-01-12/meta-data/placement/availability-zone');
};

/** Get instance identifier */
exports.getInstanceId = function() {
  return getUrlAsText('http://169.254.169.254/2012-01-12/meta-data/instance-id');
};

