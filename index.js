'use strict';

/**
 * Main script for the canvara datasource
 * This module is loaded when canvara-datasource is required in application
 *
 * @author      ritesh
 * @version     1.0.0
 */

var Schema = require('./lib/schema');
var Model = require('./lib/model');
var AWS = require('aws-sdk');

/**
 * Constructor function
 * @param {Object}  opts              aws-sdk dynamodb service class options
 */
function CanvaraDatasource(opts) {
  if(!opts || !opts.region) {
    throw new Error('AWS region is required');
  } else if(!opts || !opts.applicationPrefix) {
    throw new Error('applicationPrefix is required');
  }
  this.dynamodb = new AWS.DynamoDB({
    apiVersion: '2012-08-10',
    sslEnabled: true,
    region: opts.region
  });
}

/**
 * Create a model for given schema definition and table name
 * @param  {String}   tableName       name of the table for which to create a model instance
 * @param  {String}   primaryKey      name of the primary key attribute
 * @param  {Object}   schema          schema definition for modal
 */
CanvaraDatasource.prototype.model = function(tableName, primaryKey, schema) {
  tableName = this.applicationPrefix + tableName;
  var model = new Model(tableName, primaryKey, schema, this.dynamodb);
  return model;
};

/**
 * Return the dynamodb service instance associated with this datasource
 */
CanvaraDatasource.prototype.getDynamodb = function() {
  return this.dynamodb;
};

// export the Schema
CanvaraDatasource.Schema = Schema;

// module export
module.exports = CanvaraDatasource;