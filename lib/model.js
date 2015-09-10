'use strict';

/**
 * Instance of this class represent a model
 * A model is analogous to a dynamo db table
 * A model has schema definition associated with itself
 *
 * @author      ritesh
 * @version     1.0.0
 */

var async = require('async');
var uuid = require('node-uuid');
var _ = require('lodash');
var moment = require('moment');

/**
 * Constructor function
 *
 * @param  {String}   tableName         name of the table for which to create a model instance
 * @param  {String}   primaryKey        name of the primary key attribute
 * @param  {Object}   schema            schema definition for modal
 * @param  {Object}   serviceInstance   dynamodb service instance
 */
function Model(tableName, primaryKey, schema, serviceInstance) {
  if(!tableName) {
    throw new Error('Table name is required');
  } else if(!schema) {
    throw new Error('Schema definition is required');
  } else if(!serviceInstance) {
    throw new Error('Service instance is required');
  }
  this.tableName = tableName;
  this.schema = schema;
  this.dynamodb = serviceInstance;
  this.primaryKey = primaryKey;
}

/**
 * Get the request payload for the given entity
 * This request payload is 'params' argument 'putItem' operation
 * @see http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#putItem-property
 *
 * This method will generate a uuid verson 4 random hash for primaryKey attribute
 *
 * @param  {Object}     entity          entity
 * @param  {Function}   callback        callback function
 */
Model.prototype.getPutRequestPayload = function(entity, callback) {
  var _self = this;
  // generate uuid for primary key
  var id = uuid.v4();
  var timestamp = moment().valueOf();
  _.extend(entity, {[_self.primaryKey]: id, createdOn: timestamp, updatedOn: timestamp});
  var params = {
    Item: entity,
    ReturnConsumedCapacity: 'NONE',
    ConditionExpression: 'attribute_not_exists(' + _self.primaryKey + ')',
    ReturnValues: 'ALL_OLD'
  };
  callback(null, params);
};

/**
 * Get dynamodb request payload for 'updateItem' operation
 * This request payload is 'params' argument 'updateItem' operation
 * @see  http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#updateItem-property
 *
 * @param  {String}     id              id of the entity to update
 * @param  {Object}     entity          the data to update
 * @param  {Function}   callback        callback function
 */
Model.prototype.getUpdateRequestPayload = function(id, entity, callback) {
  var _self = this;
  async.waterfall([
    function(cb) {
      _self.schema.parseUpdateFields(entity, cb);
    },
    function(mapped, cb) {
      _.extend(mapped, {
        tableName: _self.tableName,
        Key: {
          [_self.primaryKey]: id,
        },
        ReturnConsumedCapacity: 'NONE',
        ConditionExpression: 'attribute_exists(' + _self.primaryKey + ')',
        ReturnValues: 'ALL_NEW'
      });
      cb(null, mapped);
    }
  ], callback);
};

/**
 * Get dynamodb request payload for 'getItem' operation
 * This request payload is 'params' argument 'getItem' operation
 * @see  http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#getItem-property
 *
 * @param  {String}     id              id of the entity to find
 * @param  {Function}   callback        callback function
 */
Model.prototype.getGetRequestPayload = function(id, callback) {
  var _self = this;
  var params = {
    tableName: _self.tableName,
    Key: {
      [_self.primaryKey]: id
    },
    ConsistentRead: true,
    ReturnConsumedCapacity: 'NONE'
  };
  callback(null, params);
};

/**
 * Get dynamodb request payload for 'deleteItem' operation
 * This request payload is 'params' argument 'deleteItem' operation
 * @see  http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteItem-property
 *
 * @param  {String}     id              id of the entity to delete
 * @param  {Function}   callback        callback function
 */
Model.prototype.getDeleteRequestPayload = function(id, callback) {
  var _self = this;
  var params = {
    tableName: _self.tableName,
    Key: {
      [_self.primaryKey]: id
    },
    ConditionExpression: 'attribute_exists(' + _self.primaryKey + ')',
    ReturnConsumedCapacity: 'NONE'
  };
  callback(null, params);
};

/**
 * Insert a entity into the table, represented by this model
 *
 * @param  {Object}     entity          the data to insert
 * @param  {Function}   callback        callback function
 */
Model.prototype.insert = function(entity, callback) {
  var _self = this;
  async.waterfall([
    function(cb) {
      _self.schema.validate(entity, cb);
    },
    function(validatedEntity, cb) {
      _self.getPutRequestPayload(validatedEntity, cb);
    },
    function(params, cb) {
      _self.dynamodb(params, cb);
    }
  ], callback);
};

/**
 * Find an entity with the given id
 *
 * @param  {String}     id              id of the entity to find
 * @param  {Function}   callback        callback function
 */
Model.prototype.findById = function(id, callback) {
  var _self = this;
  async.waterfall([
    function(cb) {
      _self.getGetRequestPayload(id, cb);
    },
    function(params, cb) {
      _self.dynamodb.getItem(params, cb);
    }
  ], callback);
};

/**
 * Update an entity with the given id
 *
 * @param  {String}     id              id of the entity to update
 * @param  {Object}     entity          the data to update
 * @param  {Function}   callback        callback function
 */
Model.prototype.update = function(id, entity, callback) {
  var _self = this;
  async.waterfall([
    function(cb) {
      _self.schema.validate(entity, cb);
    },
    function(validatedEntity, cb) {
      _self.getUpdateRequestPayload(id, validatedEntity, cb);
    },
    function(params, cb) {
      _self.dynamodb.updateItem(params, cb);
    }
  ], callback);
};

/**
 * Delete an entity with the given id
 *
 * @param  {String}     id              id of the entity to delete
 * @param  {Function}   callback        callback function
 */
Model.prototype.delete = function(id, callback) {
  var _self = this;
  async.waterfall([
    function(cb) {
      _self.getDeleteRequestPayload(id, cb);
    },
    function(params, cb) {
      _self.dynamodb.deleteItem(params, cb);
    }
  ], callback);
};

// module export
module.exports = Model;