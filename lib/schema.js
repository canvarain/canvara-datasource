'use strict';


/**
 * Schema definition for canvara datasource
 *
 * @author      ritesh
 * @version     1.0.0
 */

var _ = require('lodash'),
  moment = require('moment'),
  errors = require('common-errors');

/**
 * Constructor function
 * @param {Object}    definition        The schema definition for this schema instance
 */
function Schema(definition) {
  if(!definition) {
    throw new Error('Schema definition is required');
  }
  this.definition = definition;
}

/**
 * Validate the value to be of given schemaType
 * @param  {String}   key               key name
 * @param  {String}   value             value corresponding to key name
 * @param  {String}   schemaType        Schema.SchemaType
 */
Schema.prototype.validateEach = function(key, value, schemaType) {
  var SchemaTypes = this.SchemaTypes, error;
  switch(schemaType) {
    case SchemaTypes.String: {
      if(!_.isString(value)) {
        error = new errors.ValidationError(key + ' should be a valid string');
      }
      break;
    }
    case SchemaTypes.Number: {
      if(!_.isNumber(value)) {
        error = new errors.ValidationError(key + ' should be a valid number');
      }
      break;
    }
    case SchemaTypes.Binary: {
      if(!_.isTypedArray(value) || !_.isString(value)) {
        error = new errors.ValidationError(key + ' should be of binary type');
      }
      break;
    }
    case SchemaTypes.StringSet: {
      if(!_.isArray(value)) {
        error = new errors.ValidationError(key + ' should be a valid string set');
      }
      break;
    }
    case SchemaTypes.NumberSet: {
      if(!_.isArray(value)) {
        error = new errors.ValidationError(key + ' should be a valid number set');
      }
      break;
    }
    case SchemaTypes.BinarySet: {
      if(!_.isArray(value)) {
        error = new errors.ValidationError(key + ' should be a valid binary set');
      }
      break;
    }
    case SchemaTypes.Map: {
      if(!_.isObject(value)) {
        error = new errors.ValidationError(key + ' should be a valid object');
      }
      break;
    }
    case SchemaTypes.Boolean: {
      if(!_.isBoolean(value)) {
        error = new errors.ValidationError(key + ' should be a valid boolean value');
      }
      break;
    }
    case SchemaTypes.Null: {
      if(_.isNull(value)) {
        error = new errors.ValidationError(key + ' should be null');
      }
      break;
    }
    default: {
      error = new errors.ValidationError(schemaType + ' is not a valid Schema.SchemaTypes value');
    }
  }
  return error;
};

/**
 * Validate the given enity data against the schema definition for this schema instance
 *
 * @param  {Object}     entity          the entity to validate
 * @param  {Function}   callback        callback function
 */
Schema.prototype.validate = function(entity, callback) {
  var validatedEntity = {}, error;
  var definition = this.definition, _self = this;
  _.forOwn(definition, function(value, key) {
    var entityValue = entity[key];
    if(value.required && !entityValue) {
      error = new errors.ValidationError(key + ' is required');
    } else {
      error = _self.validateEach(key, entityValue, value.type);
      validatedEntity[key] = entityValue;
    }
  });
  if(error) {
    return callback(error);
  }
  callback(null, validatedEntity);
};

/**
 * Export the schema type available
 */
Schema.SchemaTypes = {
  String: 'S',
  Number: 'N',
  Binary: 'B',
  StringSet: 'SS',
  NumberSet: 'NS',
  BinarySet: 'BS',
  Map: 'M',
  List: 'L',
  Boolean: 'BOOL',
  Null: 'NULL'
};

/**
 * Generate the UpdateExpression and ExpressionAttributeValues
 * @see http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#updateItem-property
 *
 * @param  {Object}     entity          the entity for which to parse fields
 * @param  {Function}   callback        callback function
 */
Schema.prototype.parseUpdateFields = function(entity, callback) {
  var timestamp = moment().valueOf();
  var params = {UpdateExpression: 'SET updatedOn = :updatedOn', ExpressionAttributeValues: {':updatedOn': timestamp}};
  var isValid = true, updateSet = [], removeSet = [], invalidKey;
  var definition = this.definition;
  _.forOwn(entity, function(value, key) {
    if(definition.hasOwnProperty(key)) {
      var details = definition[key];
      // if value is undefined/null/empty and this key is required field
      if(details.required && !value) {
        isValid = false;
        invalidKey = key;
        // break loop early
        return false;
      } else if(details.required && value) {
        updateSet.push(key);
      } else if(!details.required && value) {
        updateSet.push(key);
      } else if(!details.required && !value) {
        removeSet.push(key);
      } else if(value) {
        updateSet.push(key);
      } else if(!value) {
        removeSet(key);
      }
    }
  });
  if(!isValid) {
    return callback(new errors.ValidationError(invalidKey + ' is required'));
  }
  if(updateSet.length > 0) {
    updateSet.forEach(function(key) {
      params.UpdateExpression = params.UpdateExpression + ' ' + key  + ' = :' + key + ',';
      params.ExpressionAttributeValues[':' + key] = entity[key];
    });
    params.UpdateExpression = params.UpdateExpression.substring(0, params.UpdateExpression.length - 1);
  }


  if(removeSet.length > 0) {
    if(!params.UpdateExpression) {
      params.UpdateExpression = 'REMOVE';
    } else {
      params.UpdateExpression = params.UpdateExpression + ' REMOVE';
    }
    removeSet.forEach(function(key) {
      params.UpdateExpression = params.UpdateExpression + ' ' + key + ',';
    });
    params.UpdateExpression = params.UpdateExpression.substring(0, params.UpdateExpression.length - 1);
  }
  callback(null, params);
};

// module export
module.exports = Schema;