"use strict";

var createHash = require('crypto').createHash
  , toString = Object.prototype.toString;

exports.validateArg = function validateArg (args, config) {
  var err;

  args.validate.forEach(function (tokens) {
    var key = tokens[0]
      , value = args[key];

    switch(tokens[1]){
      case Number:
        if (toString.call(value) !== '[object Number]') {
          err = 'Argument "' + key + '" is not a valid Number.';
        }

        break;

      case Boolean:
        if (toString.call(value) !== '[object Boolean]') {
          err = 'Argument "' + key + '" is not a valid Boolean.';
        }

        break;

      case Array:
        if (toString.call(value) !== '[object Array]') {
          err = 'Argument "' + key + '" is not a valid Array.';
        }
        if (!err && key === 'key') {
          for (var vKey=0; vKey<value.length; vKey++) {
            var vValue = value[vKey];
            var result = validateKeySize(config, vKey, vValue);
            if (result.err) {
              err = result.err;
            } else {
              args.command = args.command.replace(vValue, result['value']);
            }
          }
        }
        break;

      case Object:
        if (toString.call(value) !== '[object Object]') {
          err = 'Argument "' + key + '" is not a valid Object.';
        }

        break;

      case Function:
        if (toString.call(value) !== '[object Function]') {
          err = 'Argument "' + key + '" is not a valid Function.';
        }

        break;

      case String:
        if (toString.call(value) !== '[object String]') {
          err = 'Argument "' + key + '" is not a valid String.';
        }

        if (!err && key === 'key') {
          var result = validateKeySize(config, key, value);
          if (result.err) {
            err = result.err;
          } else {
            args.command = reallocString(args.command).replace(value, result['value']);
          }
        }
        break;

      default:
        if (toString.call(value) === '[object global]' && !tokens[2]) {
          err = 'Argument "' + key + '" is not defined.';
        }
    }
  });

  if (err){
    if (args.callback) args.callback(new Error(err));
    return false;
  }

  return true;
};

var validateKeySize = function validateKeySize(config, key, value) {
  if (value.length > config.maxKeySize) {
    if (config.keyCompression){
      return { err: false, value: createHash('md5').update(value).digest('hex') };
    } else {
      return { err: 'Argument "' + key + '" is longer than the maximum allowed length of ' + config.maxKeySize };
    }
  } else if (/[\s\n\r]/.test(value)) {
    return { err: 'The key should not contain any whitespace or new lines' };
  } else {
    return { err: false, value: value };
  }
};

//Escapes values by putting backslashes before line breaks
exports.escapeValue = function(value) {
  return value.replace(/(\r|\n)/g, '\\$1');
};

//Unescapes escaped values by removing backslashes before line breaks
exports.unescapeValue = function(value) {
  return value.replace(/\\(\r|\n)/g, '$1');
};

var reallocString = exports.reallocString = function(value) {
  // Reallocate string to fix slow string operations in node 0.10
  // see https://code.google.com/p/v8/issues/detail?id=2869 for details
  return (' ' + value).substr(1);
};
