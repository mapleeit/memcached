/**
 * @file
 * decode from response buffer to a human readable format
 * @fires Decoder#servererror
 * @fires Decoder#step
 */
const events = require('events')
const Utils = require('./utils')

const LINEBREAK = '\r\n'
const FLUSH = 1E3
const BUFFER = 1E2
const CONTINUE = 1E1
const FLAG_JSON = 1<<1
const FLAG_BINARY = 1<<2
const FLAG_NUMERIC = 1<<3

const DEFAULT_CONFIG = {
  debug: false
}

class Decoder extends events {
  constructor(S, options) {
    super()
    Object.assign(this, DEFAULT_CONFIG, options)
    
    this.S = S

    this.pipeBuffer = ''
    this.pipeArray = []

    function resultSetIsEmpty(resultSet) {
      return !resultSet || (resultSet.length === 1 && !resultSet[0]);
    }
    this.parsers = {
      // handle error responses
      'NOT_FOUND'(tokens, dataSet, err) {
        return [CONTINUE, false];
      },

      'NOT_STORED'(tokens, dataSet, err) {
        const errObj = new Error('Item is not stored');
        errObj.notStored = true;
        err.push(errObj);
        return [CONTINUE, false];
      },

      'ERROR'(tokens, dataSet, err) {
        err.push(new Error('Received an ERROR response'));
        return [FLUSH, false];
      },

      'CLIENT_ERROR'(tokens, dataSet, err) {
        err.push(new Error(tokens.splice(1).join(' ')));
        return [CONTINUE, false];
      },

      'SERVER_ERROR'(tokens, dataSet, err, queue) {
        /**
         * @event Decoder#servererror
         * @type {String} error info
         */
        this.emit('servererror', tokens.splice(1).join(' '))
        return [CONTINUE, false];
      },

      // keyword based responses
      'STORED'(tokens, dataSet) {
        return [CONTINUE, true];
      },

      'TOUCHED'(tokens, dataSet) {
        return [CONTINUE, true];
      },

      'DELETED'(tokens, dataSet) {
        return [CONTINUE, true];
      },

      'OK'(tokens, dataSet) {
        return [CONTINUE, true];
      },

      'EXISTS'(tokens, dataSet) {
        return [CONTINUE, false];
      },

      'END'(tokens, dataSet, err, queue) {
        if (!queue.length) queue.push(undefined);
        return [FLUSH, true];
      },

      // value parsing:
      'VALUE'(tokens, dataSet, err, queue) {
        var key = tokens[1]
          , flag = +tokens[2]
          , dataLen = tokens[3] // length of dataSet in raw bytes
          , cas = tokens[4]
          , multi = this.S.metaData[0] && this.S.metaData[0].multi || cas
            ? {}
            : false
          , tmp;

        // In parse data there is an '||' passing us the content of token
        // if dataSet is empty. This may be fine for other types of responses,
        // in the case of an empty string being stored in a key, it will
        // result in unexpected behavior:
        // https://github.com/3rd-Eden/node-memcached/issues/64
        if (dataLen === '0') {
          dataSet = '';
        }

        switch (flag) {
          case FLAG_JSON:
            dataSet = JSON.parse(dataSet);
            break;
          case FLAG_NUMERIC:
            dataSet = +dataSet;
            break;
          case FLAG_BINARY:
            tmp = new Buffer(dataSet.length);
            tmp.write(dataSet, 0, 'binary');
            dataSet = tmp;
            break;
        }

        // Add to queue as multiple get key key key key key returns multiple values
        if (!multi) {
          queue.push(dataSet);
        } else {
          multi[key] = dataSet;
          if (cas) multi.cas = cas;
          queue.push(multi);
        }

        return [BUFFER, false];
      },

      'INCRDECR'(tokens) {
        return [CONTINUE, +tokens[1]];
      },

      'STAT'(tokens, dataSet, err, queue) {
        queue.push([tokens[1], /^\d+$/.test(tokens[2]) ? +tokens[2] : tokens[2]]);
        return [BUFFER, true];
      },

      'VERSION'(tokens, dataSet) {
        var versionTokens = /(\d+)(?:\.)(\d+)(?:\.)(\d+)$/.exec(tokens[1]);

        return [CONTINUE, {
          server: this.S.serverAddress,
          version: versionTokens[0],
          major: versionTokens[1] || 0,
          minor: versionTokens[2] || 0,
          bugfix: versionTokens[3] || 0
        }];
      },

      'ITEM'(tokens, dataSet, err, queue) {
        queue.push({
          key: tokens[1],
          b: +tokens[2].substr(1),
          s: +tokens[4]
        });

        return [BUFFER, false];
      },
      // Amazon-specific memcached configuration information, used for node
      // auto-discovery.
      'CONFIG'() {
        return [CONTINUE, this.pipeArray[0]];
      }
    }
    this.allCommands = new RegExp('^(?:' + Object.keys(this.parsers).join('|') + '|\\d' + ')');
    this.resultParsers = {
      // combines the stats array, in to an object
      'stats': function stats(resultSet) {
        var response = {};
        if (resultSetIsEmpty(resultSet)) return response;
  
        // add references to the retrieved server
        response.server = this.S.serverAddress;
  
        // Fill the object
        resultSet.forEach(function each(statSet) {
          if (statSet) response[statSet[0]] = statSet[1];
        });
  
        return response;
      }
  
      // the settings uses the same parse format as the regular stats
    , 'stats settings': function settings() {
        return this.resultParsers.stats.apply(this, arguments);
      }
  
      // Group slabs by slab id
    , 'stats slabs': function slabs(resultSet) {
        var response = {};
        if (resultSetIsEmpty(resultSet)) return response;
  
        // add references to the retrieved server
        response.server = this.S.serverAddress;
  
        // Fill the object
        resultSet.forEach(function each(statSet) {
          if (statSet) {
            var identifier = statSet[0].split(':');
  
            if (!response[identifier[0]]) response[identifier[0]] = {};
            response[identifier[0]][identifier[1]] = statSet[1];
          }
        });
  
        return response;
      }
  
    , 'stats items': function items(resultSet) {
        var response = {};
        if (resultSetIsEmpty(resultSet)) return response;
  
        // add references to the retrieved server
        response.server = this.S.serverAddress;
  
        // Fill the object
        resultSet.forEach(function each(statSet) {
          if (statSet && statSet.length > 1) {
            var identifier = statSet[0].split(':');
  
            if (!response[identifier[1]]) response[identifier[1]] = {};
            response[identifier[1]][identifier[2]] = statSet[1];
          }
        });
  
        return response;
      }
    }
  }

  appendBuffer (bufferStream) {
    this.pipeBuffer += bufferStream

    // only call transform the data once we are sure, 100% sure, that we valid
    // response ending
    if (this.pipeBuffer.substr(this.pipeBuffer.length - 2) === LINEBREAK) {
      this.pipeBuffer = Utils.reallocString(this.pipeBuffer)

      const chunks = this.pipeBuffer.split(LINEBREAK)

      if (this.debug) {
        chunks.forEach(line => {
          console.log(S.streamID + ' >> ' + line)
        })
      }

      // Fix zero-line endings in the middle
      const chunkLength = (chunks.length - 1)
      if (chunks[chunkLength].length === 0) chunks.splice(chunkLength, 1)

      this.pipeBuffer = "" // clear!
      this.appendArray(chunks)
    }
  }

  appendArray (array) {
    this.pipeArray = this.pipeArray.concat(array)

    this.decode()
  }

  decode() {
    const S = this.S
    let queue = []
    let token
    let tokenSet
    let dataSet
    let resultSet
    let metaData
    let err = []

    while(this.pipeArray.length && this.allCommands.test(this.pipeArray[0])) {
      token = this.pipeArray.shift();
      tokenSet = token.split(' ');

      if (/^\d+$/.test(tokenSet[0])) {
          // special case for "config get cluster"
          // Amazon-specific memcached configuration information, see aws
          // documentation regarding adding auto-discovery to your client library.
          // Example response of a cache cluster containing three nodes:
          //   configversion\n
          //   hostname|ip-address|port hostname|ip-address|port hostname|ip-address|port\n\r\n
          if (/(([-.a-zA-Z0-9]+)\|(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)\|(\d+))/.test(this.pipeArray[0])) {
              tokenSet.unshift('CONFIG');
          }
          // special case for digit only's these are responses from INCR and DECR
          else {
            tokenSet.unshift('INCRDECR');
          }
      }
      // special case for value, it's required that it has a second response!
      // add the token back, and wait for the next response, we might be
      // handling a big ass response here.
      if (tokenSet[0] === 'VALUE' && this.pipeArray.indexOf('END') === -1) {
        return this.pipeArray.unshift(token);
      }

      // check for dedicated parser
      if (this.parsers[tokenSet[0]]) {

        // fetch the response content
        if (tokenSet[0] === 'VALUE') {
          dataSet = Utils.unescapeValue(this.pipeArray.shift());
        }

        resultSet = this.parsers[tokenSet[0]].call(this, tokenSet, dataSet || token, err, queue);

        // check how we need to handle the resultSet response
        switch (resultSet.shift()) {
          case BUFFER:
            break;

          case FLUSH:
            metaData = S.metaData.shift();
            resultSet = queue;

            /**
             * @event
             * @type {Object}
             * @property {Object} metaData
             * @property {String} error error info
             * @property {Mixed} result data info
             */
            this.emit('step', {
              metaData,
              error: err.length ? err : err[0],
              result: this.resultParsers[metaData.type]
                ? this.resultParsers[metaData.type].call(this, resultSet, err)
                : !Array.isArray(queue) || queue.length > 1 ? queue : queue[0]
            })

            queue.length = err.length = 0;
            break;

          default:
            metaData = S.metaData.shift();

            this.emit('step', {
              metaData,
              error: err.length > 1 ? err : err[0],
              result: resultSet[0]
            })

            err.length = 0;
            break;
        }
      } else {
        // handle unkown responses
        metaData = S.metaData.shift();

        this.emit('step', {
          metaData,
          error: new Error('Unknown response from the memcached server: "' + token + '"'),
              result: false
        })
      }

      // cleanup
      dataSet = tokenSet = metaData = undefined;

      // check if we need to remove an empty item from the array, as splitting on /r/n might cause an empty
      // item at the end..
      if (this.pipeArray[0] === '') this.pipeArray.shift();
    }    
  }
}

module.exports = Decoder