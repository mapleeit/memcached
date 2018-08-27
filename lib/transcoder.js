/**
 * @file
 * Encode from a human readable format to buffer or 
 * decode from response buffer to a human readable format
 */

const Utils = require('./utils')

const LINEBREAK = '\r\n'
const NOREPLY = ' noreply'
const FLUSH = 1E3
const BUFFER = 1E2
const CONTINUE = 1E1
const FLAG_JSON = 1<<1
const FLAG_BINARY = 1<<2
const FLAG_NUMERIC = 1<<3

const DEFAULT_CONFIG = {
  debug: false
}

class Transcoder {
  constructor(options) {
    Object.assign(this, DEFAULT_CONFIG, options)
  }
}

class Encoder extends Transcoder {
  constructor(options) {
    super(options)
  }
}


class Decoder extends Transcoder() {
  constructor(options) {
    super(options)

    this._parsers = {
      // handle error responses
      'NOT_FOUND': function notfound() {
        return [CONTINUE, false];
      }

      , 'NOT_STORED': function notstored() {
        const errObj = new Error('Item is not stored');
        errObj.notStored = true;
        err.push(errObj);
        return [CONTINUE, false];
      }

      , 'ERROR': function error() {
        err.push(new Error('Received an ERROR response'));
        return [FLUSH, false];
      }

      , 'CLIENT_ERROR': function clienterror(tokens, dataSet, err) {
        err.push(new Error(tokens.splice(1).join(' ')));
        return [CONTINUE, false];
      }

      , 'SERVER_ERROR': function servererror(tokens, dataSet, err, queue, S, memcached) {
        (memcached || this.memcached).connectionIssue(tokens.splice(1).join(' '), this);
        return [CONTINUE, false];
      }

      // keyword based responses
      , 'STORED': function stored(tokens, dataSet) {
        return [CONTINUE, true];
      }

      , 'TOUCHED': function touched(tokens, dataSet) {
        return [CONTINUE, true];
      }

      , 'DELETED': function deleted(tokens, dataSet) {
        return [CONTINUE, true];
      }

      , 'OK': function ok(tokens, dataSet) {
        return [CONTINUE, true];
      }

      , 'EXISTS': function exists(tokens, dataSet) {
        return [CONTINUE, false];
      }

      , 'END': function end(tokens, dataSet, err, queue) {
        if (!queue.length) queue.push(undefined);
        return [FLUSH, true];
      }

      // value parsing:
      , 'VALUE': function value(tokens, dataSet, err, queue) {
        var key = tokens[1]
          , flag = +tokens[2]
          , dataLen = tokens[3] // length of dataSet in raw bytes
          , cas = tokens[4]
          , multi = this.metaData[0] && this.metaData[0].multi || cas
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
      }

      , 'INCRDECR': function incrdecr(tokens) {
        return [CONTINUE, +tokens[1]];
      }

      , 'STAT': function stat(tokens, dataSet, err, queue) {
        queue.push([tokens[1], /^\d+$/.test(tokens[2]) ? +tokens[2] : tokens[2]]);
        return [BUFFER, true];
      }

      , 'VERSION': function version(tokens, dataSet) {
        var versionTokens = /(\d+)(?:\.)(\d+)(?:\.)(\d+)$/.exec(tokens[1]);

        return [CONTINUE, {
          server: this.serverAddress
          , version: versionTokens[0]
          , major: versionTokens[1] || 0
          , minor: versionTokens[2] || 0
          , bugfix: versionTokens[3] || 0
        }];
      }

      , 'ITEM': function item(tokens, dataSet, err, queue) {
        queue.push({
          key: tokens[1]
          , b: +tokens[2].substr(1)
          , s: +tokens[4]
        });

        return [BUFFER, false];
      }
      // Amazon-specific memcached configuration information, used for node
      // auto-discovery.
      , 'CONFIG': function () {
        return [CONTINUE, this.bufferArray[0]];
      }
    }
    this._allCommands = new RegExp('^(?:'
      + Object.keys(this.parsers).join('|')
      + '|\\d'
      + ')');

  }

  getBufferArrayFromData (responseBuffer) {
    const chunks = responseBuffer.split(LINEBREAK)
    const chunksLength = chunks.length - 1
    // Fix zero-line endings in the middle
    if (chunks[chunksLength].length === 0) chunks.splice(chunksLength, 1)

    if (this.debug) {
      chunks.forEach((line) => {
        console.log('decoding >> ' + line);
      });
    }
    return chunks
  }

  decode(bufferArray) {
    this._parse(bufferArray)
  }

  _parse(bufferArray) {
    let queue = []
      , dataSet
      , resultSet
      , metaData
      , err = []

    while (bufferArray.length && this._allCommands.test(bufferArray[0])) {
      const token = bufferArray.shift()
      const tokenSet = token.split(' ')

      if (/^\d+$/.test(tokenSet[0])) {
        // special case for "config get cluster"
        // Amazon-specific memcached configuration information, see aws
        // documentation regarding adding auto-discovery to your client library.
        // Example response of a cache cluster containing three nodes:
        //   configversion\n
        //   hostname|ip-address|port hostname|ip-address|port hostname|ip-address|port\n\r\n
        if (/(([-.a-zA-Z0-9]+)\|(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)\|(\d+))/.test(bufferArray[0])) {
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
      if (tokenSet[0] === 'VALUE' && bufferArray.indexOf('END') === -1) {
        return bufferArray.unshift(token);
      }

      // check for dedicated parser
      if (this._parsers[tokenSet[0]]) {

        // fetch the response content
        if (tokenSet[0] === 'VALUE') {
          dataSet = Utils.unescapeValue(bufferArray.shift());
        }

        const parser = this._parsers[tokenSet[0]]
        // resultSet = parser.call(S, tokenSet, dataSet || token, err, queue, this)
        resultSet = parser(tokenSet, dataSet || token, err, queue)

        // check how we need to handle the resultSet response
        switch (resultSet.shift()) {
          case BUFFER:
            break;

          case FLUSH:
            metaData = S.metaData.shift();
            resultSet = queue;

            // if we have a callback, call it
            if (metaData && metaData.callback) {
              metaData.execution = Date.now() - metaData.start;
                this.delegateCallback(
                  metaData
                , err.length ? err : err[0]

                  // see if optional parsing needs to be applied to make the result set more readable
                , privates.resultParsers[metaData.type]
                    ? privates.resultParsers[metaData.type].call(S, resultSet, err)
                    : !Array.isArray(queue) || queue.length > 1 ? queue : queue[0]
                ,metaData.callback
              );
            }

            queue.length = err.length = 0;
            break;

          default:
            metaData = S.metaData.shift();

            if (metaData && metaData.callback) {
              metaData.execution = Date.now() - metaData.start;
              this.delegateCallback(metaData, err.length > 1 ? err : err[0], resultSet[0], metaData.callback);
            }

            err.length = 0;
            break;
        }
      } else {
        // handle unkown responses
        metaData = S.metaData.shift();
        if (metaData && metaData.callback){
          metaData.execution = Date.now() - metaData.start;
            this.delegateCallback(metaData, new Error('Unknown response from the memcached server: "' + token + '"'), false, metaData.callback);
        }
      }

      // cleanup
      dataSet = tokenSet = metaData = undefined;

      // check if we need to remove an empty item from the array, 
      // as splitting on /r/n might cause an empty
      // item at the end..
      if (S.bufferArray[0] === '') S.bufferArray.shift();
    }
  }

  decode(memcached, S, BufferStream) {
    console.log(`decoding ${BufferStream}`)

    S.responseBuffer += BufferStream;

    // only call transform the data once we are sure, 100% sure, that we valid
    // response ending
    if (S.responseBuffer.substr(S.responseBuffer.length - 2) === LINEBREAK) {
      S.responseBuffer = Utils.reallocString(S.responseBuffer);

      var chunks = S.responseBuffer.split(LINEBREAK);

      if (memcached.debug) {
        chunks.forEach(function each(line) {
          console.log(S.streamID + ' >> ' + line);
        });
      }

      // Fix zero-line endings in the middle
      var chunkLength = (chunks.length - 1);
      if (chunks[chunkLength].length === 0) chunks.splice(chunkLength, 1);

      S.responseBuffer = ""; // clear!
      memcached.rawDataReceived(S, bufferArray = S.bufferArray.concat(chunks));

    }
  }
}
}

module.exports = {
  Encoder,
  Decoder
}