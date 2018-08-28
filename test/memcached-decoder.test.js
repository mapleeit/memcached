'use strict';

/**
 * Test dependencies
 */
var assert = require('assert')
  , fs = require('fs')
  , common = require('./common')
  , Memcached = require('..')
  , Decoder = require('../lib/decoder')

global.testnumbers = global.testnumbers || +(Math.random(10) * 1000000).toFixed();

/**
 * Expresso test suite for all `parser` related
 * memcached commands
 */
describe('Memcached parser', function() {
  it('chunked response', function (done) {
    var memcached = new Memcached(common.servers.single)
      , message = common.alphabet(256)
      , chunks = []
      , chunk = 'VALUE tests::#{key} 2 {length}'
      , chunkJSON = JSON.stringify({
            lines: []
          , message: message
          , id: null
        })
      , testnr = ++global.testnumbers
      , callbacks = 0;

      // Build up our tests
      var S = {
        queries: []
        , streamID: 0
      };
      S.decoder = new Decoder(S)

      // Build up our chunk data
      chunks.push(chunk.replace('{key}', 1).replace('{length}', chunkJSON.length));
      chunks.push(chunkJSON);
      chunks.push(chunk.replace('{key}', 2).replace('{length}', chunkJSON.length));

      // console.log(chunks.join('\r\n') +'\r\n')

      // Insert first chunk
      S.decoder.appendBuffer(chunks.join('\r\n') +'\r\n');

      // We check for bufferArray length otherwise it will crash on 'SyntaxError: Unexpected token V'
      assert.equal(S.decoder.pipeArray.length, 3);

      chunks = []
      chunks.push(chunkJSON)
      chunks.push(chunk.replace('{key}', 1).replace('{length}', chunkJSON.length));
      chunks.push(chunkJSON);
      chunks.push(chunk.replace('{key}', 2).replace('{length}', chunkJSON.length));
      chunks.push(chunkJSON)
      chunks.push('END')
      // Insert second chunk
      S.decoder.appendBuffer(chunks.join('\r\n') + '\r\n');

      // Check if everything is cleared up nicely.
      assert.equal(S.decoder.pipeBuffer.length, 0);
      assert.equal(S.decoder.pipeArray.length, 0);
      assert.equal(S.queries.length, 0);

      memcached.end();
      done();
  });
});
