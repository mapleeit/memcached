const events = require('events')
const Stream = require('net').Stream
const Socket = require('net').Socket

const HashRing = require('hashring')
const Jackpot = require('jackpot')
const Connection = require('./connection')
const IssueLog = Connection.IssueLog
const Utils = require('./utils')

const LINEBREAK = '\r\n'
const NOREPLY = ' noreply'
const FLUSH = 1E3
const BUFFER = 1E2
const CONTINUE = 1E1
const FLAG_JSON = 1<<1
const FLAG_BINARY = 1<<2
const FLAG_NUMERIC = 1<<3

// const DecoderSymbol = Symbol('Decoder')
/**
 * Get server list by parsing first args of the Client constructor
 * @param {Mixed} args servers info
 * @returns {Array} servers server list
 */
const getServers = (args) => {
  const DEFALT_SERVER = 'localhost:11211'
  let servers = []
  // Parse down the connection arguments
  switch (Object.prototype.toString.call(args)) {
    case '[object Object]':
      servers = Object.keys(args)
      break

    case '[object Array]':
      servers = args.length ? args : [DEFALT_SERVER]
      break

    default:
      servers.push(args || DEFALT_SERVER)
      break
  }

  if (!servers.length) {
    throw new Error('No servers where supplied in the arguments');
  }

  return servers
}

const DEFAULT_CONFIG = {
  maxKeySize: 250,         // max key size allowed by Memcached
  maxExpiration: 2592000,  // max expiration duration allowed by Memcached
  maxValue: 1048576,       // max length of value allowed by Memcached
  activeQueries: 0,
  maxQueueSize: -1,
  algorithm: 'md5',        // hashing algorithm that is used for key mapping
  compatibility: 'ketama', // hashring compatibility

  poolSize: 10,            // maximal parallel connections
  retries: 5,              // Connection pool retries to pull connection from pool
  factor: 3,               // Connection pool retry exponential backoff factor
  minTimeout: 1000,        // Connection pool retry min delay before retrying
  maxTimeout: 60000,       // Connection pool retry max delay before retrying
  randomize: false,        // Connection pool retry timeout randomization

  reconnect: 18000000,     // if dead, attempt reconnect each xx ms
  timeout: 5000,           // after x ms the server should send a timeout if we can't connect
  failures: 5,             // Number of times a server can have an issue before marked dead
  failuresTimeout: 300000,   // Time after which `failures` will be reset to original value, since last failure
  retry: 30000,            // When a server has an error, wait this amount of time before retrying
  idle: 5000,              // Remove connection from pool when no I/O after `idle` ms
  remove: false,           // remove server if dead if false, we will attempt to reconnect
  redundancy: false,       // allows you do re-distribute the keys over a x amount of servers
  keyCompression: true,    // compress keys if they are to large (md5)
  namespace: '',           // sentinel to prepend to all memcache keys for namespacing the entries
  debug: false            // Output the commands and responses
}

class Client extends events {
  /**
   * Constructs a new memcached client
   * @constructor
   * @param {Mixed} args Array, string or object with servers
   * @param {Object} options options
   * @api public
   */
  constructor(args, options) {
    super()
    Object.assign(this, DEFAULT_CONFIG, options)

    this.servers = getServers(args)
    this.connections = {}
    this.issues = []

    function resultSetIsEmpty(resultSet) {
      return !resultSet || (resultSet.length === 1 && !resultSet[0]);
    }

    this.parsers = {
      // handle error responses
      'NOT_FOUND': function notfound(tokens, dataSet, err) {
        return [CONTINUE, false];
      }
  
    , 'NOT_STORED': function notstored(tokens, dataSet, err) {
        var errObj = new Error('Item is not stored');
        errObj.notStored = true;
        err.push(errObj);
        return [CONTINUE, false];
      }
  
    , 'ERROR': function error(tokens, dataSet, err) {
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
    , 'CONFIG': function() {
        return [CONTINUE, this.bufferArray[0]];
      }
    }
    this.allCommands = new RegExp('^(?:' + Object.keys(this.parsers).join('|') + '|\\d' + ')')
    this.bufferedCommands = new RegExp('^(?:' + Object.keys(this.parsers).join('|') + ')');
    this.resultParsers = {
      // combines the stats array, in to an object
      'stats': function stats(resultSet) {
        var response = {};
        if (resultSetIsEmpty(resultSet)) return response;
  
        // add references to the retrieved server
        response.server = this.serverAddress;
  
        // Fill the object
        resultSet.forEach(function each(statSet) {
          if (statSet) response[statSet[0]] = statSet[1];
        });
  
        return response;
      }
  
      // the settings uses the same parse format as the regular stats
    , 'stats settings': function settings() {
        return privates.resultParsers.stats.apply(this, arguments);
      }
  
      // Group slabs by slab id
    , 'stats slabs': function slabs(resultSet) {
        var response = {};
        if (resultSetIsEmpty(resultSet)) return response;
  
        // add references to the retrieved server
        response.server = this.serverAddress;
  
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
        response.server = this.serverAddress;
  
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

    const compatibility = this.compatibility || this.compatiblity;
    this.HashRing = new HashRing(args, this.algorithm, {
      'compatibility': compatibility,
      'default port': compatibility === 'ketama' ? 11211 : null
    })
  }

  // Creates or generates a new connection for the give server, the callback
  // will receive the connection if the operation was successful
  connect(server, callback) {
    // Default port to 11211
    if (!server.match(/(.+):(\d+)$/)) {
      server = server + ':11211'
    }

    // server is dead, bail out
    if (server in this.issues && this.issues[server].failed) {
      return callback(false, false);
    }

    // fetch from connection pool
    if (server in this.connections) {
      return this.connections[server].pull(callback);
    }

    // No connection factory created yet, so we must build one
    this.connections[server] = this._createConnectionPool(server)

    // now that we have setup our connection factory we can allocate a new
    // connection
    this.connections[server].pull(callback);
  }

  /**
   * Create a new connection pool
   * @params {String} server path or ip:port string
   * @private
   */
  _createConnectionPool(server) {
    const memcached = this
    const serverTokens = server[0] === '/'
      ? server
      : /(.*):(\d+){1,}$/.exec(server).reverse()

    // Pop original string from array
    if (Array.isArray(serverTokens)) serverTokens.pop();

    let sid = 0

    const pool = new Jackpot(this.poolSize)
    pool.retries = this.retries
    pool.factor = this.factor
    pool.minTimeout = this.minTimeout
    pool.maxTimeout = this.maxTimeout
    pool.randomize = this.randomize

    pool.setMaxListeners(0)
    pool.factory(function () {
      const S = Array.isArray(serverTokens)
        ? new Stream
        : new Socket

      // config the Stream
      S.streamID = sid++;
      S.setTimeout(memcached.timeout);
      S.setNoDelay(true);
      S.setEncoding('utf8');
      S.metaData = [];
      S.responseBuffer = "";
      S.bufferArray = [];
      S.serverAddress = server;
      S.tokens = [].concat(serverTokens);
      S.memcached = memcached;

      S.on('close', () => {
        memcached.debug && console.log(`socket close, sid: ${S.streamID}`)
      })

      S.on('data', (data) => {
        if (memcached.debug) {
          console.log(`socket data received, sid: ${S.streamID}`)
          console.log(data)
        }

        memcached.buffer(S, data)

        // let bufferArray = memcached[DecoderSymbol].getBufferArrayFromData(data)
        // S.bufferArray = S.bufferArray.concat(bufferArray)

        // memcached[DecoderSymbol].decode(S.bufferArray)
      })

      S.on('connect', () => {
        memcached.debug && console.log(`socket connect, sid: ${S.streamID}`)

        S.setTimeout(S.memcached.idle, () => {
          this.remove(S)
        })

        S.on('error', e => {
          memcached.connectionIssue(e.toString(), S)
          this.remove(S)
        })
      })

      S.on('timeout', () => {
        memcached.debug && console.log(`socket timeout, sid: ${S.streamID}`)
      })

      S.on('end', () => {
        memcached.debug && console.log(`socket end, sid: ${S.streamID}`)
      })

      // connect the net.Stream (or net.Socket) [port, hostname]
      S.connect.apply(S, S.tokens)

      return S
    })
    pool.on('error', e => {
      memcached.debug && console.log('Connection error', e)
    })

    return pool
  }

  // Exposes buffer to test-suite
  buffer(S, data) {
    S.responseBuffer += data

    // only call transform the data once we are sure, 100% sure, that we valid
    // response ending
    if (S.responseBuffer.substr(S.responseBuffer.length - 2) === LINEBREAK) {
      S.responseBuffer = Utils.reallocString(S.responseBuffer)

      const chunks = S.responseBuffer.split(LINEBREAK)

      if (this.debug) {
        chunks.forEach(line => {
          console.log(S.streamID + ' >> ' + line)
        })
      }

      // Fix zero-line endings in the middle
      const chunkLength = (chunks.length - 1)
      if (chunks[chunkLength].length === 0) chunks.splice(chunkLength, 1)

      S.responseBuffer = "" // clear!
      S.bufferArray = S.bufferArray.concat(chunks)
      this.rawDataReceived(S)
    }
  }

  /**
   * Creates a multi stream, so it's easier to query agains multiple memcached
   * servers.
   * 
   * @param {Array|Boolean} keys finding all needed servers based on key array, if `false` then use all servers 
   * @param {Function} callback the callback
   * @private
   */
  multi (keys, callback) {
    const map = {}
    let servers

    // gets all servers based on the supplied keys,
    // or just gives all servers if we don't have keys
    if (keys) {
      keys.forEach((key) => {
        const server = this.servers.length === 1
          ? this.servers[0]
          : this.HashRing.get(key);

        if (map[server]){
          map[server].push(key);
        } else {
          map[server] = [key];
        }
      });

      // store the servers
      servers = Object.keys(map);
    } else {
      servers = this.servers;
    }

    let i = servers.length

    while (i--) {
       //memcached.delegateCallback(this, servers[i], map[servers[i]], i, servers.length, callback);
      callback.call(this, servers[i], map[servers[i]], i, servers.length);
    }
  }

  // Executes the command on the net.Stream, if no server is supplied it will
  // use the query.key to get the server from the HashRing
  command(queryCompiler, server) {
    this.activeQueries++;
    const query = queryCompiler()

    if (this.activeQueries > this.maxQueueSize && this.maxQueueSize > 0){
      this.makeCallback(query.callback, "over queue limit", null);
      query = null;
      return;
    }

    // generate a regular query,
    const redundancy = this.redundancy && this.redundancy < this.servers.length
    const queryRedundancy = query.redundancyEnabled

    // validate the arguments
    if (query.validate && !Utils.validateArg(query, this)) {
      this.activeQueries--;
      return;
    }

    // try to find the correct server for this query
    if (!server) {
      // no need to do a hashring lookup if we only have one server assigned to
      // us
      if (this.servers.length === 1) {
        server = this.servers[0];
      } else {
        if (redundancy && queryRedundancy) {
          redundancy = this.HashRing.createRange(query.key, (this.redundancy + 1), true);
          server = redundancy.shift();
        } else {
          server = this.HashRing.get(query.key);
        }
      }
    }

    // check if any server exists or and if the server is still alive
    // a server may not exist if the manager was never able to connect
    // to any server.
    if (!server || (server in this.issues && this.issues[server].failed)) {
      return query.callback && this.makeCallback(query.callback, new Error(['Server at', server, 'not available'].join(' ')));
    }

    this.connect(server, (error, S) => {
      if (this.debug) {
        query.command.split(LINEBREAK).forEach(line => {
          console.log(S.streamID + ' << ' + line)
        })
      }

      // S not set if unable to connect to server
      if (!S) {
        const S = {
          serverAddress: server,
          tokens: server.split(':').reverse()
        }
        const message = error || 'Unable to connect to server';
        this.connectionIssue(message, S);
        return query.callback && this.makeCallback(query.callback, new Error(message));
      }

      // Other errors besides inability to connect to server
      if (error) {
        this.connectionIssue(error.toString(), S)
        return query.callback && this.makeCallback(query.callback, error)
      }

      if (S.readyState !== 'open') {
        const message = 'Connection readyState is set to ' + S.readyState;
        this.connectionIssue(message, S);
        return query.callback && this.makeCallback(query.callback,new Error(message));
      }

      // used for request timing
      query.start = Date.now();
      S.metaData.push(query);
      S.write(Utils.reallocString(query.command + LINEBREAK))
    })

    // if we have redundancy enabled and the query is used for redundancy, than
    // we are going loop over the servers, check if we can reach them, and
    // connect to the correct net connection. because all redundancy queries are
    // executed with "no reply" we do not need to store the callback as there
    // will be no value to parse.
    if (redundancy && queryRedundancy) {
      queryRedundancy = queryCompiler(queryRedundancy);

      redundancy.forEach(server => {
        if (server in this.issues && this.issues[server].failed){
            return;
        }

        this.connect(server, (error, S) => {
          if (!S || error || S.readyState !== 'open') return;
          S.write(queryRedundancy.command + LINEBREAK);
        })
      })
    }
  }

  // Logs all connection issues, and handles them off. Marking all requests as
  // cache misses.
  connectionIssue(error, S) {
    if (S && S.end) S.end()

    let issue
    const server = S.serverAddress

    // check for existing issue logs, or create a new log
    if (server in this.issues) {
      issue = this.issues[server]
    } else {
      issue = this.issues[server] = new IssueLog({
        server: server,
        tokens: S.tokens,
        reconnect: this.reconnect,
        failures: this.failures,
        failuresTimeout: this.failuresTimeout,
        retry: this.retry,
        remove: this.remove,
        failOverServers: this.failOverServers || null
      })
    }

    issue.on('issue', details => {
      this.emit('issue', details)
    })
    issue.on('failure', details => {
      this.emit('failure', details)
    })
    issue.on('reconnecting', details => {
      this.emit('reconnecting', details)
    })
    issue.on('reconnect', details => {
      this.emit('reconnect', details)
    })
    issue.on('remove', details => {
      this.emit('remove', details)
      this.connections[server].end()

      if (issue.failOverServers && issue.failOverServers.length) {
        this.HashRing.swap(server, issue.failOverServers.shift());
      } else {
        this.HashRing.remove(server);
        this.emit('failure', details);
      }
    })

    issue.log(error)
  }

  // Kills all active connections
  end() {
    Object.keys(this.connections).forEach((key) => {
      this.connections[key].free(0)
    })
  }

  delegateCallback() {
    this.activeQueries--;
    var master = arguments[0];
    var cb = arguments[arguments.length-1];
    var args = Array.prototype.slice.call(arguments, 1, arguments.length-1);
    cb.apply(master, args);
  }

  makeCallback(cb) {
    this.activeQueries--;
    const args = Array.prototype.slice.call(arguments, 1);
    cb.apply(this, args); //loose first
  }

  // The actual parsers function that scan over the responseBuffer in search of
  // Memcached response identifiers. Once we have found one, we will send it to
  // the dedicated parsers that will transform the data in a human readable
  // format, deciding if we should queue it up, or send it to a callback fn.
  rawDataReceived(S) {
    var queue = []
      , token
      , tokenSet
      , dataSet
      , resultSet
      , metaData
      , err = []

    while(S.bufferArray.length && this.allCommands.test(S.bufferArray[0])) {
      token = S.bufferArray.shift();
      tokenSet = token.split(' ');

      if (/^\d+$/.test(tokenSet[0])) {
          // special case for "config get cluster"
          // Amazon-specific memcached configuration information, see aws
          // documentation regarding adding auto-discovery to your client library.
          // Example response of a cache cluster containing three nodes:
          //   configversion\n
          //   hostname|ip-address|port hostname|ip-address|port hostname|ip-address|port\n\r\n
          if (/(([-.a-zA-Z0-9]+)\|(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)\|(\d+))/.test(S.bufferArray[0])) {
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
      if (tokenSet[0] === 'VALUE' && S.bufferArray.indexOf('END') === -1) {
        return S.bufferArray.unshift(token);
      }

      // check for dedicated parser
      if (this.parsers[tokenSet[0]]) {

        // fetch the response content
        if (tokenSet[0] === 'VALUE') {
          dataSet = Utils.unescapeValue(S.bufferArray.shift());
        }

        resultSet = this.parsers[tokenSet[0]].call(S, tokenSet, dataSet || token, err, queue, this);

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
                , this.resultParsers[metaData.type]
                    ? this.resultParsers[metaData.type].call(S, resultSet, err)
                    : !Array.isArray(queue) || queue.length > 1 ? queue : queue[0]
                , metaData.callback
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

      // check if we need to remove an empty item from the array, as splitting on /r/n might cause an empty
      // item at the end..
      if (S.bufferArray[0] === '') S.bufferArray.shift();
    }    
  }

  // Small wrapper function that only executes errors when we have a callback
  _errorResponse (error, callback) {
    if (typeof callback === 'function') {
      this.makeCallback(callback,error, false);
    }

    return false
  }

  // As all command nearly use the same syntax we are going to proxy them all to
  // this function to ease maintenance. This is possible because most set
  // commands will use the same syntax for the Memcached server. Some commands
  // do not require a lifetime and a flag, but the memcached server is smart
  // enough to ignore those.
  _setters (type, validate, key, value, lifetime, callback, cas) {
    const fullkey = this.namespace + key;
    var flag = 0
      , valuetype = typeof value
      , length;

    if (Buffer.isBuffer(value)) {
      flag = FLAG_BINARY;
      value = value.toString('binary');
    } else if (valuetype === 'number') {
      flag = FLAG_NUMERIC;
      value = value.toString();
    } else if (valuetype !== 'string') {
      flag = FLAG_JSON;
      value = JSON.stringify(value);
    }

    value = Utils.escapeValue(value);

    length = Buffer.byteLength(value);
    if (length > this.maxValue) {
      const error = new Error('The length of the value is greater than ' + this.maxValue)
      return this._errorResponse(error, callback)
    }

    this.command((noreply) => {
      return {
          key: fullkey
        , callback: callback
        , lifetime: lifetime
        , value: value
        , cas: cas
        , validate: validate
        , type: type
        , redundancyEnabled: false
        , command: [type, fullkey, flag, lifetime, length].join(' ') +
               (cas ? ' ' + cas : '') +
               (noreply ? NOREPLY : '') +
               LINEBREAK + value
      };
    });
  }

  // This is where the actual Memcached API layer begins:

  /**
   * Touches the given key
   * @param {String} key the given key
   * @param {Number} lifetime After how long should the key expire measured in `seconds`
   * @param {Function} callback the callback
   */
  touch(key, lifetime, callback) {
    const fullkey = this.namespace + key;
    this.command(() => {
      return {
          key: fullkey
        , callback: callback
        , lifetime: lifetime
        , validate: [['key', String], ['lifetime', Number], ['callback', Function]]
        , type: 'touch'
        , command: ['touch', fullkey, lifetime].join(' ')
      };
    });
  }

  /**
   * Get the value for the given key
   * @param {Mixed} key string or an array for keys
   * @param {Function} callback the callback
   */
  get(key, callback) {
    if (Array.isArray(key)) return this.getMulti.apply(this, arguments)

    const fullkey = this.namespace + key;
    this.command((noreply) => {
      return {
          key: fullkey,
          callback: callback,
          validate: [
            ['key', String], 
            ['callback', Function]
          ],
          type: 'get',
          command: 'get ' + fullkey
      }
    })
  }

  /**
   * Get the value for the given key
   * the difference between get and gets is that gets, also returns a cas value
   * and gets doesn't support multi-gets at this moment
   * 
   * @param {Mixed} key string or an array for keys
   * @param {Function} callback the callback
   */
  gets(key, callback) {
    const fullkey = this.namespace + key;
    this.command((noreply) => {
      return {
          key: fullkey,
          callback: callback,
          validate: [
            ['key', String], 
            ['callback', Function]
          ],
          type: 'gets',
          command: 'gets ' + fullkey
      }
    })
  }

  /**
   * Retrieves a bunch of values from multiple keys
   * 
   * @param {Array} keys all the keys that needs to be fetched
   * @param {Function} callback the callback
   */
  getMulti(keys, callback) {
    const responses = {}
    let errors = []
    let calls

    if (this.namespace.length) keys = keys.map(key => this.namespace + key)

    const handle = (err, results) => {
      if (err) errors.push(err)

      results = Array.isArray(results) ? results: [results]
      results.forEach(value => {
        if (value && this.namespace.length) {
          const nsKey = Object.keys(value)[0]
          let newValue = {}

          newValue[nsKey.replace(this.namespace, '')] = value[nsKey]
          Object.assign(responses, newValue)
        } else {
          Object.assign(responses, value)
        }
      })

      if (!--calls) callback(errors.length ? erros : undefined, responses)
    }

    this.multi(keys, (server, key, index, totals) => {
      if (!calls) calls = totals

      this.command(noreply => {
        return {
          callback: handle,
          multi: true,
          type: 'get',
          command: `get ${key.join(' ')}`,
          key: keys,
          validate: [['key', Array], ['callback', Function]]
        }
      }, server)
    })
  }

  /**
   * Stores a new value in Memcached
   * @param {String} key the name of the key
   * @param {Mixed} value Either a buffer, JSON, number or string that you want to store
   * @param {Number} lifetime how long the data needs to be stored measured in `seconds`
   * @param {Function} callback the callback
   */
  set(key, value, lifetime, callback) {
    this._setters('set', [
      ['key', String],
      ['value', String],
      ['lifetime', Number],
      ['callback', Function]
    ], key, value, lifetime, callback)
  }

  /**
   * Replaces the value in memcached
   * @param {String} key the name of the key
   * @param {Mixed} value Either a buffer, JSON, number or string that you want to store
   * @param {Number} lifetime how long the data needs to be stored measured in `seconds`
   * @param {Function} callback the callback
   */
  replace(key, value, lifetime, callback) {
    this._setters('replace', [
      ['key', String],
      ['value', String],
      ['lifetime', Number],
      ['callback', Function]
    ], key, value, lifetime, callback)
  }

  /**
   * Add the value, only if it's not in memcached already
   * @param {String} key the name of the key
   * @param {Mixed} value Either a buffer, JSON, number or string that you want to store
   * @param {Number} lifetime how long the data needs to be stored measured in `seconds`
   * @param {Function} callback the callback
   */
  add(key, value, lifetime, callback) {
    this._setters('add', [
      ['key', String],
      ['value', String],
      ['lifetime', Number],
      ['callback', Function]
    ], key, value, lifetime, callback)
  }

  /**
   * Check and set.
   * Add the value, only if it matches the given CAS value
   * @param {String} key the name of the key
   * @param {Mixed} value Either a buffer, JSON, number or string that you want to store
   * @param {String} cas the CAS value
   * @param {Number} lifetime how long the data needs to be stored measured in `seconds`
   * @param {Function} callback the callback
   */
  cas(key, value, cas, lifetime, callback) {
    this._setters('cas', [
      ['key', String],
      ['value', String],
      ['lifetime', Number],
      ['callback', Function]
    ], key, value, lifetime, callback, cas)
  }

  /**
   * Add the given value string to the value of an existing item
   * @param {String} key the name of the key
   * @param {Mixed} value Either a buffer, JSON, number or string that you want to append
   * @param {Function} callback the callback
   */
  append(key, value, callback) {
    this._setters('append', [
      ['key', String],
      ['value', String],
      ['lifetime', Number],
      ['callback', Function]
    ], key, value, 0, callback)
  }

  /**
   * Add the given value string to the value of an existing item
   * @param {String} key the name of the key
   * @param {Mixed} value Either a buffer, JSON, number or string that you want to prepend
   * @param {Function} callback the callback
   */
  prepend(key, value, callback) {
    this._setters('prepend', [
      ['key', String],
      ['value', String],
      ['lifetime', Number],
      ['callback', Function]
    ], key, value, 0, callback)
  }

  /**
   * Increment a given key
   * @param {String} key the name of the key
   * @param {Number} value The increment
   * @param {Function} callback the callback
   */
  increment(key, value, callback) {
    const fullkey = this.namespace + key;
    this.command((noreply) => {
      return {
          key: fullkey,
          callback: callback,
          value: value,
          validate: [
              ['key', String],
              ['value', Number],
              ['callback', Function]
          ],
          type: 'incr',
          redundancyEnabled: true,
          command: ['incr', fullkey, value].join(' ') +
               (noreply ? NOREPLY : '')
      };
    });
  }

  // alias for `increment`
  incr() {
    this.increment(...arguments)
  }

  /**
   * Decrement a given key
   * @param {String} key the name of the key
   * @param {Number} value The Decrement
   * @param {Function} callback the callback
   */
  decrement(key, value, callback) {
    const fullkey = this.namespace + key;
    this.command((noreply) => {
      return {
          key: fullkey,
          callback: callback,
          value: value,
          validate: [
              ['key', String],
              ['value', Number],
              ['callback', Function]
          ],
          type: 'decr',
          redundancyEnabled: true,
          command: ['decr', fullkey, value].join(' ') +
               (noreply ? NOREPLY : '')
      };
    });
  }

  // alias for `decrement`
  decr() {
    this.decrement(...arguments)
  }

  /**
   * Remove the key from memcached
   * @param {String} key the name of the key
   * @param {Function} callback the callback
   */
  delete(key, callback) {
    const fullkey = this.namespace + key;
    this.command(noreply => {
      return {
          key: fullkey,
          callback: callback,
          validate: [
              ['key', String],
              ['callback', Function]
          ],
          type: 'delete',
          redundancyEnabled: true,
          command: 'delete ' + fullkey +
               (noreply ? NOREPLY : '')
      };
    });
  }

  // alias for `delete`
  del() {
    this.delete(...arguments)
  }

  // Small wrapper that handle single keyword commands such as FLUSH ALL, VERSION and STAT
  _singles (type, callback) {
      const responses = []
      let errors
      let calls

      // handle multiple servers
    function handle(err, results) {
      if (err) {
        errors = errors || [];
        errors.push(err);
      }
      if (results) responses = responses.concat(results);

      // multi calls should ALWAYS return an array!
      if (!--calls) {
          callback(errors && errors.length ? errors.pop() : undefined, responses);
      }
    }

    this.multi(false, (server, keys, index, totals) => {
      if (!calls) calls = totals;

      this.command(function singlesCommand(noreply) {
        return {
            callback: handle,
            type: type,
            command: type
        };
      }, server);
    });
  }

  version (callback) {
    this._singles('version', callback)
  }

  stats (callback) {
    this._singles('stats', callback)
  }

  slabs (callback) {
    this._singles('stats slabs', callback)
  }

  // alias for `slabs`
  statsSlabs () {
    this.slabs(...arguments)
  }

  items () {
    this._singles('stats items', callback)
  }

  // alias for `items`
  statsItems () {
    this.items(...arguments)
  }

  settings () {
    this._singles('stats settings', callback)
  }

  // alias for `settings`
  statsSettings () {
    this.settings(...arguments)
  }

  flush () {
    this._singles('flush_all', callback)
  }

  // alias for `flush`
  flushAll () {
    this.flush(...arguments)
  }

  /**
   * Inspect cache
   * see simple_cachedump.js for an example
   * 
   * @param {String} server the server
   * @param {Number} slabid slab id you want to inspect
   * @param {Number} limit result limit, 0 mean no limit
   * @param {Function} callback the callback
   */
  cachedump(server, slabid, limit, callback) {
    this.command(noreply => {
      return {
          callback: callback
        , number: number
        , slabid: slabid
        , validate: [
              ['number', Number]
            , ['slabid', Number]
            , ['callback', Function]
          ]
        , type: 'stats cachedump'
        , command: 'stats cachedump ' + slabid + ' ' + number
      };
    }, server);
  }
}

module.exports = Client