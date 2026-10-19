import { createRequire as __cr } from 'node:module'; const require = __cr(import.meta.url);
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __commonJS = (cb, mod) => function __require2() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports, module) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: Symbol("kIsForOnEventAttribute"),
      kListener: Symbol("kListener"),
      kStatusCode: Symbol("status-code"),
      kWebSocket: Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports, module) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = __require("bufferutil");
        module.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports, module) {
    "use strict";
    var kDone = Symbol("kDone");
    var kRun = Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports, module) {
    "use strict";
    var zlib = __require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = Symbol("permessage-deflate");
    var kTotalLength = Symbol("total-length");
    var kCallback = Symbol("callback");
    var kBuffers = Symbol("buffers");
    var kError = Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num4 = +value;
                if (!Number.isInteger(num4) || num4 < 8 || num4 > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num4;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num4 = +value;
              if (!Number.isInteger(num4) || num4 < 8 || num4 > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num4;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports, module) {
    "use strict";
    var { isUtf8 } = __require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = __require("utf-8-validate");
        module.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports, module) {
    "use strict";
    var { Writable } = __require("stream");
    var PerMessageDeflate2 = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxBufferedChunks = options.maxBufferedChunks | 0;
        this._maxFragments = options.maxFragments | 0;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._numFragments = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              false,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num4 = buf.readUInt32BE(0);
        if (num4 > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num4 * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          const error = this.createError(
            RangeError,
            "Too many message fragments",
            false,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._numFragments = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module.exports = Receiver2;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports, module) {
    "use strict";
    var { Duplex } = __require("stream");
    var { randomFillSync } = __require("crypto");
    var {
      types: { isUint8Array }
    } = __require("util");
    var PerMessageDeflate2 = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) return [target, data];
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (skipMasking) return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else if (isUint8Array(data)) {
            buf.set(data, 2);
          } else {
            throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports, module) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = Symbol("kCode");
    var kData = Symbol("kData");
    var kError = Symbol("kError");
    var kMessage = Symbol("kMessage");
    var kReason = Symbol("kReason");
    var kTarget = Symbol("kTarget");
    var kType = Symbol("kType");
    var kWasClean = Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension2].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events");
    var https = __require("https");
    var http2 = __require("http");
    var net = __require("net");
    var tls = __require("tls");
    var { randomBytes: randomBytes4, createHash: createHash3 } = __require("crypto");
    var { Duplex, Readable } = __require("stream");
    var { URL: URL2 } = __require("url");
    var PerMessageDeflate2 = require_permessage_deflate();
    var Receiver2 = require_receiver();
    var Sender2 = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            if (!options || options.protocols === void 0) {
              protocols = [];
            } else if (Array.isArray(options.protocols)) {
              protocols = options.protocols;
            } else {
              protocols = [options.protocols];
            }
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              if (options.protocols === void 0) {
                protocols = [];
              } else if (Array.isArray(options.protocols)) {
                protocols = options.protocols;
              } else {
                protocols = [options.protocols];
              }
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate2.extensionName]) {
          this._extensions[PerMessageDeflate2.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        this._readyState = _WebSocket.CLOSING;
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate2.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        protocols: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL2) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes4(16).toString("base64");
      const request = isSecure ? https.request : http2.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate2({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const parts = opts.path.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash3("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports, module) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = __require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null) return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused) ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module.exports = createWebSocketStream2;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1) end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1) end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events");
    var http2 = __require("http");
    var { Duplex } = __require("stream");
    var { createHash: createHash3 } = __require("crypto");
    var extension2 = require_extension();
    var PerMessageDeflate2 = require_permessage_deflate();
    var subprotocol2 = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http2.createServer((req, res) => {
            const body = http2.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension2.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate2.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]);
              extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash3("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          const params = extensions[PerMessageDeflate2.extensionName].params;
          const value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module.exports = WebSocketServer2;
    function addListeners(server, map) {
      for (const event of Object.keys(map)) server.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server) {
      server._state = CLOSED;
      server.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http2.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http2.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
      if (server.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// node_modules/postgres-array/index.js
var require_postgres_array = __commonJS({
  "node_modules/postgres-array/index.js"(exports) {
    "use strict";
    exports.parse = function(source, transform) {
      return new ArrayParser(source, transform).parse();
    };
    var ArrayParser = class _ArrayParser {
      constructor(source, transform) {
        this.source = source;
        this.transform = transform || identity;
        this.position = 0;
        this.entries = [];
        this.recorded = [];
        this.dimension = 0;
      }
      isEof() {
        return this.position >= this.source.length;
      }
      nextCharacter() {
        var character = this.source[this.position++];
        if (character === "\\") {
          return {
            value: this.source[this.position++],
            escaped: true
          };
        }
        return {
          value: character,
          escaped: false
        };
      }
      record(character) {
        this.recorded.push(character);
      }
      newEntry(includeEmpty) {
        var entry;
        if (this.recorded.length > 0 || includeEmpty) {
          entry = this.recorded.join("");
          if (entry === "NULL" && !includeEmpty) {
            entry = null;
          }
          if (entry !== null) entry = this.transform(entry);
          this.entries.push(entry);
          this.recorded = [];
        }
      }
      consumeDimensions() {
        if (this.source[0] === "[") {
          while (!this.isEof()) {
            var char = this.nextCharacter();
            if (char.value === "=") break;
          }
        }
      }
      parse(nested) {
        var character, parser, quote;
        this.consumeDimensions();
        while (!this.isEof()) {
          character = this.nextCharacter();
          if (character.value === "{" && !quote) {
            this.dimension++;
            if (this.dimension > 1) {
              parser = new _ArrayParser(this.source.substr(this.position - 1), this.transform);
              this.entries.push(parser.parse(true));
              this.position += parser.position - 2;
            }
          } else if (character.value === "}" && !quote) {
            this.dimension--;
            if (!this.dimension) {
              this.newEntry();
              if (nested) return this.entries;
            }
          } else if (character.value === '"' && !character.escaped) {
            if (quote) this.newEntry(true);
            quote = !quote;
          } else if (character.value === "," && !quote) {
            this.newEntry();
          } else {
            this.record(character.value);
          }
        }
        if (this.dimension !== 0) {
          throw new Error("array dimension not balanced");
        }
        return this.entries;
      }
    };
    function identity(value) {
      return value;
    }
  }
});

// node_modules/pg-types/lib/arrayParser.js
var require_arrayParser = __commonJS({
  "node_modules/pg-types/lib/arrayParser.js"(exports, module) {
    var array = require_postgres_array();
    module.exports = {
      create: function(source, transform) {
        return {
          parse: function() {
            return array.parse(source, transform);
          }
        };
      }
    };
  }
});

// node_modules/postgres-date/index.js
var require_postgres_date = __commonJS({
  "node_modules/postgres-date/index.js"(exports, module) {
    "use strict";
    var DATE_TIME = /(\d{1,})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(\.\d{1,})?.*?( BC)?$/;
    var DATE = /^(\d{1,})-(\d{2})-(\d{2})( BC)?$/;
    var TIME_ZONE = /([Z+-])(\d{2})?:?(\d{2})?:?(\d{2})?/;
    var INFINITY = /^-?infinity$/;
    module.exports = function parseDate(isoDate) {
      if (INFINITY.test(isoDate)) {
        return Number(isoDate.replace("i", "I"));
      }
      var matches = DATE_TIME.exec(isoDate);
      if (!matches) {
        return getDate(isoDate) || null;
      }
      var isBC = !!matches[8];
      var year = parseInt(matches[1], 10);
      if (isBC) {
        year = bcYearToNegativeYear(year);
      }
      var month = parseInt(matches[2], 10) - 1;
      var day = matches[3];
      var hour = parseInt(matches[4], 10);
      var minute = parseInt(matches[5], 10);
      var second = parseInt(matches[6], 10);
      var ms = matches[7];
      ms = ms ? 1e3 * parseFloat(ms) : 0;
      var date;
      var offset = timeZoneOffset(isoDate);
      if (offset != null) {
        date = new Date(Date.UTC(year, month, day, hour, minute, second, ms));
        if (is0To99(year)) {
          date.setUTCFullYear(year);
        }
        if (offset !== 0) {
          date.setTime(date.getTime() - offset);
        }
      } else {
        date = new Date(year, month, day, hour, minute, second, ms);
        if (is0To99(year)) {
          date.setFullYear(year);
        }
      }
      return date;
    };
    function getDate(isoDate) {
      var matches = DATE.exec(isoDate);
      if (!matches) {
        return;
      }
      var year = parseInt(matches[1], 10);
      var isBC = !!matches[4];
      if (isBC) {
        year = bcYearToNegativeYear(year);
      }
      var month = parseInt(matches[2], 10) - 1;
      var day = matches[3];
      var date = new Date(year, month, day);
      if (is0To99(year)) {
        date.setFullYear(year);
      }
      return date;
    }
    function timeZoneOffset(isoDate) {
      if (isoDate.endsWith("+00")) {
        return 0;
      }
      var zone = TIME_ZONE.exec(isoDate.split(" ")[1]);
      if (!zone) return;
      var type = zone[1];
      if (type === "Z") {
        return 0;
      }
      var sign = type === "-" ? -1 : 1;
      var offset = parseInt(zone[2], 10) * 3600 + parseInt(zone[3] || 0, 10) * 60 + parseInt(zone[4] || 0, 10);
      return offset * sign * 1e3;
    }
    function bcYearToNegativeYear(year) {
      return -(year - 1);
    }
    function is0To99(num4) {
      return num4 >= 0 && num4 < 100;
    }
  }
});

// node_modules/xtend/mutable.js
var require_mutable = __commonJS({
  "node_modules/xtend/mutable.js"(exports, module) {
    module.exports = extend;
    var hasOwnProperty = Object.prototype.hasOwnProperty;
    function extend(target) {
      for (var i = 1; i < arguments.length; i++) {
        var source = arguments[i];
        for (var key in source) {
          if (hasOwnProperty.call(source, key)) {
            target[key] = source[key];
          }
        }
      }
      return target;
    }
  }
});

// node_modules/postgres-interval/index.js
var require_postgres_interval = __commonJS({
  "node_modules/postgres-interval/index.js"(exports, module) {
    "use strict";
    var extend = require_mutable();
    module.exports = PostgresInterval;
    function PostgresInterval(raw) {
      if (!(this instanceof PostgresInterval)) {
        return new PostgresInterval(raw);
      }
      extend(this, parse(raw));
    }
    var properties = ["seconds", "minutes", "hours", "days", "months", "years"];
    PostgresInterval.prototype.toPostgres = function() {
      var filtered = properties.filter(this.hasOwnProperty, this);
      if (this.milliseconds && filtered.indexOf("seconds") < 0) {
        filtered.push("seconds");
      }
      if (filtered.length === 0) return "0";
      return filtered.map(function(property) {
        var value = this[property] || 0;
        if (property === "seconds" && this.milliseconds) {
          value = (value + this.milliseconds / 1e3).toFixed(6).replace(/\.?0+$/, "");
        }
        return value + " " + property;
      }, this).join(" ");
    };
    var propertiesISOEquivalent = {
      years: "Y",
      months: "M",
      days: "D",
      hours: "H",
      minutes: "M",
      seconds: "S"
    };
    var dateProperties = ["years", "months", "days"];
    var timeProperties = ["hours", "minutes", "seconds"];
    PostgresInterval.prototype.toISOString = PostgresInterval.prototype.toISO = function() {
      var datePart = dateProperties.map(buildProperty, this).join("");
      var timePart = timeProperties.map(buildProperty, this).join("");
      return "P" + datePart + "T" + timePart;
      function buildProperty(property) {
        var value = this[property] || 0;
        if (property === "seconds" && this.milliseconds) {
          value = (value + this.milliseconds / 1e3).toFixed(6).replace(/0+$/, "");
        }
        return value + propertiesISOEquivalent[property];
      }
    };
    var NUMBER = "([+-]?\\d+)";
    var YEAR = NUMBER + "\\s+years?";
    var MONTH = NUMBER + "\\s+mons?";
    var DAY2 = NUMBER + "\\s+days?";
    var TIME = "([+-])?([\\d]*):(\\d\\d):(\\d\\d)\\.?(\\d{1,6})?";
    var INTERVAL = new RegExp([YEAR, MONTH, DAY2, TIME].map(function(regexString) {
      return "(" + regexString + ")?";
    }).join("\\s*"));
    var positions = {
      years: 2,
      months: 4,
      days: 6,
      hours: 9,
      minutes: 10,
      seconds: 11,
      milliseconds: 12
    };
    var negatives = ["hours", "minutes", "seconds", "milliseconds"];
    function parseMilliseconds(fraction) {
      var microseconds = fraction + "000000".slice(fraction.length);
      return parseInt(microseconds, 10) / 1e3;
    }
    function parse(interval) {
      if (!interval) return {};
      var matches = INTERVAL.exec(interval);
      var isNegative = matches[8] === "-";
      return Object.keys(positions).reduce(function(parsed, property) {
        var position = positions[property];
        var value = matches[position];
        if (!value) return parsed;
        value = property === "milliseconds" ? parseMilliseconds(value) : parseInt(value, 10);
        if (!value) return parsed;
        if (isNegative && ~negatives.indexOf(property)) {
          value *= -1;
        }
        parsed[property] = value;
        return parsed;
      }, {});
    }
  }
});

// node_modules/postgres-bytea/index.js
var require_postgres_bytea = __commonJS({
  "node_modules/postgres-bytea/index.js"(exports, module) {
    "use strict";
    var bufferFrom = Buffer.from || Buffer;
    module.exports = function parseBytea(input) {
      if (/^\\x/.test(input)) {
        return bufferFrom(input.substr(2), "hex");
      }
      var output = "";
      var i = 0;
      while (i < input.length) {
        if (input[i] !== "\\") {
          output += input[i];
          ++i;
        } else {
          if (/[0-7]{3}/.test(input.substr(i + 1, 3))) {
            output += String.fromCharCode(parseInt(input.substr(i + 1, 3), 8));
            i += 4;
          } else {
            var backslashes = 1;
            while (i + backslashes < input.length && input[i + backslashes] === "\\") {
              backslashes++;
            }
            for (var k = 0; k < Math.floor(backslashes / 2); ++k) {
              output += "\\";
            }
            i += Math.floor(backslashes / 2) * 2;
          }
        }
      }
      return bufferFrom(output, "binary");
    };
  }
});

// node_modules/pg-types/lib/textParsers.js
var require_textParsers = __commonJS({
  "node_modules/pg-types/lib/textParsers.js"(exports, module) {
    var array = require_postgres_array();
    var arrayParser = require_arrayParser();
    var parseDate = require_postgres_date();
    var parseInterval = require_postgres_interval();
    var parseByteA = require_postgres_bytea();
    function allowNull(fn) {
      return function nullAllowed(value) {
        if (value === null) return value;
        return fn(value);
      };
    }
    function parseBool(value) {
      if (value === null) return value;
      return value === "TRUE" || value === "t" || value === "true" || value === "y" || value === "yes" || value === "on" || value === "1";
    }
    function parseBoolArray(value) {
      if (!value) return null;
      return array.parse(value, parseBool);
    }
    function parseBaseTenInt(string) {
      return parseInt(string, 10);
    }
    function parseIntegerArray(value) {
      if (!value) return null;
      return array.parse(value, allowNull(parseBaseTenInt));
    }
    function parseBigIntegerArray(value) {
      if (!value) return null;
      return array.parse(value, allowNull(function(entry) {
        return parseBigInteger(entry).trim();
      }));
    }
    var parsePointArray = function(value) {
      if (!value) {
        return null;
      }
      var p = arrayParser.create(value, function(entry) {
        if (entry !== null) {
          entry = parsePoint(entry);
        }
        return entry;
      });
      return p.parse();
    };
    var parseFloatArray = function(value) {
      if (!value) {
        return null;
      }
      var p = arrayParser.create(value, function(entry) {
        if (entry !== null) {
          entry = parseFloat(entry);
        }
        return entry;
      });
      return p.parse();
    };
    var parseStringArray = function(value) {
      if (!value) {
        return null;
      }
      var p = arrayParser.create(value);
      return p.parse();
    };
    var parseDateArray = function(value) {
      if (!value) {
        return null;
      }
      var p = arrayParser.create(value, function(entry) {
        if (entry !== null) {
          entry = parseDate(entry);
        }
        return entry;
      });
      return p.parse();
    };
    var parseIntervalArray = function(value) {
      if (!value) {
        return null;
      }
      var p = arrayParser.create(value, function(entry) {
        if (entry !== null) {
          entry = parseInterval(entry);
        }
        return entry;
      });
      return p.parse();
    };
    var parseByteAArray = function(value) {
      if (!value) {
        return null;
      }
      return array.parse(value, allowNull(parseByteA));
    };
    var parseInteger = function(value) {
      return parseInt(value, 10);
    };
    var parseBigInteger = function(value) {
      var valStr = String(value);
      if (/^\d+$/.test(valStr)) {
        return valStr;
      }
      return value;
    };
    var parseJsonArray = function(value) {
      if (!value) {
        return null;
      }
      return array.parse(value, allowNull(JSON.parse));
    };
    var parsePoint = function(value) {
      if (value[0] !== "(") {
        return null;
      }
      value = value.substring(1, value.length - 1).split(",");
      return {
        x: parseFloat(value[0]),
        y: parseFloat(value[1])
      };
    };
    var parseCircle = function(value) {
      if (value[0] !== "<" && value[1] !== "(") {
        return null;
      }
      var point = "(";
      var radius = "";
      var pointParsed = false;
      for (var i = 2; i < value.length - 1; i++) {
        if (!pointParsed) {
          point += value[i];
        }
        if (value[i] === ")") {
          pointParsed = true;
          continue;
        } else if (!pointParsed) {
          continue;
        }
        if (value[i] === ",") {
          continue;
        }
        radius += value[i];
      }
      var result = parsePoint(point);
      result.radius = parseFloat(radius);
      return result;
    };
    var init = function(register) {
      register(20, parseBigInteger);
      register(21, parseInteger);
      register(23, parseInteger);
      register(26, parseInteger);
      register(700, parseFloat);
      register(701, parseFloat);
      register(16, parseBool);
      register(1082, parseDate);
      register(1114, parseDate);
      register(1184, parseDate);
      register(600, parsePoint);
      register(651, parseStringArray);
      register(718, parseCircle);
      register(1e3, parseBoolArray);
      register(1001, parseByteAArray);
      register(1005, parseIntegerArray);
      register(1007, parseIntegerArray);
      register(1028, parseIntegerArray);
      register(1016, parseBigIntegerArray);
      register(1017, parsePointArray);
      register(1021, parseFloatArray);
      register(1022, parseFloatArray);
      register(1231, parseFloatArray);
      register(1014, parseStringArray);
      register(1015, parseStringArray);
      register(1008, parseStringArray);
      register(1009, parseStringArray);
      register(1040, parseStringArray);
      register(1041, parseStringArray);
      register(1115, parseDateArray);
      register(1182, parseDateArray);
      register(1185, parseDateArray);
      register(1186, parseInterval);
      register(1187, parseIntervalArray);
      register(17, parseByteA);
      register(114, JSON.parse.bind(JSON));
      register(3802, JSON.parse.bind(JSON));
      register(199, parseJsonArray);
      register(3807, parseJsonArray);
      register(3907, parseStringArray);
      register(2951, parseStringArray);
      register(791, parseStringArray);
      register(1183, parseStringArray);
      register(1270, parseStringArray);
    };
    module.exports = {
      init
    };
  }
});

// node_modules/pg-int8/index.js
var require_pg_int8 = __commonJS({
  "node_modules/pg-int8/index.js"(exports, module) {
    "use strict";
    var BASE = 1e6;
    function readInt8(buffer) {
      var high = buffer.readInt32BE(0);
      var low = buffer.readUInt32BE(4);
      var sign = "";
      if (high < 0) {
        high = ~high + (low === 0);
        low = ~low + 1 >>> 0;
        sign = "-";
      }
      var result = "";
      var carry;
      var t;
      var digits;
      var pad;
      var l;
      var i;
      {
        carry = high % BASE;
        high = high / BASE >>> 0;
        t = 4294967296 * carry + low;
        low = t / BASE >>> 0;
        digits = "" + (t - BASE * low);
        if (low === 0 && high === 0) {
          return sign + digits + result;
        }
        pad = "";
        l = 6 - digits.length;
        for (i = 0; i < l; i++) {
          pad += "0";
        }
        result = pad + digits + result;
      }
      {
        carry = high % BASE;
        high = high / BASE >>> 0;
        t = 4294967296 * carry + low;
        low = t / BASE >>> 0;
        digits = "" + (t - BASE * low);
        if (low === 0 && high === 0) {
          return sign + digits + result;
        }
        pad = "";
        l = 6 - digits.length;
        for (i = 0; i < l; i++) {
          pad += "0";
        }
        result = pad + digits + result;
      }
      {
        carry = high % BASE;
        high = high / BASE >>> 0;
        t = 4294967296 * carry + low;
        low = t / BASE >>> 0;
        digits = "" + (t - BASE * low);
        if (low === 0 && high === 0) {
          return sign + digits + result;
        }
        pad = "";
        l = 6 - digits.length;
        for (i = 0; i < l; i++) {
          pad += "0";
        }
        result = pad + digits + result;
      }
      {
        carry = high % BASE;
        t = 4294967296 * carry + low;
        digits = "" + t % BASE;
        return sign + digits + result;
      }
    }
    module.exports = readInt8;
  }
});

// node_modules/pg-types/lib/binaryParsers.js
var require_binaryParsers = __commonJS({
  "node_modules/pg-types/lib/binaryParsers.js"(exports, module) {
    var parseInt64 = require_pg_int8();
    var parseBits = function(data, bits, offset, invert, callback) {
      offset = offset || 0;
      invert = invert || false;
      callback = callback || function(lastValue, newValue, bits2) {
        return lastValue * Math.pow(2, bits2) + newValue;
      };
      var offsetBytes = offset >> 3;
      var inv = function(value) {
        if (invert) {
          return ~value & 255;
        }
        return value;
      };
      var mask = 255;
      var firstBits = 8 - offset % 8;
      if (bits < firstBits) {
        mask = 255 << 8 - bits & 255;
        firstBits = bits;
      }
      if (offset) {
        mask = mask >> offset % 8;
      }
      var result = 0;
      if (offset % 8 + bits >= 8) {
        result = callback(0, inv(data[offsetBytes]) & mask, firstBits);
      }
      var bytes = bits + offset >> 3;
      for (var i = offsetBytes + 1; i < bytes; i++) {
        result = callback(result, inv(data[i]), 8);
      }
      var lastBits = (bits + offset) % 8;
      if (lastBits > 0) {
        result = callback(result, inv(data[bytes]) >> 8 - lastBits, lastBits);
      }
      return result;
    };
    var parseFloatFromBits = function(data, precisionBits, exponentBits) {
      var bias = Math.pow(2, exponentBits - 1) - 1;
      var sign = parseBits(data, 1);
      var exponent = parseBits(data, exponentBits, 1);
      if (exponent === 0) {
        return 0;
      }
      var precisionBitsCounter = 1;
      var parsePrecisionBits = function(lastValue, newValue, bits) {
        if (lastValue === 0) {
          lastValue = 1;
        }
        for (var i = 1; i <= bits; i++) {
          precisionBitsCounter /= 2;
          if ((newValue & 1 << bits - i) > 0) {
            lastValue += precisionBitsCounter;
          }
        }
        return lastValue;
      };
      var mantissa = parseBits(data, precisionBits, exponentBits + 1, false, parsePrecisionBits);
      if (exponent == Math.pow(2, exponentBits + 1) - 1) {
        if (mantissa === 0) {
          return sign === 0 ? Infinity : -Infinity;
        }
        return NaN;
      }
      return (sign === 0 ? 1 : -1) * Math.pow(2, exponent - bias) * mantissa;
    };
    var parseInt16 = function(value) {
      if (parseBits(value, 1) == 1) {
        return -1 * (parseBits(value, 15, 1, true) + 1);
      }
      return parseBits(value, 15, 1);
    };
    var parseInt32 = function(value) {
      if (parseBits(value, 1) == 1) {
        return -1 * (parseBits(value, 31, 1, true) + 1);
      }
      return parseBits(value, 31, 1);
    };
    var parseFloat32 = function(value) {
      return parseFloatFromBits(value, 23, 8);
    };
    var parseFloat64 = function(value) {
      return parseFloatFromBits(value, 52, 11);
    };
    var parseNumeric = function(value) {
      var sign = parseBits(value, 16, 32);
      if (sign == 49152) {
        return NaN;
      }
      var weight = Math.pow(1e4, parseBits(value, 16, 16));
      var result = 0;
      var digits = [];
      var ndigits = parseBits(value, 16);
      for (var i = 0; i < ndigits; i++) {
        result += parseBits(value, 16, 64 + 16 * i) * weight;
        weight /= 1e4;
      }
      var scale = Math.pow(10, parseBits(value, 16, 48));
      return (sign === 0 ? 1 : -1) * Math.round(result * scale) / scale;
    };
    var parseDate = function(isUTC, value) {
      var sign = parseBits(value, 1);
      var rawValue = parseBits(value, 63, 1);
      var result = new Date((sign === 0 ? 1 : -1) * rawValue / 1e3 + 9466848e5);
      if (!isUTC) {
        result.setTime(result.getTime() + result.getTimezoneOffset() * 6e4);
      }
      result.usec = rawValue % 1e3;
      result.getMicroSeconds = function() {
        return this.usec;
      };
      result.setMicroSeconds = function(value2) {
        this.usec = value2;
      };
      result.getUTCMicroSeconds = function() {
        return this.usec;
      };
      return result;
    };
    var parseArray = function(value) {
      var dim = parseBits(value, 32);
      var flags = parseBits(value, 32, 32);
      var elementType = parseBits(value, 32, 64);
      var offset = 96;
      var dims = [];
      for (var i = 0; i < dim; i++) {
        dims[i] = parseBits(value, 32, offset);
        offset += 32;
        offset += 32;
      }
      var parseElement = function(elementType2) {
        var length = parseBits(value, 32, offset);
        offset += 32;
        if (length == 4294967295) {
          return null;
        }
        var result;
        if (elementType2 == 23 || elementType2 == 20) {
          result = parseBits(value, length * 8, offset);
          offset += length * 8;
          return result;
        } else if (elementType2 == 25) {
          result = value.toString(this.encoding, offset >> 3, (offset += length << 3) >> 3);
          return result;
        } else {
          console.log("ERROR: ElementType not implemented: " + elementType2);
        }
      };
      var parse = function(dimension, elementType2) {
        var array = [];
        var i2;
        if (dimension.length > 1) {
          var count2 = dimension.shift();
          for (i2 = 0; i2 < count2; i2++) {
            array[i2] = parse(dimension, elementType2);
          }
          dimension.unshift(count2);
        } else {
          for (i2 = 0; i2 < dimension[0]; i2++) {
            array[i2] = parseElement(elementType2);
          }
        }
        return array;
      };
      return parse(dims, elementType);
    };
    var parseText = function(value) {
      return value.toString("utf8");
    };
    var parseBool = function(value) {
      if (value === null) return null;
      return parseBits(value, 8) > 0;
    };
    var init = function(register) {
      register(20, parseInt64);
      register(21, parseInt16);
      register(23, parseInt32);
      register(26, parseInt32);
      register(1700, parseNumeric);
      register(700, parseFloat32);
      register(701, parseFloat64);
      register(16, parseBool);
      register(1114, parseDate.bind(null, false));
      register(1184, parseDate.bind(null, true));
      register(1e3, parseArray);
      register(1007, parseArray);
      register(1016, parseArray);
      register(1008, parseArray);
      register(1009, parseArray);
      register(25, parseText);
    };
    module.exports = {
      init
    };
  }
});

// node_modules/pg-types/lib/builtins.js
var require_builtins = __commonJS({
  "node_modules/pg-types/lib/builtins.js"(exports, module) {
    module.exports = {
      BOOL: 16,
      BYTEA: 17,
      CHAR: 18,
      INT8: 20,
      INT2: 21,
      INT4: 23,
      REGPROC: 24,
      TEXT: 25,
      OID: 26,
      TID: 27,
      XID: 28,
      CID: 29,
      JSON: 114,
      XML: 142,
      PG_NODE_TREE: 194,
      SMGR: 210,
      PATH: 602,
      POLYGON: 604,
      CIDR: 650,
      FLOAT4: 700,
      FLOAT8: 701,
      ABSTIME: 702,
      RELTIME: 703,
      TINTERVAL: 704,
      CIRCLE: 718,
      MACADDR8: 774,
      MONEY: 790,
      MACADDR: 829,
      INET: 869,
      ACLITEM: 1033,
      BPCHAR: 1042,
      VARCHAR: 1043,
      DATE: 1082,
      TIME: 1083,
      TIMESTAMP: 1114,
      TIMESTAMPTZ: 1184,
      INTERVAL: 1186,
      TIMETZ: 1266,
      BIT: 1560,
      VARBIT: 1562,
      NUMERIC: 1700,
      REFCURSOR: 1790,
      REGPROCEDURE: 2202,
      REGOPER: 2203,
      REGOPERATOR: 2204,
      REGCLASS: 2205,
      REGTYPE: 2206,
      UUID: 2950,
      TXID_SNAPSHOT: 2970,
      PG_LSN: 3220,
      PG_NDISTINCT: 3361,
      PG_DEPENDENCIES: 3402,
      TSVECTOR: 3614,
      TSQUERY: 3615,
      GTSVECTOR: 3642,
      REGCONFIG: 3734,
      REGDICTIONARY: 3769,
      JSONB: 3802,
      REGNAMESPACE: 4089,
      REGROLE: 4096
    };
  }
});

// node_modules/pg-types/index.js
var require_pg_types = __commonJS({
  "node_modules/pg-types/index.js"(exports) {
    var textParsers = require_textParsers();
    var binaryParsers = require_binaryParsers();
    var arrayParser = require_arrayParser();
    var builtinTypes = require_builtins();
    exports.getTypeParser = getTypeParser;
    exports.setTypeParser = setTypeParser;
    exports.arrayParser = arrayParser;
    exports.builtins = builtinTypes;
    var typeParsers = {
      text: {},
      binary: {}
    };
    function noParse(val) {
      return String(val);
    }
    function getTypeParser(oid, format) {
      format = format || "text";
      if (!typeParsers[format]) {
        return noParse;
      }
      return typeParsers[format][oid] || noParse;
    }
    function setTypeParser(oid, format, parseFn) {
      if (typeof format == "function") {
        parseFn = format;
        format = "text";
      }
      typeParsers[format][oid] = parseFn;
    }
    textParsers.init(function(oid, converter) {
      typeParsers.text[oid] = converter;
    });
    binaryParsers.init(function(oid, converter) {
      typeParsers.binary[oid] = converter;
    });
  }
});

// node_modules/pg/lib/defaults.js
var require_defaults = __commonJS({
  "node_modules/pg/lib/defaults.js"(exports, module) {
    "use strict";
    var user;
    try {
      user = process.platform === "win32" ? process.env.USERNAME : process.env.USER;
    } catch {
    }
    module.exports = {
      // database host. defaults to localhost
      host: "localhost",
      // database user's name
      user,
      // name of database to connect
      database: void 0,
      // database user's password
      password: null,
      // a Postgres connection string to be used instead of setting individual connection items
      // NOTE:  Setting this value will cause it to override any other value (such as database or user) defined
      // in the defaults object.
      connectionString: void 0,
      // database port
      port: 5432,
      // number of rows to return at a time from a prepared statement's
      // portal. 0 will return all rows at once
      rows: 0,
      // binary result mode
      binary: false,
      // Connection pool options - see https://github.com/brianc/node-pg-pool
      // number of connections to use in connection pool
      // 0 will disable connection pooling
      max: 10,
      // max milliseconds a client can go unused before it is removed
      // from the pool and destroyed
      idleTimeoutMillis: 3e4,
      client_encoding: "",
      ssl: false,
      // SSL negotiation style: 'postgres' (traditional SSLRequest) or 'direct'
      sslnegotiation: void 0,
      application_name: void 0,
      fallback_application_name: void 0,
      options: void 0,
      parseInputDatesAsUTC: false,
      // max milliseconds any query using this connection will execute for before timing out in error.
      // false=unlimited
      statement_timeout: false,
      // Abort any statement that waits longer than the specified duration in milliseconds while attempting to acquire a lock.
      // false=unlimited
      lock_timeout: false,
      // Terminate any session with an open transaction that has been idle for longer than the specified duration in milliseconds
      // false=unlimited
      idle_in_transaction_session_timeout: false,
      // max milliseconds to wait for query to complete (client side)
      query_timeout: false,
      connect_timeout: 0,
      keepalives: 1,
      keepalives_idle: 0
    };
    var pgTypes = require_pg_types();
    var parseBigInteger = pgTypes.getTypeParser(20, "text");
    var parseBigIntegerArray = pgTypes.getTypeParser(1016, "text");
    module.exports.__defineSetter__("parseInt8", function(val) {
      pgTypes.setTypeParser(20, "text", val ? pgTypes.getTypeParser(23, "text") : parseBigInteger);
      pgTypes.setTypeParser(1016, "text", val ? pgTypes.getTypeParser(1007, "text") : parseBigIntegerArray);
    });
  }
});

// node_modules/pg/lib/utils.js
var require_utils = __commonJS({
  "node_modules/pg/lib/utils.js"(exports, module) {
    "use strict";
    var defaults2 = require_defaults();
    var { isDate } = __require("util/types");
    function escapeElement(elementRepresentation) {
      const escaped = elementRepresentation.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
      return '"' + escaped + '"';
    }
    function arrayString(val) {
      let result = "{";
      for (let i = 0; i < val.length; i++) {
        if (i > 0) {
          result += ",";
        }
        let item = val[i];
        if (item == null) {
          result += "NULL";
        } else if (Array.isArray(item)) {
          result += arrayString(item);
        } else if (ArrayBuffer.isView(item)) {
          if (!(item instanceof Buffer)) {
            item = Buffer.from(item.buffer, item.byteOffset, item.byteLength);
          }
          result += "\\\\x" + item.toString("hex");
        } else {
          result += escapeElement(prepareValue(item));
        }
      }
      result += "}";
      return result;
    }
    var prepareValue = function(val, seen) {
      if (val == null) {
        return null;
      }
      if (typeof val === "object") {
        if (val instanceof Buffer) {
          return val;
        }
        if (ArrayBuffer.isView(val)) {
          return Buffer.from(val.buffer, val.byteOffset, val.byteLength);
        }
        if (isDate(val)) {
          if (defaults2.parseInputDatesAsUTC) {
            return dateToStringUTC(val);
          } else {
            return dateToString(val);
          }
        }
        if (Array.isArray(val)) {
          return arrayString(val);
        }
        return prepareObject(val, seen);
      }
      return val.toString();
    };
    function prepareObject(val, seen) {
      if (val && typeof val.toPostgres === "function") {
        seen = seen || [];
        if (seen.indexOf(val) !== -1) {
          throw new Error('circular reference detected while preparing "' + val + '" for query');
        }
        seen.push(val);
        return prepareValue(val.toPostgres(prepareValue), seen);
      }
      return JSON.stringify(val);
    }
    function dateToString(date) {
      let offset = -date.getTimezoneOffset();
      let year = date.getFullYear();
      const isBCYear = year < 1;
      if (isBCYear) year = Math.abs(year) + 1;
      let ret = String(year).padStart(4, "0") + "-" + String(date.getMonth() + 1).padStart(2, "0") + "-" + String(date.getDate()).padStart(2, "0") + "T" + String(date.getHours()).padStart(2, "0") + ":" + String(date.getMinutes()).padStart(2, "0") + ":" + String(date.getSeconds()).padStart(2, "0") + "." + String(date.getMilliseconds()).padStart(3, "0");
      if (offset < 0) {
        ret += "-";
        offset *= -1;
      } else {
        ret += "+";
      }
      ret += String(Math.floor(offset / 60)).padStart(2, "0") + ":" + String(offset % 60).padStart(2, "0");
      if (isBCYear) ret += " BC";
      return ret;
    }
    function dateToStringUTC(date) {
      let year = date.getUTCFullYear();
      const isBCYear = year < 1;
      if (isBCYear) year = Math.abs(year) + 1;
      let ret = String(year).padStart(4, "0") + "-" + String(date.getUTCMonth() + 1).padStart(2, "0") + "-" + String(date.getUTCDate()).padStart(2, "0") + "T" + String(date.getUTCHours()).padStart(2, "0") + ":" + String(date.getUTCMinutes()).padStart(2, "0") + ":" + String(date.getUTCSeconds()).padStart(2, "0") + "." + String(date.getUTCMilliseconds()).padStart(3, "0");
      ret += "+00:00";
      if (isBCYear) ret += " BC";
      return ret;
    }
    function normalizeQueryConfig(config, values, callback) {
      config = typeof config === "string" ? { text: config } : config;
      if (values) {
        if (typeof values === "function") {
          config.callback = values;
        } else {
          config.values = values;
        }
      }
      if (callback) {
        config.callback = callback;
      }
      return config;
    }
    var escapeIdentifier2 = function(str2) {
      return '"' + str2.replace(/"/g, '""') + '"';
    };
    var escapeLiteral2 = function(str2) {
      let hasBackslash = false;
      let escaped = "'";
      if (str2 == null) {
        return "''";
      }
      if (typeof str2 !== "string") {
        return "''";
      }
      for (let i = 0; i < str2.length; i++) {
        const c = str2[i];
        if (c === "'") {
          escaped += c + c;
        } else if (c === "\\") {
          escaped += c + c;
          hasBackslash = true;
        } else {
          escaped += c;
        }
      }
      escaped += "'";
      if (hasBackslash === true) {
        escaped = " E" + escaped;
      }
      return escaped;
    };
    module.exports = {
      prepareValue: function prepareValueWrapper(value) {
        return prepareValue(value);
      },
      normalizeQueryConfig,
      escapeIdentifier: escapeIdentifier2,
      escapeLiteral: escapeLiteral2
    };
  }
});

// node_modules/pg/lib/crypto/utils.js
var require_utils2 = __commonJS({
  "node_modules/pg/lib/crypto/utils.js"(exports, module) {
    var nodeCrypto = __require("crypto");
    module.exports = {
      postgresMd5PasswordHash,
      randomBytes: randomBytes4,
      deriveKey,
      sha256,
      hashByName,
      hmacSha256,
      md5
    };
    var webCrypto = nodeCrypto.webcrypto || globalThis.crypto;
    var subtleCrypto = webCrypto.subtle;
    var textEncoder = new TextEncoder();
    function randomBytes4(length) {
      return webCrypto.getRandomValues(Buffer.alloc(length));
    }
    async function md5(string) {
      try {
        return nodeCrypto.createHash("md5").update(string, "utf-8").digest("hex");
      } catch (e) {
        const data = typeof string === "string" ? textEncoder.encode(string) : string;
        const hash = await subtleCrypto.digest("MD5", data);
        return Array.from(new Uint8Array(hash)).map((b) => b.toString(16).padStart(2, "0")).join("");
      }
    }
    async function postgresMd5PasswordHash(user, password, salt) {
      const inner = await md5(password + user);
      const outer = await md5(Buffer.concat([Buffer.from(inner), salt]));
      return "md5" + outer;
    }
    async function sha256(text) {
      return await subtleCrypto.digest("SHA-256", text);
    }
    async function hashByName(hashName, text) {
      return await subtleCrypto.digest(hashName, text);
    }
    async function hmacSha256(keyBuffer, msg) {
      const key = await subtleCrypto.importKey("raw", keyBuffer, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      return await subtleCrypto.sign("HMAC", key, textEncoder.encode(msg));
    }
    async function deriveKey(password, salt, iterations) {
      const key = await subtleCrypto.importKey("raw", textEncoder.encode(password), "PBKDF2", false, ["deriveBits"]);
      const params = { name: "PBKDF2", hash: "SHA-256", salt, iterations };
      return await subtleCrypto.deriveBits(params, key, 32 * 8, ["deriveBits"]);
    }
  }
});

// node_modules/pg/lib/crypto/cert-signatures.js
var require_cert_signatures = __commonJS({
  "node_modules/pg/lib/crypto/cert-signatures.js"(exports, module) {
    function x509Error(msg, cert) {
      return new Error("SASL channel binding: " + msg + " when parsing public certificate " + cert.toString("base64"));
    }
    function readASN1Length(data, index) {
      let length = data[index++];
      if (length < 128) return { length, index };
      const lengthBytes = length & 127;
      if (lengthBytes > 4) throw x509Error("bad length", data);
      length = 0;
      for (let i = 0; i < lengthBytes; i++) {
        length = length << 8 | data[index++];
      }
      return { length, index };
    }
    function readASN1OID(data, index) {
      if (data[index++] !== 6) throw x509Error("non-OID data", data);
      const { length: OIDLength, index: indexAfterOIDLength } = readASN1Length(data, index);
      index = indexAfterOIDLength;
      const lastIndex = index + OIDLength;
      const byte1 = data[index++];
      let oid = (byte1 / 40 >> 0) + "." + byte1 % 40;
      while (index < lastIndex) {
        let value = 0;
        while (index < lastIndex) {
          const nextByte = data[index++];
          value = value << 7 | nextByte & 127;
          if (nextByte < 128) break;
        }
        oid += "." + value;
      }
      return { oid, index };
    }
    function expectASN1Seq(data, index) {
      if (data[index++] !== 48) throw x509Error("non-sequence data", data);
      return readASN1Length(data, index);
    }
    function signatureAlgorithmHashFromCertificate(data, index) {
      if (index === void 0) index = 0;
      index = expectASN1Seq(data, index).index;
      const { length: certInfoLength, index: indexAfterCertInfoLength } = expectASN1Seq(data, index);
      index = indexAfterCertInfoLength + certInfoLength;
      index = expectASN1Seq(data, index).index;
      const { oid, index: indexAfterOID } = readASN1OID(data, index);
      switch (oid) {
        // RSA
        case "1.2.840.113549.1.1.4":
          return "MD5";
        case "1.2.840.113549.1.1.5":
          return "SHA-1";
        case "1.2.840.113549.1.1.11":
          return "SHA-256";
        case "1.2.840.113549.1.1.12":
          return "SHA-384";
        case "1.2.840.113549.1.1.13":
          return "SHA-512";
        case "1.2.840.113549.1.1.14":
          return "SHA-224";
        case "1.2.840.113549.1.1.15":
          return "SHA512-224";
        case "1.2.840.113549.1.1.16":
          return "SHA512-256";
        // ECDSA
        case "1.2.840.10045.4.1":
          return "SHA-1";
        case "1.2.840.10045.4.3.1":
          return "SHA-224";
        case "1.2.840.10045.4.3.2":
          return "SHA-256";
        case "1.2.840.10045.4.3.3":
          return "SHA-384";
        case "1.2.840.10045.4.3.4":
          return "SHA-512";
        // RSASSA-PSS: hash is indicated separately
        case "1.2.840.113549.1.1.10": {
          index = indexAfterOID;
          index = expectASN1Seq(data, index).index;
          if (data[index++] !== 160) throw x509Error("non-tag data", data);
          index = readASN1Length(data, index).index;
          index = expectASN1Seq(data, index).index;
          const { oid: hashOID } = readASN1OID(data, index);
          switch (hashOID) {
            // standalone hash OIDs
            case "1.2.840.113549.2.5":
              return "MD5";
            case "1.3.14.3.2.26":
              return "SHA-1";
            case "2.16.840.1.101.3.4.2.1":
              return "SHA-256";
            case "2.16.840.1.101.3.4.2.2":
              return "SHA-384";
            case "2.16.840.1.101.3.4.2.3":
              return "SHA-512";
          }
          throw x509Error("unknown hash OID " + hashOID, data);
        }
        // Ed25519 -- see https: return//github.com/openssl/openssl/issues/15477
        case "1.3.101.110":
        case "1.3.101.112":
          return "SHA-512";
        // Ed448 -- still not in pg 17.2 (if supported, digest would be SHAKE256 x 64 bytes)
        case "1.3.101.111":
        case "1.3.101.113":
          throw x509Error("Ed448 certificate channel binding is not currently supported by Postgres");
      }
      throw x509Error("unknown OID " + oid, data);
    }
    module.exports = { signatureAlgorithmHashFromCertificate };
  }
});

// node_modules/pg/lib/crypto/sasl.js
var require_sasl = __commonJS({
  "node_modules/pg/lib/crypto/sasl.js"(exports, module) {
    "use strict";
    var crypto = require_utils2();
    var { signatureAlgorithmHashFromCertificate } = require_cert_signatures();
    function saslprep(password) {
      const nonAsciiSpace = /[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]/g;
      const mappedToNothing = /[\u00AD\u034F\u1806\u180B\u180C\u180D\u200C\u200D\u2060\uFE00-\uFE0F\uFEFF]/g;
      return password.replace(nonAsciiSpace, " ").replace(mappedToNothing, "").normalize("NFKC");
    }
    var DEFAULT_MAX_SCRAM_ITERATIONS = 1e5;
    function startSession(mechanisms, stream, scramMaxIterations = DEFAULT_MAX_SCRAM_ITERATIONS) {
      const candidates = ["SCRAM-SHA-256"];
      if (stream) candidates.unshift("SCRAM-SHA-256-PLUS");
      const mechanism = candidates.find((candidate2) => mechanisms.includes(candidate2));
      if (!mechanism) {
        throw new Error("SASL: Only mechanism(s) " + candidates.join(" and ") + " are supported");
      }
      if (mechanism === "SCRAM-SHA-256-PLUS" && typeof stream.getPeerCertificate !== "function") {
        throw new Error("SASL: Mechanism SCRAM-SHA-256-PLUS requires a certificate");
      }
      const clientNonce = crypto.randomBytes(18).toString("base64");
      const gs2Header = mechanism === "SCRAM-SHA-256-PLUS" ? "p=tls-server-end-point" : stream ? "y" : "n";
      return {
        mechanism,
        clientNonce,
        response: gs2Header + ",,n=*,r=" + clientNonce,
        message: "SASLInitialResponse",
        scramMaxIterations
      };
    }
    async function continueSession(session, password, serverData, stream) {
      if (session.message !== "SASLInitialResponse") {
        throw new Error("SASL: Last message was not SASLInitialResponse");
      }
      if (typeof password !== "string") {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: client password must be a string");
      }
      if (password === "") {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: client password must be a non-empty string");
      }
      if (typeof serverData !== "string") {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: serverData must be a string");
      }
      const sv = parseServerFirstMessage(serverData);
      if (!sv.nonce.startsWith(session.clientNonce)) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: server nonce does not start with client nonce");
      } else if (sv.nonce.length === session.clientNonce.length) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: server nonce is too short");
      }
      const scramMaxIterations = typeof session.scramMaxIterations === "number" ? session.scramMaxIterations : DEFAULT_MAX_SCRAM_ITERATIONS;
      if (scramMaxIterations !== 0 && sv.iteration > scramMaxIterations) {
        throw new Error(
          "SASL: SCRAM-SERVER-FIRST-MESSAGE: iteration count " + sv.iteration + " exceeds scramMaxIterations of " + scramMaxIterations
        );
      }
      const clientFirstMessageBare = "n=*,r=" + session.clientNonce;
      const serverFirstMessage = "r=" + sv.nonce + ",s=" + sv.salt + ",i=" + sv.iteration;
      let channelBinding = stream ? "eSws" : "biws";
      if (session.mechanism === "SCRAM-SHA-256-PLUS") {
        const peerCert = stream.getPeerCertificate().raw;
        let hashName = signatureAlgorithmHashFromCertificate(peerCert);
        if (hashName === "MD5" || hashName === "SHA-1") hashName = "SHA-256";
        const certHash = await crypto.hashByName(hashName, peerCert);
        const bindingData = Buffer.concat([Buffer.from("p=tls-server-end-point,,"), Buffer.from(certHash)]);
        channelBinding = bindingData.toString("base64");
      }
      const clientFinalMessageWithoutProof = "c=" + channelBinding + ",r=" + sv.nonce;
      const authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
      const saltBytes = Buffer.from(sv.salt, "base64");
      const saltedPassword = await crypto.deriveKey(saslprep(password), saltBytes, sv.iteration);
      const clientKey = await crypto.hmacSha256(saltedPassword, "Client Key");
      const storedKey = await crypto.sha256(clientKey);
      const clientSignature = await crypto.hmacSha256(storedKey, authMessage);
      const clientProof = xorBuffers(Buffer.from(clientKey), Buffer.from(clientSignature)).toString("base64");
      const serverKey = await crypto.hmacSha256(saltedPassword, "Server Key");
      const serverSignatureBytes = await crypto.hmacSha256(serverKey, authMessage);
      session.message = "SASLResponse";
      session.serverSignature = Buffer.from(serverSignatureBytes).toString("base64");
      session.response = clientFinalMessageWithoutProof + ",p=" + clientProof;
    }
    function finalizeSession(session, serverData) {
      if (session.message !== "SASLResponse") {
        throw new Error("SASL: Last message was not SASLResponse");
      }
      if (typeof serverData !== "string") {
        throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: serverData must be a string");
      }
      const { serverSignature } = parseServerFinalMessage(serverData);
      if (serverSignature !== session.serverSignature) {
        throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature does not match");
      }
    }
    function isPrintableChars(text) {
      if (typeof text !== "string") {
        throw new TypeError("SASL: text must be a string");
      }
      return text.split("").map((_, i) => text.charCodeAt(i)).every((c) => c >= 33 && c <= 43 || c >= 45 && c <= 126);
    }
    function isBase64(text) {
      return /^(?:[a-zA-Z0-9+/]{4})*(?:[a-zA-Z0-9+/]{2}==|[a-zA-Z0-9+/]{3}=)?$/.test(text);
    }
    function parseAttributePairs(text) {
      if (typeof text !== "string") {
        throw new TypeError("SASL: attribute pairs text must be a string");
      }
      return new Map(
        text.split(",").map((attrValue) => {
          if (!/^.=/.test(attrValue)) {
            throw new Error("SASL: Invalid attribute pair entry");
          }
          const name = attrValue[0];
          const value = attrValue.substring(2);
          return [name, value];
        })
      );
    }
    function parseServerFirstMessage(data) {
      const attrPairs = parseAttributePairs(data);
      const nonce = attrPairs.get("r");
      if (!nonce) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: nonce missing");
      } else if (!isPrintableChars(nonce)) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: nonce must only contain printable characters");
      }
      const salt = attrPairs.get("s");
      if (!salt) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: salt missing");
      } else if (!isBase64(salt)) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: salt must be base64");
      }
      const iterationText = attrPairs.get("i");
      if (!iterationText) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: iteration missing");
      } else if (!/^[1-9][0-9]*$/.test(iterationText)) {
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: invalid iteration count");
      }
      const iteration = parseInt(iterationText, 10);
      return {
        nonce,
        salt,
        iteration
      };
    }
    function parseServerFinalMessage(serverData) {
      const attrPairs = parseAttributePairs(serverData);
      const error = attrPairs.get("e");
      const serverSignature = attrPairs.get("v");
      if (error) {
        throw new Error(`SASL: SCRAM-SERVER-FINAL-MESSAGE: server returned error: "${error}"`);
      }
      if (!serverSignature) {
        throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature is missing");
      } else if (!isBase64(serverSignature)) {
        throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature must be base64");
      }
      return {
        serverSignature
      };
    }
    function xorBuffers(a, b) {
      if (!Buffer.isBuffer(a)) {
        throw new TypeError("first argument must be a Buffer");
      }
      if (!Buffer.isBuffer(b)) {
        throw new TypeError("second argument must be a Buffer");
      }
      if (a.length !== b.length) {
        throw new Error("Buffer lengths must match");
      }
      if (a.length === 0) {
        throw new Error("Buffers cannot be empty");
      }
      return Buffer.from(a.map((_, i) => a[i] ^ b[i]));
    }
    module.exports = {
      startSession,
      continueSession,
      finalizeSession,
      DEFAULT_MAX_SCRAM_ITERATIONS
    };
  }
});

// node_modules/pg/lib/type-overrides.js
var require_type_overrides = __commonJS({
  "node_modules/pg/lib/type-overrides.js"(exports, module) {
    "use strict";
    var types2 = require_pg_types();
    function TypeOverrides2(userTypes) {
      this._types = userTypes || types2;
      this.text = {};
      this.binary = {};
    }
    TypeOverrides2.prototype.getOverrides = function(format) {
      switch (format) {
        case "text":
          return this.text;
        case "binary":
          return this.binary;
        default:
          return {};
      }
    };
    TypeOverrides2.prototype.setTypeParser = function(oid, format, parseFn) {
      if (typeof format === "function") {
        parseFn = format;
        format = "text";
      }
      this.getOverrides(format)[oid] = parseFn;
    };
    TypeOverrides2.prototype.getTypeParser = function(oid, format) {
      format = format || "text";
      return this.getOverrides(format)[oid] || this._types.getTypeParser(oid, format);
    };
    module.exports = TypeOverrides2;
  }
});

// node_modules/pg-connection-string/index.js
var require_pg_connection_string = __commonJS({
  "node_modules/pg-connection-string/index.js"(exports, module) {
    "use strict";
    function parse(str2, options = {}) {
      if (str2.charAt(0) === "/") {
        const config2 = str2.split(" ");
        return { host: config2[0], database: config2[1] };
      }
      const config = /* @__PURE__ */ Object.create(null);
      let result;
      let dummyHost = false;
      if (/ |%[^a-f0-9]|%[a-f0-9][^a-f0-9]/i.test(str2)) {
        str2 = encodeURI(str2).replace(/%25(\d\d)/g, "%$1");
      }
      try {
        try {
          result = new URL(str2, "postgres://base");
        } catch (e) {
          result = new URL(str2.replace("@/", "@___DUMMY___/"), "postgres://base");
          dummyHost = true;
        }
      } catch (err) {
        err.input && (err.input = "*****REDACTED*****");
        throw err;
      }
      for (const entry of result.searchParams.entries()) {
        config[entry[0]] = entry[1];
      }
      config.user = config.user || decodeURIComponent(result.username);
      config.password = config.password || decodeURIComponent(result.password);
      if (result.protocol == "socket:") {
        config.host = decodeURI(result.pathname);
        config.database = result.searchParams.get("db");
        config.client_encoding = result.searchParams.get("encoding");
        return config;
      }
      const hostname = dummyHost ? "" : result.hostname;
      if (!config.host) {
        config.host = decodeURIComponent(hostname);
      } else if (hostname && /^%2f/i.test(hostname)) {
        result.pathname = hostname + result.pathname;
      }
      if (!config.port) {
        config.port = result.port;
      }
      const pathname = result.pathname.slice(1) || null;
      config.database = pathname ? decodeURI(pathname) : null;
      if (config.ssl === "true" || config.ssl === "1") {
        config.ssl = true;
      }
      if (config.ssl === "0") {
        config.ssl = false;
      }
      if (config.sslcert || config.sslkey || config.sslrootcert || config.sslmode) {
        config.ssl = {};
      }
      if (config.sslnegotiation === "direct" && config.ssl === void 0) {
        config.ssl = true;
      }
      const fs = config.sslcert || config.sslkey || config.sslrootcert ? __require("fs") : null;
      if (config.sslcert) {
        config.ssl.cert = fs.readFileSync(config.sslcert).toString();
      }
      if (config.sslkey) {
        config.ssl.key = fs.readFileSync(config.sslkey).toString();
      }
      if (config.sslrootcert) {
        config.ssl.ca = fs.readFileSync(config.sslrootcert).toString();
      }
      if (options.useLibpqCompat && config.uselibpqcompat) {
        throw new Error("Both useLibpqCompat and uselibpqcompat are set. Please use only one of them.");
      }
      if (config.uselibpqcompat === "true" || options.useLibpqCompat) {
        switch (config.sslmode) {
          case "disable": {
            config.ssl = false;
            break;
          }
          case "prefer": {
            config.ssl.rejectUnauthorized = false;
            break;
          }
          case "require": {
            if (config.sslrootcert) {
              config.ssl.checkServerIdentity = function() {
              };
            } else {
              config.ssl.rejectUnauthorized = false;
            }
            break;
          }
          case "verify-ca": {
            if (!config.ssl.ca) {
              throw new Error(
                "SECURITY WARNING: Using sslmode=verify-ca requires specifying a CA with sslrootcert. If a public CA is used, verify-ca allows connections to a server that somebody else may have registered with the CA, making you vulnerable to Man-in-the-Middle attacks. Either specify a custom CA certificate with sslrootcert parameter or use sslmode=verify-full for proper security."
              );
            }
            config.ssl.checkServerIdentity = function() {
            };
            break;
          }
          case "verify-full": {
            break;
          }
        }
      } else {
        switch (config.sslmode) {
          case "disable": {
            config.ssl = false;
            break;
          }
          case "prefer":
          case "require":
          case "verify-ca":
          case "verify-full": {
            if (config.sslmode !== "verify-full") {
              deprecatedSslModeWarning(config.sslmode);
            }
            break;
          }
          case "no-verify": {
            config.ssl.rejectUnauthorized = false;
            break;
          }
        }
      }
      return config;
    }
    function toConnectionOptions(sslConfig) {
      const connectionOptions = Object.entries(sslConfig).reduce((c, [key, value]) => {
        if (value !== void 0 && value !== null) {
          c[key] = value;
        }
        return c;
      }, /* @__PURE__ */ Object.create(null));
      return connectionOptions;
    }
    function toClientConfig(config) {
      const poolConfig = Object.entries(config).reduce((c, [key, value]) => {
        if (key === "ssl") {
          const sslConfig = value;
          if (typeof sslConfig === "boolean") {
            c[key] = sslConfig;
          }
          if (typeof sslConfig === "object") {
            c[key] = toConnectionOptions(sslConfig);
          }
        } else if (value !== void 0 && value !== null) {
          if (key === "port") {
            if (value !== "") {
              const v = parseInt(value, 10);
              if (isNaN(v)) {
                throw new Error(`Invalid ${key}: ${value}`);
              }
              c[key] = v;
            }
          } else {
            c[key] = value;
          }
        }
        return c;
      }, /* @__PURE__ */ Object.create(null));
      return poolConfig;
    }
    function parseIntoClientConfig(str2) {
      return toClientConfig(parse(str2));
    }
    function deprecatedSslModeWarning(sslmode) {
      if (!deprecatedSslModeWarning.warned && typeof process !== "undefined" && process.emitWarning) {
        deprecatedSslModeWarning.warned = true;
        process.emitWarning(`SECURITY WARNING: The SSL modes 'prefer', 'require', and 'verify-ca' are treated as aliases for 'verify-full'.
In the next major version (pg-connection-string v3.0.0 and pg v9.0.0), these modes will adopt standard libpq semantics, which have weaker security guarantees.

To prepare for this change:
- If you want the current behavior, explicitly use 'sslmode=verify-full'
- If you want libpq compatibility now, use 'uselibpqcompat=true&sslmode=${sslmode}'

See https://www.postgresql.org/docs/current/libpq-ssl.html for libpq SSL mode definitions.`);
      }
    }
    module.exports = parse;
    parse.parse = parse;
    parse.toClientConfig = toClientConfig;
    parse.parseIntoClientConfig = parseIntoClientConfig;
  }
});

// node_modules/pg/lib/connection-parameters.js
var require_connection_parameters = __commonJS({
  "node_modules/pg/lib/connection-parameters.js"(exports, module) {
    "use strict";
    var dns = __require("dns");
    var defaults2 = require_defaults();
    var parse = require_pg_connection_string().parse;
    var val = function(key, config, envVar) {
      if (config[key]) {
        return config[key];
      }
      if (envVar === void 0) {
        envVar = process.env["PG" + key.toUpperCase()];
      } else if (envVar === false) {
      } else {
        envVar = process.env[envVar];
      }
      return envVar || defaults2[key];
    };
    var readSSLConfigFromEnvironment = function() {
      switch (process.env.PGSSLMODE) {
        case "disable":
          return false;
        case "prefer":
        case "require":
        case "verify-ca":
        case "verify-full":
          return true;
        case "no-verify":
          return { rejectUnauthorized: false };
      }
      return defaults2.ssl;
    };
    var quoteParamValue = function(value) {
      return "'" + ("" + value).replace(/\\/g, "\\\\").replace(/'/g, "\\'") + "'";
    };
    var add = function(params, config, paramName) {
      const value = config[paramName];
      if (value !== void 0 && value !== null) {
        params.push(paramName + "=" + quoteParamValue(value));
      }
    };
    var ConnectionParameters = class {
      constructor(config) {
        config = typeof config === "string" ? parse(config) : config || {};
        if (config.connectionString) {
          config = Object.assign({}, config, parse(config.connectionString));
        }
        this.user = val("user", config);
        this.database = val("database", config);
        if (this.database === void 0) {
          this.database = this.user;
        }
        this.port = parseInt(val("port", config), 10);
        this.host = val("host", config);
        Object.defineProperty(this, "password", {
          configurable: true,
          enumerable: false,
          writable: true,
          value: val("password", config)
        });
        this.binary = val("binary", config);
        this.options = val("options", config);
        this.ssl = typeof config.ssl === "undefined" ? readSSLConfigFromEnvironment() : config.ssl;
        if (typeof this.ssl === "string") {
          if (this.ssl === "true") {
            this.ssl = true;
          }
        }
        if (this.ssl === "no-verify") {
          this.ssl = { rejectUnauthorized: false };
        }
        if (this.ssl && this.ssl.key) {
          Object.defineProperty(this.ssl, "key", {
            enumerable: false
          });
        }
        this.sslnegotiation = val("sslnegotiation", config, "PGSSLNEGOTIATION");
        if (this.sslnegotiation !== void 0 && this.sslnegotiation !== "postgres" && this.sslnegotiation !== "direct") {
          throw new Error(
            `Invalid sslnegotiation value: "${this.sslnegotiation}". Valid values are "postgres" and "direct".`
          );
        }
        if (this.sslnegotiation === "direct" && !this.ssl) {
          throw new Error("sslnegotiation=direct requires SSL to be enabled");
        }
        this.client_encoding = val("client_encoding", config);
        this.replication = val("replication", config);
        this.isDomainSocket = !(this.host || "").indexOf("/");
        this.application_name = val("application_name", config, "PGAPPNAME");
        this.fallback_application_name = val("fallback_application_name", config, false);
        this.statement_timeout = val("statement_timeout", config, false);
        this.lock_timeout = val("lock_timeout", config, false);
        this.idle_in_transaction_session_timeout = val("idle_in_transaction_session_timeout", config, false);
        this.query_timeout = val("query_timeout", config, false);
        if (config.connectionTimeoutMillis === void 0) {
          this.connect_timeout = process.env.PGCONNECT_TIMEOUT || 0;
        } else {
          this.connect_timeout = Math.floor(config.connectionTimeoutMillis / 1e3);
        }
        if (config.keepAlive === false) {
          this.keepalives = 0;
        } else if (config.keepAlive === true) {
          this.keepalives = 1;
        }
        if (typeof config.keepAliveInitialDelayMillis === "number") {
          this.keepalives_idle = Math.floor(config.keepAliveInitialDelayMillis / 1e3);
        }
      }
      getLibpqConnectionString(cb) {
        const params = [];
        add(params, this, "user");
        add(params, this, "password");
        add(params, this, "port");
        add(params, this, "application_name");
        add(params, this, "fallback_application_name");
        add(params, this, "connect_timeout");
        add(params, this, "options");
        const ssl = typeof this.ssl === "object" ? this.ssl : this.ssl ? { sslmode: this.ssl } : {};
        add(params, ssl, "sslmode");
        add(params, ssl, "sslca");
        add(params, ssl, "sslkey");
        add(params, ssl, "sslcert");
        add(params, ssl, "sslrootcert");
        add(params, this, "sslnegotiation");
        if (this.database) {
          params.push("dbname=" + quoteParamValue(this.database));
        }
        if (this.replication) {
          params.push("replication=" + quoteParamValue(this.replication));
        }
        if (this.host) {
          params.push("host=" + quoteParamValue(this.host));
        }
        if (this.isDomainSocket) {
          return cb(null, params.join(" "));
        }
        if (this.client_encoding) {
          params.push("client_encoding=" + quoteParamValue(this.client_encoding));
        }
        dns.lookup(this.host, function(err, address) {
          if (err) return cb(err, null);
          params.push("hostaddr=" + quoteParamValue(address));
          return cb(null, params.join(" "));
        });
      }
    };
    module.exports = ConnectionParameters;
  }
});

// node_modules/pg/lib/result.js
var require_result = __commonJS({
  "node_modules/pg/lib/result.js"(exports, module) {
    "use strict";
    var types2 = require_pg_types();
    var matchRegexp = /^([A-Za-z]+)(?: (\d+))?(?: (\d+))?/;
    var Result2 = class {
      constructor(rowMode, types3) {
        this.command = null;
        this.rowCount = null;
        this.oid = null;
        this.rows = [];
        this.fields = [];
        this._parsers = void 0;
        this._types = types3;
        this.RowCtor = null;
        this.rowAsArray = rowMode === "array";
        if (this.rowAsArray) {
          this.parseRow = this._parseRowAsArray;
        }
        this._prebuiltEmptyResultObject = null;
      }
      // adds a command complete message
      addCommandComplete(msg) {
        let match;
        if (msg.text) {
          match = matchRegexp.exec(msg.text);
        } else {
          match = matchRegexp.exec(msg.command);
        }
        if (match) {
          this.command = match[1];
          if (match[3]) {
            this.oid = parseInt(match[2], 10);
            this.rowCount = parseInt(match[3], 10);
          } else if (match[2]) {
            this.rowCount = parseInt(match[2], 10);
          }
        }
      }
      _parseRowAsArray(rowData) {
        const row = new Array(rowData.length);
        for (let i = 0, len = rowData.length; i < len; i++) {
          const rawValue = rowData[i];
          if (rawValue !== null) {
            row[i] = this._parsers[i](rawValue);
          } else {
            row[i] = null;
          }
        }
        return row;
      }
      parseRow(rowData) {
        const row = { ...this._prebuiltEmptyResultObject };
        for (let i = 0, len = rowData.length; i < len; i++) {
          const rawValue = rowData[i];
          const field = this.fields[i].name;
          if (rawValue !== null) {
            const v = this.fields[i].format === "binary" ? Buffer.from(rawValue) : rawValue;
            row[field] = this._parsers[i](v);
          } else {
            row[field] = null;
          }
        }
        return row;
      }
      addRow(row) {
        this.rows.push(row);
      }
      addFields(fieldDescriptions) {
        this.fields = fieldDescriptions;
        if (this.fields.length) {
          this._parsers = new Array(fieldDescriptions.length);
        }
        const row = /* @__PURE__ */ Object.create(null);
        for (let i = 0; i < fieldDescriptions.length; i++) {
          const desc = fieldDescriptions[i];
          row[desc.name] = null;
          if (this._types) {
            this._parsers[i] = this._types.getTypeParser(desc.dataTypeID, desc.format || "text");
          } else {
            this._parsers[i] = types2.getTypeParser(desc.dataTypeID, desc.format || "text");
          }
        }
        this._prebuiltEmptyResultObject = { ...row };
      }
    };
    module.exports = Result2;
  }
});

// node_modules/pg/lib/query.js
var require_query = __commonJS({
  "node_modules/pg/lib/query.js"(exports, module) {
    "use strict";
    var { EventEmitter } = __require("events");
    var Result2 = require_result();
    var utils = require_utils();
    var Query2 = class extends EventEmitter {
      constructor(config, values, callback) {
        super();
        config = utils.normalizeQueryConfig(config, values, callback);
        this.text = config.text;
        this.values = config.values;
        this.rows = config.rows;
        this.types = config.types;
        this.name = config.name;
        this.queryMode = config.queryMode;
        this.binary = config.binary;
        this.portal = config.portal || "";
        this.callback = config.callback;
        this._rowMode = config.rowMode;
        if (process.domain && config.callback) {
          this.callback = process.domain.bind(config.callback);
        }
        this._result = new Result2(this._rowMode, this.types);
        this._results = this._result;
        this._canceledDueToError = false;
      }
      requiresPreparation() {
        if (this.queryMode === "extended") {
          return true;
        }
        if (this.name) {
          return true;
        }
        if (this.rows) {
          return true;
        }
        if (!this.text) {
          return false;
        }
        if (!this.values) {
          return false;
        }
        return this.values.length > 0;
      }
      _checkForMultirow() {
        if (this._result.command) {
          if (!Array.isArray(this._results)) {
            this._results = [this._result];
          }
          this._result = new Result2(this._rowMode, this._result._types);
          this._results.push(this._result);
        }
      }
      // associates row metadata from the supplied
      // message with this query object
      // metadata used when parsing row results
      handleRowDescription(msg) {
        this._checkForMultirow();
        this._result.addFields(msg.fields);
        this._accumulateRows = this.callback || !this.listeners("row").length;
      }
      handleDataRow(msg) {
        let row;
        if (this._canceledDueToError) {
          return;
        }
        try {
          row = this._result.parseRow(msg.fields);
        } catch (err) {
          this._canceledDueToError = err;
          return;
        }
        this.emit("row", row, this._result);
        if (this._accumulateRows) {
          this._result.addRow(row);
        }
      }
      handleCommandComplete(msg, connection) {
        this._checkForMultirow();
        this._result.addCommandComplete(msg);
        if (this.rows) {
          connection.sync();
        }
      }
      // if a named prepared statement is created with empty query text
      // the backend will send an emptyQuery message but *not* a command complete message
      // since we pipeline sync immediately after execute we don't need to do anything here
      // unless we have rows specified, in which case we did not pipeline the initial sync call
      handleEmptyQuery(connection) {
        if (this.rows) {
          connection.sync();
        }
      }
      handleError(err, connection) {
        if (this._canceledDueToError) {
          err = this._canceledDueToError;
          this._canceledDueToError = false;
        }
        if (this.callback) {
          return this.callback(err);
        }
        this.emit("error", err);
      }
      handleReadyForQuery(con) {
        if (this._canceledDueToError) {
          return this.handleError(this._canceledDueToError, con);
        }
        if (this.callback) {
          try {
            this.callback(null, this._results);
          } catch (err) {
            process.nextTick(() => {
              throw err;
            });
          }
        }
        this.emit("end", this._results);
      }
      submit(connection) {
        if (typeof this.text !== "string" && typeof this.name !== "string") {
          return new Error("A query must have either text or a name. Supplying neither is unsupported.");
        }
        const previous = connection.parsedStatements[this.name] || connection.submittedNamedStatements[this.name];
        if (this.text && previous && this.text !== previous) {
          return new Error(`Prepared statements must be unique - '${this.name}' was used for a different statement`);
        }
        if (this.values && !Array.isArray(this.values)) {
          return new Error("Query values must be an array");
        }
        if (this.requiresPreparation()) {
          connection.stream.cork && connection.stream.cork();
          try {
            this.prepare(connection);
          } finally {
            connection.stream.uncork && connection.stream.uncork();
          }
        } else {
          connection.query(this.text);
        }
        return null;
      }
      hasBeenParsed(connection) {
        return this.name && (connection.parsedStatements[this.name] || connection.submittedNamedStatements[this.name]);
      }
      handlePortalSuspended(connection) {
        this._getRows(connection, this.rows);
      }
      _getRows(connection, rows) {
        connection.execute({
          portal: this.portal,
          rows
        });
        if (!rows) {
          connection.sync();
        } else {
          connection.flush();
        }
      }
      // http://developer.postgresql.org/pgdocs/postgres/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
      prepare(connection) {
        if (!this.hasBeenParsed(connection)) {
          connection.parse({
            text: this.text,
            name: this.name,
            types: this.types
          });
          if (this.name) {
            connection.submittedNamedStatements[this.name] = this.text;
          }
        }
        try {
          connection.bind({
            portal: this.portal,
            statement: this.name,
            values: this.values,
            binary: this.binary,
            valueMapper: utils.prepareValue
          });
        } catch (err) {
          connection.close({ type: "S", name: this.name });
          connection.sync();
          this.handleError(err, connection);
          return;
        }
        connection.describe({
          type: "P",
          name: this.portal || ""
        });
        this._getRows(connection, this.rows);
      }
      handleCopyInResponse(connection) {
        connection.sendCopyFail("No source stream defined");
      }
      handleCopyData(msg, connection) {
      }
    };
    module.exports = Query2;
  }
});

// node_modules/pg-protocol/dist/messages.js
var require_messages = __commonJS({
  "node_modules/pg-protocol/dist/messages.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.NoticeMessage = exports.DataRowMessage = exports.CommandCompleteMessage = exports.ReadyForQueryMessage = exports.NotificationResponseMessage = exports.BackendKeyDataMessage = exports.AuthenticationMD5Password = exports.ParameterStatusMessage = exports.ParameterDescriptionMessage = exports.RowDescriptionMessage = exports.Field = exports.CopyResponse = exports.CopyDataMessage = exports.DatabaseError = exports.copyDone = exports.emptyQuery = exports.replicationStart = exports.portalSuspended = exports.noData = exports.closeComplete = exports.bindComplete = exports.parseComplete = void 0;
    exports.parseComplete = {
      name: "parseComplete",
      length: 5
    };
    exports.bindComplete = {
      name: "bindComplete",
      length: 5
    };
    exports.closeComplete = {
      name: "closeComplete",
      length: 5
    };
    exports.noData = {
      name: "noData",
      length: 5
    };
    exports.portalSuspended = {
      name: "portalSuspended",
      length: 5
    };
    exports.replicationStart = {
      name: "replicationStart",
      length: 4
    };
    exports.emptyQuery = {
      name: "emptyQuery",
      length: 4
    };
    exports.copyDone = {
      name: "copyDone",
      length: 4
    };
    var DatabaseError2 = class extends Error {
      constructor(message, length, name) {
        super(message);
        this.length = length;
        this.name = name;
      }
    };
    exports.DatabaseError = DatabaseError2;
    var CopyDataMessage = class {
      constructor(length, chunk) {
        this.length = length;
        this.chunk = chunk;
        this.name = "copyData";
      }
    };
    exports.CopyDataMessage = CopyDataMessage;
    var CopyResponse = class {
      constructor(length, name, binary, columnCount) {
        this.length = length;
        this.name = name;
        this.binary = binary;
        this.columnTypes = new Array(columnCount);
      }
    };
    exports.CopyResponse = CopyResponse;
    var Field = class {
      constructor(name, tableID, columnID, dataTypeID, dataTypeSize, dataTypeModifier, format) {
        this.name = name;
        this.tableID = tableID;
        this.columnID = columnID;
        this.dataTypeID = dataTypeID;
        this.dataTypeSize = dataTypeSize;
        this.dataTypeModifier = dataTypeModifier;
        this.format = format;
      }
    };
    exports.Field = Field;
    var RowDescriptionMessage = class {
      constructor(length, fieldCount) {
        this.length = length;
        this.fieldCount = fieldCount;
        this.name = "rowDescription";
        this.fields = new Array(this.fieldCount);
      }
    };
    exports.RowDescriptionMessage = RowDescriptionMessage;
    var ParameterDescriptionMessage = class {
      constructor(length, parameterCount) {
        this.length = length;
        this.parameterCount = parameterCount;
        this.name = "parameterDescription";
        this.dataTypeIDs = new Array(this.parameterCount);
      }
    };
    exports.ParameterDescriptionMessage = ParameterDescriptionMessage;
    var ParameterStatusMessage = class {
      constructor(length, parameterName, parameterValue) {
        this.length = length;
        this.parameterName = parameterName;
        this.parameterValue = parameterValue;
        this.name = "parameterStatus";
      }
    };
    exports.ParameterStatusMessage = ParameterStatusMessage;
    var AuthenticationMD5Password = class {
      constructor(length, salt) {
        this.length = length;
        this.salt = salt;
        this.name = "authenticationMD5Password";
      }
    };
    exports.AuthenticationMD5Password = AuthenticationMD5Password;
    var BackendKeyDataMessage = class {
      constructor(length, processID, secretKey) {
        this.length = length;
        this.processID = processID;
        this.secretKey = secretKey;
        this.name = "backendKeyData";
      }
    };
    exports.BackendKeyDataMessage = BackendKeyDataMessage;
    var NotificationResponseMessage = class {
      constructor(length, processId, channel, payload) {
        this.length = length;
        this.processId = processId;
        this.channel = channel;
        this.payload = payload;
        this.name = "notification";
      }
    };
    exports.NotificationResponseMessage = NotificationResponseMessage;
    var ReadyForQueryMessage = class {
      constructor(length, status) {
        this.length = length;
        this.status = status;
        this.name = "readyForQuery";
      }
    };
    exports.ReadyForQueryMessage = ReadyForQueryMessage;
    var CommandCompleteMessage = class {
      constructor(length, text) {
        this.length = length;
        this.text = text;
        this.name = "commandComplete";
      }
    };
    exports.CommandCompleteMessage = CommandCompleteMessage;
    var DataRowMessage = class {
      constructor(length, fields) {
        this.length = length;
        this.fields = fields;
        this.name = "dataRow";
        this.fieldCount = fields.length;
      }
    };
    exports.DataRowMessage = DataRowMessage;
    var NoticeMessage = class {
      constructor(length, message) {
        this.length = length;
        this.message = message;
        this.name = "notice";
      }
    };
    exports.NoticeMessage = NoticeMessage;
  }
});

// node_modules/pg-protocol/dist/buffer-writer.js
var require_buffer_writer = __commonJS({
  "node_modules/pg-protocol/dist/buffer-writer.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.Writer = void 0;
    var Writer = class {
      constructor(size = 256) {
        this.size = size;
        this.offset = 5;
        this.headerPosition = 0;
        this.buffer = Buffer.allocUnsafe(size);
      }
      ensure(size) {
        const remaining = this.buffer.length - this.offset;
        if (remaining < size) {
          const oldBuffer = this.buffer;
          const newSize = oldBuffer.length + (oldBuffer.length >> 1) + size;
          this.buffer = Buffer.allocUnsafe(newSize);
          oldBuffer.copy(this.buffer);
        }
      }
      addInt32(num4) {
        this.ensure(4);
        this.buffer[this.offset++] = num4 >>> 24 & 255;
        this.buffer[this.offset++] = num4 >>> 16 & 255;
        this.buffer[this.offset++] = num4 >>> 8 & 255;
        this.buffer[this.offset++] = num4 >>> 0 & 255;
        return this;
      }
      addInt16(num4) {
        this.ensure(2);
        this.buffer[this.offset++] = num4 >>> 8 & 255;
        this.buffer[this.offset++] = num4 >>> 0 & 255;
        return this;
      }
      addCString(string) {
        if (!string) {
          this.ensure(1);
        } else {
          const len = Buffer.byteLength(string);
          this.ensure(len + 1);
          this.buffer.write(string, this.offset, "utf-8");
          this.offset += len;
        }
        this.buffer[this.offset++] = 0;
        return this;
      }
      addString(string = "") {
        const len = Buffer.byteLength(string);
        this.ensure(len);
        this.buffer.write(string, this.offset);
        this.offset += len;
        return this;
      }
      // Write an Int32 byte-length prefix immediately followed by the string's UTF-8
      // bytes. Postgres' Bind wire format prefixes every parameter with its length,
      // and doing it in one method computes Buffer.byteLength ONCE — the previous
      // `addInt32(Buffer.byteLength(s)).addString(s)` pairing scanned the string
      // three times (byteLength for the prefix, byteLength again inside addString,
      // then the encode), which is costly for large text parameters.
      addInt32PrefixedString(string) {
        const len = Buffer.byteLength(string);
        this.ensure(4 + len);
        const buffer = this.buffer;
        let offset = this.offset;
        buffer[offset++] = len >>> 24 & 255;
        buffer[offset++] = len >>> 16 & 255;
        buffer[offset++] = len >>> 8 & 255;
        buffer[offset++] = len >>> 0 & 255;
        buffer.write(string, offset, "utf-8");
        this.offset = offset + len;
        return this;
      }
      add(otherBuffer) {
        this.ensure(otherBuffer.length);
        otherBuffer.copy(this.buffer, this.offset);
        this.offset += otherBuffer.length;
        return this;
      }
      join(code) {
        if (code) {
          this.buffer[this.headerPosition] = code;
          const length = this.offset - (this.headerPosition + 1);
          this.buffer.writeInt32BE(length, this.headerPosition + 1);
        }
        return this.buffer.slice(code ? 0 : 5, this.offset);
      }
      flush(code) {
        const result = this.join(code);
        this.offset = 5;
        this.headerPosition = 0;
        this.buffer = Buffer.allocUnsafe(this.size);
        return result;
      }
      clear() {
        this.offset = 5;
        this.headerPosition = 0;
      }
    };
    exports.Writer = Writer;
  }
});

// node_modules/pg-protocol/dist/serializer.js
var require_serializer = __commonJS({
  "node_modules/pg-protocol/dist/serializer.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.serialize = void 0;
    var buffer_writer_1 = require_buffer_writer();
    var writer = new buffer_writer_1.Writer();
    var startup = (opts) => {
      writer.addInt16(3).addInt16(0);
      for (const key of Object.keys(opts)) {
        writer.addCString(key).addCString(opts[key]);
      }
      writer.addCString("client_encoding").addCString("UTF8");
      const bodyBuffer = writer.addCString("").flush();
      const length = bodyBuffer.length + 4;
      return new buffer_writer_1.Writer().addInt32(length).add(bodyBuffer).flush();
    };
    var requestSsl = () => {
      const response = Buffer.allocUnsafe(8);
      response.writeInt32BE(8, 0);
      response.writeInt32BE(80877103, 4);
      return response;
    };
    var password = (password2) => {
      return writer.addCString(password2).flush(
        112
        /* code.startup */
      );
    };
    var sendSASLInitialResponseMessage = function(mechanism, initialResponse) {
      writer.addCString(mechanism).addInt32PrefixedString(initialResponse);
      return writer.flush(
        112
        /* code.startup */
      );
    };
    var sendSCRAMClientFinalMessage = function(additionalData) {
      return writer.addString(additionalData).flush(
        112
        /* code.startup */
      );
    };
    var query = (text) => {
      return writer.addCString(text).flush(
        81
        /* code.query */
      );
    };
    var emptyArray = [];
    var parse = (query2) => {
      const name = query2.name || "";
      if (name.length > 63) {
        console.error("Warning! Postgres only supports 63 characters for query names.");
        console.error("You supplied %s (%s)", name, name.length);
        console.error("This can cause conflicts and silent errors executing queries");
      }
      const types2 = query2.types || emptyArray;
      const len = types2.length;
      const buffer = writer.addCString(name).addCString(query2.text).addInt16(len);
      for (let i = 0; i < len; i++) {
        buffer.addInt32(types2[i]);
      }
      return writer.flush(
        80
        /* code.parse */
      );
    };
    var paramWriter = new buffer_writer_1.Writer();
    var writeValues = function(values, valueMapper) {
      for (let i = 0; i < values.length; i++) {
        const mappedVal = valueMapper ? valueMapper(values[i], i) : values[i];
        if (mappedVal == null) {
          writer.addInt16(
            0
            /* ParamType.STRING */
          );
          paramWriter.addInt32(-1);
        } else if (mappedVal instanceof Buffer) {
          writer.addInt16(
            1
            /* ParamType.BINARY */
          );
          paramWriter.addInt32(mappedVal.length);
          paramWriter.add(mappedVal);
        } else {
          writer.addInt16(
            0
            /* ParamType.STRING */
          );
          paramWriter.addInt32PrefixedString(mappedVal);
        }
      }
    };
    var bind = (config = {}) => {
      const portal = config.portal || "";
      const statement = config.statement || "";
      const binary = config.binary || false;
      const values = config.values || emptyArray;
      const len = values.length;
      writer.addCString(portal).addCString(statement);
      writer.addInt16(len);
      try {
        writeValues(values, config.valueMapper);
      } catch (err) {
        writer.clear();
        paramWriter.clear();
        throw err;
      }
      writer.addInt16(len);
      writer.add(paramWriter.flush());
      writer.addInt16(1);
      writer.addInt16(
        binary ? 1 : 0
        /* ParamType.STRING */
      );
      return writer.flush(
        66
        /* code.bind */
      );
    };
    var emptyExecute = Buffer.from([69, 0, 0, 0, 9, 0, 0, 0, 0, 0]);
    var execute = (config) => {
      if (!config || !config.portal && !config.rows) {
        return emptyExecute;
      }
      const portal = config.portal || "";
      const rows = config.rows || 0;
      const portalLength = Buffer.byteLength(portal);
      const len = 4 + portalLength + 1 + 4;
      const buff = Buffer.allocUnsafe(1 + len);
      buff[0] = 69;
      buff.writeInt32BE(len, 1);
      buff.write(portal, 5, "utf-8");
      buff[portalLength + 5] = 0;
      buff.writeUInt32BE(rows, buff.length - 4);
      return buff;
    };
    var cancel = (processID, secretKey) => {
      const buffer = Buffer.allocUnsafe(16);
      buffer.writeInt32BE(16, 0);
      buffer.writeInt16BE(1234, 4);
      buffer.writeInt16BE(5678, 6);
      buffer.writeInt32BE(processID, 8);
      buffer.writeInt32BE(secretKey, 12);
      return buffer;
    };
    var cstringMessage = (code, string) => {
      const stringLen = Buffer.byteLength(string);
      const len = 4 + stringLen + 1;
      const buffer = Buffer.allocUnsafe(1 + len);
      buffer[0] = code;
      buffer.writeInt32BE(len, 1);
      buffer.write(string, 5, "utf-8");
      buffer[len] = 0;
      return buffer;
    };
    var emptyDescribePortal = writer.addCString("P").flush(
      68
      /* code.describe */
    );
    var emptyDescribeStatement = writer.addCString("S").flush(
      68
      /* code.describe */
    );
    var describe = (msg) => {
      return msg.name ? cstringMessage(68, `${msg.type}${msg.name || ""}`) : msg.type === "P" ? emptyDescribePortal : emptyDescribeStatement;
    };
    var close = (msg) => {
      const text = `${msg.type}${msg.name || ""}`;
      return cstringMessage(67, text);
    };
    var copyData = (chunk) => {
      return writer.add(chunk).flush(
        100
        /* code.copyFromChunk */
      );
    };
    var copyFail = (message) => {
      return cstringMessage(102, message);
    };
    var codeOnlyBuffer = (code) => Buffer.from([code, 0, 0, 0, 4]);
    var flushBuffer = codeOnlyBuffer(
      72
      /* code.flush */
    );
    var syncBuffer = codeOnlyBuffer(
      83
      /* code.sync */
    );
    var endBuffer = codeOnlyBuffer(
      88
      /* code.end */
    );
    var copyDoneBuffer = codeOnlyBuffer(
      99
      /* code.copyDone */
    );
    var serialize = {
      startup,
      password,
      requestSsl,
      sendSASLInitialResponseMessage,
      sendSCRAMClientFinalMessage,
      query,
      parse,
      bind,
      execute,
      describe,
      close,
      flush: () => flushBuffer,
      sync: () => syncBuffer,
      end: () => endBuffer,
      copyData,
      copyDone: () => copyDoneBuffer,
      copyFail,
      cancel
    };
    exports.serialize = serialize;
  }
});

// node_modules/pg-protocol/dist/buffer-reader.js
var require_buffer_reader = __commonJS({
  "node_modules/pg-protocol/dist/buffer-reader.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.BufferReader = void 0;
    var BufferReader = class {
      constructor(offset = 0) {
        this.offset = offset;
        this.buffer = Buffer.allocUnsafe(0);
        this.encoding = "utf-8";
      }
      setBuffer(offset, buffer) {
        this.offset = offset;
        this.buffer = buffer;
      }
      int16() {
        const result = this.buffer.readInt16BE(this.offset);
        this.offset += 2;
        return result;
      }
      byte() {
        const result = this.buffer[this.offset];
        this.offset++;
        return result;
      }
      int32() {
        const result = this.buffer.readInt32BE(this.offset);
        this.offset += 4;
        return result;
      }
      uint32() {
        const result = this.buffer.readUInt32BE(this.offset);
        this.offset += 4;
        return result;
      }
      string(length) {
        const result = this.buffer.toString(this.encoding, this.offset, this.offset + length);
        this.offset += length;
        return result;
      }
      cstring() {
        const start = this.offset;
        let end = start;
        while (this.buffer[end++]) {
        }
        this.offset = end;
        return this.buffer.toString(this.encoding, start, end - 1);
      }
      bytes(length) {
        const result = this.buffer.slice(this.offset, this.offset + length);
        this.offset += length;
        return result;
      }
    };
    exports.BufferReader = BufferReader;
  }
});

// node_modules/pg-protocol/dist/parser.js
var require_parser = __commonJS({
  "node_modules/pg-protocol/dist/parser.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.Parser = void 0;
    var messages_1 = require_messages();
    var buffer_reader_1 = require_buffer_reader();
    var CODE_LENGTH = 1;
    var LEN_LENGTH = 4;
    var HEADER_LENGTH = CODE_LENGTH + LEN_LENGTH;
    var LATEINIT_LENGTH = -1;
    var emptyBuffer = Buffer.allocUnsafe(0);
    var Parser = class {
      constructor(opts) {
        this.buffer = emptyBuffer;
        this.bufferLength = 0;
        this.bufferOffset = 0;
        this.reader = new buffer_reader_1.BufferReader();
        if ((opts === null || opts === void 0 ? void 0 : opts.mode) === "binary") {
          throw new Error("Binary mode not supported yet");
        }
        this.mode = (opts === null || opts === void 0 ? void 0 : opts.mode) || "text";
      }
      parse(buffer, callback) {
        this.mergeBuffer(buffer);
        const bufferFullLength = this.bufferOffset + this.bufferLength;
        let offset = this.bufferOffset;
        while (offset + HEADER_LENGTH <= bufferFullLength) {
          const code = this.buffer[offset];
          const length = this.buffer.readUInt32BE(offset + CODE_LENGTH);
          const fullMessageLength = CODE_LENGTH + length;
          if (fullMessageLength + offset <= bufferFullLength) {
            const message = this.handlePacket(offset + HEADER_LENGTH, code, length, this.buffer);
            callback(message);
            offset += fullMessageLength;
          } else {
            break;
          }
        }
        if (offset === bufferFullLength) {
          this.buffer = emptyBuffer;
          this.bufferLength = 0;
          this.bufferOffset = 0;
        } else {
          this.bufferLength = bufferFullLength - offset;
          this.bufferOffset = offset;
        }
      }
      mergeBuffer(buffer) {
        if (this.bufferLength > 0) {
          const newLength = this.bufferLength + buffer.byteLength;
          const newFullLength = newLength + this.bufferOffset;
          if (newFullLength > this.buffer.byteLength) {
            let newBuffer;
            if (newLength <= this.buffer.byteLength && this.bufferOffset >= this.bufferLength) {
              newBuffer = this.buffer;
            } else {
              let newBufferLength = this.buffer.byteLength * 2;
              while (newLength >= newBufferLength) {
                newBufferLength *= 2;
              }
              newBuffer = Buffer.allocUnsafe(newBufferLength);
            }
            this.buffer.copy(newBuffer, 0, this.bufferOffset, this.bufferOffset + this.bufferLength);
            this.buffer = newBuffer;
            this.bufferOffset = 0;
          }
          buffer.copy(this.buffer, this.bufferOffset + this.bufferLength);
          this.bufferLength = newLength;
        } else {
          this.buffer = buffer;
          this.bufferOffset = 0;
          this.bufferLength = buffer.byteLength;
        }
      }
      handlePacket(offset, code, length, bytes) {
        const { reader } = this;
        reader.setBuffer(offset, bytes);
        let message;
        switch (code) {
          case 50:
            message = messages_1.bindComplete;
            break;
          case 49:
            message = messages_1.parseComplete;
            break;
          case 51:
            message = messages_1.closeComplete;
            break;
          case 110:
            message = messages_1.noData;
            break;
          case 115:
            message = messages_1.portalSuspended;
            break;
          case 99:
            message = messages_1.copyDone;
            break;
          case 87:
            message = messages_1.replicationStart;
            break;
          case 73:
            message = messages_1.emptyQuery;
            break;
          case 68:
            message = parseDataRowMessage(reader);
            break;
          case 67:
            message = parseCommandCompleteMessage(reader);
            break;
          case 90:
            message = parseReadyForQueryMessage(reader);
            break;
          case 65:
            message = parseNotificationMessage(reader);
            break;
          case 82:
            message = parseAuthenticationResponse(reader, length);
            break;
          case 83:
            message = parseParameterStatusMessage(reader);
            break;
          case 75:
            message = parseBackendKeyData(reader);
            break;
          case 69:
            message = parseErrorMessage(reader, "error");
            break;
          case 78:
            message = parseErrorMessage(reader, "notice");
            break;
          case 84:
            message = parseRowDescriptionMessage(reader);
            break;
          case 116:
            message = parseParameterDescriptionMessage(reader);
            break;
          case 71:
            message = parseCopyInMessage(reader);
            break;
          case 72:
            message = parseCopyOutMessage(reader);
            break;
          case 100:
            message = parseCopyData(reader, length);
            break;
          default:
            return new messages_1.DatabaseError("received invalid response: " + code.toString(16), length, "error");
        }
        reader.setBuffer(0, emptyBuffer);
        message.length = length;
        return message;
      }
    };
    exports.Parser = Parser;
    var parseReadyForQueryMessage = (reader) => {
      const status = reader.string(1);
      return new messages_1.ReadyForQueryMessage(LATEINIT_LENGTH, status);
    };
    var parseCommandCompleteMessage = (reader) => {
      const text = reader.cstring();
      return new messages_1.CommandCompleteMessage(LATEINIT_LENGTH, text);
    };
    var parseCopyData = (reader, length) => {
      const chunk = reader.bytes(length - 4);
      return new messages_1.CopyDataMessage(LATEINIT_LENGTH, chunk);
    };
    var parseCopyInMessage = (reader) => parseCopyMessage(reader, "copyInResponse");
    var parseCopyOutMessage = (reader) => parseCopyMessage(reader, "copyOutResponse");
    var parseCopyMessage = (reader, messageName) => {
      const isBinary = reader.byte() !== 0;
      const columnCount = reader.int16();
      const message = new messages_1.CopyResponse(LATEINIT_LENGTH, messageName, isBinary, columnCount);
      for (let i = 0; i < columnCount; i++) {
        message.columnTypes[i] = reader.int16();
      }
      return message;
    };
    var parseNotificationMessage = (reader) => {
      const processId = reader.int32();
      const channel = reader.cstring();
      const payload = reader.cstring();
      return new messages_1.NotificationResponseMessage(LATEINIT_LENGTH, processId, channel, payload);
    };
    var parseRowDescriptionMessage = (reader) => {
      const fieldCount = reader.int16();
      const message = new messages_1.RowDescriptionMessage(LATEINIT_LENGTH, fieldCount);
      for (let i = 0; i < fieldCount; i++) {
        message.fields[i] = parseField(reader);
      }
      return message;
    };
    var parseField = (reader) => {
      const name = reader.cstring();
      const tableID = reader.uint32();
      const columnID = reader.int16();
      const dataTypeID = reader.uint32();
      const dataTypeSize = reader.int16();
      const dataTypeModifier = reader.int32();
      const mode = reader.int16() === 0 ? "text" : "binary";
      return new messages_1.Field(name, tableID, columnID, dataTypeID, dataTypeSize, dataTypeModifier, mode);
    };
    var parseParameterDescriptionMessage = (reader) => {
      const parameterCount = reader.int16();
      const message = new messages_1.ParameterDescriptionMessage(LATEINIT_LENGTH, parameterCount);
      for (let i = 0; i < parameterCount; i++) {
        message.dataTypeIDs[i] = reader.uint32();
      }
      return message;
    };
    var parseDataRowMessage = (reader) => {
      const fieldCount = reader.int16();
      const fields = new Array(fieldCount);
      for (let i = 0; i < fieldCount; i++) {
        const len = reader.int32();
        fields[i] = len === -1 ? null : reader.string(len);
      }
      return new messages_1.DataRowMessage(LATEINIT_LENGTH, fields);
    };
    var parseParameterStatusMessage = (reader) => {
      const name = reader.cstring();
      const value = reader.cstring();
      return new messages_1.ParameterStatusMessage(LATEINIT_LENGTH, name, value);
    };
    var parseBackendKeyData = (reader) => {
      const processID = reader.int32();
      const secretKey = reader.int32();
      return new messages_1.BackendKeyDataMessage(LATEINIT_LENGTH, processID, secretKey);
    };
    var parseAuthenticationResponse = (reader, length) => {
      const code = reader.int32();
      const message = {
        name: "authenticationOk",
        length
      };
      switch (code) {
        case 0:
          break;
        case 3:
          if (message.length === 8) {
            message.name = "authenticationCleartextPassword";
          }
          break;
        case 5:
          if (message.length === 12) {
            message.name = "authenticationMD5Password";
            const salt = reader.bytes(4);
            return new messages_1.AuthenticationMD5Password(LATEINIT_LENGTH, salt);
          }
          break;
        case 10:
          {
            message.name = "authenticationSASL";
            message.mechanisms = [];
            let mechanism;
            do {
              mechanism = reader.cstring();
              if (mechanism) {
                message.mechanisms.push(mechanism);
              }
            } while (mechanism);
          }
          break;
        case 11:
          message.name = "authenticationSASLContinue";
          message.data = reader.string(length - 8);
          break;
        case 12:
          message.name = "authenticationSASLFinal";
          message.data = reader.string(length - 8);
          break;
        default:
          throw new Error("Unknown authenticationOk message type " + code);
      }
      return message;
    };
    var parseErrorMessage = (reader, name) => {
      const fields = {};
      let fieldType = reader.string(1);
      while (fieldType !== "\0") {
        fields[fieldType] = reader.cstring();
        fieldType = reader.string(1);
      }
      const messageValue = fields.M;
      const message = name === "notice" ? new messages_1.NoticeMessage(LATEINIT_LENGTH, messageValue) : new messages_1.DatabaseError(messageValue, LATEINIT_LENGTH, name);
      message.severity = fields.S;
      message.code = fields.C;
      message.detail = fields.D;
      message.hint = fields.H;
      message.position = fields.P;
      message.internalPosition = fields.p;
      message.internalQuery = fields.q;
      message.where = fields.W;
      message.schema = fields.s;
      message.table = fields.t;
      message.column = fields.c;
      message.dataType = fields.d;
      message.constraint = fields.n;
      message.file = fields.F;
      message.line = fields.L;
      message.routine = fields.R;
      return message;
    };
  }
});

// node_modules/pg-protocol/dist/index.js
var require_dist = __commonJS({
  "node_modules/pg-protocol/dist/index.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.DatabaseError = exports.serialize = void 0;
    exports.parse = parse;
    var messages_1 = require_messages();
    Object.defineProperty(exports, "DatabaseError", { enumerable: true, get: function() {
      return messages_1.DatabaseError;
    } });
    var serializer_1 = require_serializer();
    Object.defineProperty(exports, "serialize", { enumerable: true, get: function() {
      return serializer_1.serialize;
    } });
    var parser_1 = require_parser();
    function parse(stream, callback) {
      const parser = new parser_1.Parser();
      stream.on("data", (buffer) => parser.parse(buffer, callback));
      return new Promise((resolve2) => stream.on("end", () => resolve2()));
    }
  }
});

// node_modules/pg-cloudflare/dist/empty.js
var require_empty = __commonJS({
  "node_modules/pg-cloudflare/dist/empty.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.default = {};
  }
});

// node_modules/pg/lib/stream.js
var require_stream2 = __commonJS({
  "node_modules/pg/lib/stream.js"(exports, module) {
    var { getStream, getSecureStream } = getStreamFuncs();
    module.exports = {
      /**
       * Get a socket stream compatible with the current runtime environment.
       * @returns {Duplex}
       */
      getStream,
      /**
       * Get a TLS secured socket, compatible with the current environment,
       * using the socket and other settings given in `options`.
       * @returns {Duplex}
       */
      getSecureStream
    };
    function getNodejsStreamFuncs() {
      function getStream2(ssl) {
        const net = __require("net");
        return new net.Socket();
      }
      function getSecureStream2(options) {
        const tls = __require("tls");
        return tls.connect(options);
      }
      return {
        getStream: getStream2,
        getSecureStream: getSecureStream2
      };
    }
    function getCloudflareStreamFuncs() {
      function getStream2(ssl) {
        const { CloudflareSocket } = require_empty();
        return new CloudflareSocket(ssl);
      }
      function getSecureStream2(options) {
        options.socket.startTls(options);
        return options.socket;
      }
      return {
        getStream: getStream2,
        getSecureStream: getSecureStream2
      };
    }
    function isCloudflareRuntime() {
      if (typeof navigator === "object" && navigator !== null && typeof navigator.userAgent === "string") {
        return navigator.userAgent === "Cloudflare-Workers";
      }
      if (typeof Response === "function") {
        const resp = new Response(null, { cf: { thing: true } });
        if (typeof resp.cf === "object" && resp.cf !== null && resp.cf.thing) {
          return true;
        }
      }
      return false;
    }
    function getStreamFuncs() {
      if (isCloudflareRuntime()) {
        return getCloudflareStreamFuncs();
      }
      return getNodejsStreamFuncs();
    }
  }
});

// node_modules/pg/lib/connection.js
var require_connection = __commonJS({
  "node_modules/pg/lib/connection.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events").EventEmitter;
    var { parse, serialize } = require_dist();
    var stream = require_stream2();
    var { getStream } = stream;
    var flushBuffer = serialize.flush();
    var syncBuffer = serialize.sync();
    var endBuffer = serialize.end();
    var Connection2 = class extends EventEmitter {
      constructor(config) {
        super();
        config = config || {};
        this.stream = config.stream || getStream(config.ssl);
        if (typeof this.stream === "function") {
          this.stream = this.stream(config);
        }
        this._keepAlive = config.keepAlive;
        this._keepAliveInitialDelayMillis = config.keepAliveInitialDelayMillis;
        this.parsedStatements = {};
        this.submittedNamedStatements = {};
        this.ssl = config.ssl || false;
        this.sslNegotiation = config.sslNegotiation || "postgres";
        this._ending = false;
        this._emitMessage = false;
        const self = this;
        this.on("newListener", function(eventName) {
          if (eventName === "message") {
            self._emitMessage = true;
          }
        });
      }
      connect(port, host) {
        const self = this;
        this._connecting = true;
        this.stream.setNoDelay(true);
        this.stream.connect(port, host);
        this.stream.once("connect", function() {
          if (self._keepAlive) {
            self.stream.setKeepAlive(true, self._keepAliveInitialDelayMillis);
          }
          self.emit("connect");
        });
        const reportStreamError = function(error) {
          if (self._ending && (error.code === "ECONNRESET" || error.code === "EPIPE")) {
            return;
          }
          self.emit("error", error);
        };
        this.stream.on("error", reportStreamError);
        this.stream.on("close", function() {
          self.emit("end");
        });
        if (!this.ssl) {
          return this.attachListeners(this.stream);
        }
        if (this.sslNegotiation === "direct") {
          return this.stream.once("connect", function() {
            self.upgradeToSSL(host, reportStreamError);
          });
        }
        this.stream.once("data", function(buffer) {
          const responseCode = buffer.toString("utf8");
          switch (responseCode) {
            case "S":
              break;
            case "N":
              self.stream.end();
              return self.emit("error", new Error("The server does not support SSL connections"));
            default:
              self.stream.end();
              return self.emit("error", new Error("There was an error establishing an SSL connection"));
          }
          self.upgradeToSSL(host, reportStreamError);
        });
      }
      upgradeToSSL(host, reportStreamError) {
        const self = this;
        const options = {
          socket: self.stream
        };
        if (self.ssl !== true) {
          Object.assign(options, self.ssl);
          if ("key" in self.ssl) {
            options.key = self.ssl.key;
          }
        }
        if (self.sslNegotiation === "direct") {
          options.ALPNProtocols = ["postgresql"];
        }
        const net = __require("net");
        if (net.isIP && net.isIP(host) === 0) {
          options.servername = host;
        }
        try {
          self.stream = stream.getSecureStream(options);
        } catch (err) {
          return self.emit("error", err);
        }
        self.attachListeners(self.stream);
        self.stream.on("error", reportStreamError);
        self.emit("sslconnect");
      }
      attachListeners(stream2) {
        parse(stream2, (msg) => {
          const eventName = msg.name === "error" ? "errorMessage" : msg.name;
          if (this._emitMessage) {
            this.emit("message", msg);
          }
          this.emit(eventName, msg);
        });
      }
      requestSsl() {
        this.stream.write(serialize.requestSsl());
      }
      startup(config) {
        this.stream.write(serialize.startup(config));
      }
      cancel(processID, secretKey) {
        this._send(serialize.cancel(processID, secretKey));
      }
      password(password) {
        this._send(serialize.password(password));
      }
      sendSASLInitialResponseMessage(mechanism, initialResponse) {
        this._send(serialize.sendSASLInitialResponseMessage(mechanism, initialResponse));
      }
      sendSCRAMClientFinalMessage(additionalData) {
        this._send(serialize.sendSCRAMClientFinalMessage(additionalData));
      }
      _send(buffer) {
        if (!this.stream.writable) {
          return false;
        }
        return this.stream.write(buffer);
      }
      query(text) {
        this._send(serialize.query(text));
      }
      // send parse message
      parse(query) {
        this._send(serialize.parse(query));
      }
      // send bind message
      bind(config) {
        this._send(serialize.bind(config));
      }
      // send execute message
      execute(config) {
        this._send(serialize.execute(config));
      }
      flush() {
        if (this.stream.writable) {
          this.stream.write(flushBuffer);
        }
      }
      sync() {
        this._ending = true;
        this._send(syncBuffer);
      }
      ref() {
        this.stream.ref();
      }
      unref() {
        this.stream.unref();
      }
      end() {
        this._ending = true;
        if (!this._connecting || !this.stream.writable) {
          this.stream.end();
          return;
        }
        return this.stream.write(endBuffer, () => {
          this.stream.end();
        });
      }
      close(msg) {
        this._send(serialize.close(msg));
      }
      describe(msg) {
        this._send(serialize.describe(msg));
      }
      sendCopyFromChunk(chunk) {
        this._send(serialize.copyData(chunk));
      }
      endCopyFrom() {
        this._send(serialize.copyDone());
      }
      sendCopyFail(msg) {
        this._send(serialize.copyFail(msg));
      }
    };
    module.exports = Connection2;
  }
});

// node_modules/split2/index.js
var require_split2 = __commonJS({
  "node_modules/split2/index.js"(exports, module) {
    "use strict";
    var { Transform } = __require("stream");
    var { StringDecoder } = __require("string_decoder");
    var kLast = Symbol("last");
    var kDecoder = Symbol("decoder");
    function transform(chunk, enc, cb) {
      let list;
      if (this.overflow) {
        const buf = this[kDecoder].write(chunk);
        list = buf.split(this.matcher);
        if (list.length === 1) return cb();
        list.shift();
        this.overflow = false;
      } else {
        this[kLast] += this[kDecoder].write(chunk);
        list = this[kLast].split(this.matcher);
      }
      this[kLast] = list.pop();
      for (let i = 0; i < list.length; i++) {
        try {
          push(this, this.mapper(list[i]));
        } catch (error) {
          return cb(error);
        }
      }
      this.overflow = this[kLast].length > this.maxLength;
      if (this.overflow && !this.skipOverflow) {
        cb(new Error("maximum buffer reached"));
        return;
      }
      cb();
    }
    function flush2(cb) {
      this[kLast] += this[kDecoder].end();
      if (this[kLast]) {
        try {
          push(this, this.mapper(this[kLast]));
        } catch (error) {
          return cb(error);
        }
      }
      cb();
    }
    function push(self, val) {
      if (val !== void 0) {
        self.push(val);
      }
    }
    function noop(incoming) {
      return incoming;
    }
    function split(matcher, mapper, options) {
      matcher = matcher || /\r?\n/;
      mapper = mapper || noop;
      options = options || {};
      switch (arguments.length) {
        case 1:
          if (typeof matcher === "function") {
            mapper = matcher;
            matcher = /\r?\n/;
          } else if (typeof matcher === "object" && !(matcher instanceof RegExp) && !matcher[Symbol.split]) {
            options = matcher;
            matcher = /\r?\n/;
          }
          break;
        case 2:
          if (typeof matcher === "function") {
            options = mapper;
            mapper = matcher;
            matcher = /\r?\n/;
          } else if (typeof mapper === "object") {
            options = mapper;
            mapper = noop;
          }
      }
      options = Object.assign({}, options);
      options.autoDestroy = true;
      options.transform = transform;
      options.flush = flush2;
      options.readableObjectMode = true;
      const stream = new Transform(options);
      stream[kLast] = "";
      stream[kDecoder] = new StringDecoder("utf8");
      stream.matcher = matcher;
      stream.mapper = mapper;
      stream.maxLength = options.maxLength;
      stream.skipOverflow = options.skipOverflow || false;
      stream.overflow = false;
      stream._destroy = function(err, cb) {
        this._writableState.errorEmitted = false;
        cb(err);
      };
      return stream;
    }
    module.exports = split;
  }
});

// node_modules/pgpass/lib/helper.js
var require_helper = __commonJS({
  "node_modules/pgpass/lib/helper.js"(exports, module) {
    "use strict";
    var path = __require("path");
    var Stream = __require("stream").Stream;
    var split = require_split2();
    var util = __require("util");
    var defaultPort = 5432;
    var isWin = process.platform === "win32";
    var warnStream = process.stderr;
    var S_IRWXG = 56;
    var S_IRWXO = 7;
    var S_IFMT = 61440;
    var S_IFREG = 32768;
    function isRegFile(mode) {
      return (mode & S_IFMT) == S_IFREG;
    }
    var fieldNames = ["host", "port", "database", "user", "password"];
    var nrOfFields = fieldNames.length;
    var passKey = fieldNames[nrOfFields - 1];
    function warn() {
      var isWritable = warnStream instanceof Stream && true === warnStream.writable;
      if (isWritable) {
        var args = Array.prototype.slice.call(arguments).concat("\n");
        warnStream.write(util.format.apply(util, args));
      }
    }
    Object.defineProperty(module.exports, "isWin", {
      get: function() {
        return isWin;
      },
      set: function(val) {
        isWin = val;
      }
    });
    module.exports.warnTo = function(stream) {
      var old = warnStream;
      warnStream = stream;
      return old;
    };
    module.exports.getFileName = function(rawEnv) {
      var env = rawEnv || process.env;
      var file = env.PGPASSFILE || (isWin ? path.join(env.APPDATA || "./", "postgresql", "pgpass.conf") : path.join(env.HOME || "./", ".pgpass"));
      return file;
    };
    module.exports.usePgPass = function(stats, fname) {
      if (Object.prototype.hasOwnProperty.call(process.env, "PGPASSWORD")) {
        return false;
      }
      if (isWin) {
        return true;
      }
      fname = fname || "<unkn>";
      if (!isRegFile(stats.mode)) {
        warn('WARNING: password file "%s" is not a plain file', fname);
        return false;
      }
      if (stats.mode & (S_IRWXG | S_IRWXO)) {
        warn('WARNING: password file "%s" has group or world access; permissions should be u=rw (0600) or less', fname);
        return false;
      }
      return true;
    };
    var matcher = module.exports.match = function(connInfo, entry) {
      return fieldNames.slice(0, -1).reduce(function(prev, field, idx2) {
        if (idx2 == 1) {
          if (Number(connInfo[field] || defaultPort) === Number(entry[field])) {
            return prev && true;
          }
        }
        return prev && (entry[field] === "*" || entry[field] === connInfo[field]);
      }, true);
    };
    module.exports.getPassword = function(connInfo, stream, cb) {
      var pass;
      var lineStream = stream.pipe(split());
      function onLine(line) {
        var entry = parseLine(line);
        if (entry && isValidEntry(entry) && matcher(connInfo, entry)) {
          pass = entry[passKey];
          lineStream.end();
        }
      }
      var onEnd = function() {
        stream.destroy();
        cb(pass);
      };
      var onErr = function(err) {
        stream.destroy();
        warn("WARNING: error on reading file: %s", err);
        cb(void 0);
      };
      stream.on("error", onErr);
      lineStream.on("data", onLine).on("end", onEnd).on("error", onErr);
    };
    var parseLine = module.exports.parseLine = function(line) {
      if (line.length < 11 || line.match(/^\s+#/)) {
        return null;
      }
      var curChar = "";
      var prevChar = "";
      var fieldIdx = 0;
      var startIdx = 0;
      var endIdx = 0;
      var obj = {};
      var isLastField = false;
      var addToObj = function(idx2, i0, i1) {
        var field = line.substring(i0, i1);
        if (!Object.hasOwnProperty.call(process.env, "PGPASS_NO_DEESCAPE")) {
          field = field.replace(/\\([:\\])/g, "$1");
        }
        obj[fieldNames[idx2]] = field;
      };
      for (var i = 0; i < line.length - 1; i += 1) {
        curChar = line.charAt(i + 1);
        prevChar = line.charAt(i);
        isLastField = fieldIdx == nrOfFields - 1;
        if (isLastField) {
          addToObj(fieldIdx, startIdx);
          break;
        }
        if (i >= 0 && curChar == ":" && prevChar !== "\\") {
          addToObj(fieldIdx, startIdx, i + 1);
          startIdx = i + 2;
          fieldIdx += 1;
        }
      }
      obj = Object.keys(obj).length === nrOfFields ? obj : null;
      return obj;
    };
    var isValidEntry = module.exports.isValidEntry = function(entry) {
      var rules = {
        // host
        0: function(x) {
          return x.length > 0;
        },
        // port
        1: function(x) {
          if (x === "*") {
            return true;
          }
          x = Number(x);
          return isFinite(x) && x > 0 && x < 9007199254740992 && Math.floor(x) === x;
        },
        // database
        2: function(x) {
          return x.length > 0;
        },
        // username
        3: function(x) {
          return x.length > 0;
        },
        // password
        4: function(x) {
          return x.length > 0;
        }
      };
      for (var idx2 = 0; idx2 < fieldNames.length; idx2 += 1) {
        var rule = rules[idx2];
        var value = entry[fieldNames[idx2]] || "";
        var res = rule(value);
        if (!res) {
          return false;
        }
      }
      return true;
    };
  }
});

// node_modules/pgpass/lib/index.js
var require_lib = __commonJS({
  "node_modules/pgpass/lib/index.js"(exports, module) {
    "use strict";
    var path = __require("path");
    var fs = __require("fs");
    var helper = require_helper();
    module.exports = function(connInfo, cb) {
      var file = helper.getFileName();
      fs.stat(file, function(err, stat) {
        if (err || !helper.usePgPass(stat, file)) {
          return cb(void 0);
        }
        var st = fs.createReadStream(file);
        helper.getPassword(connInfo, st, cb);
      });
    };
    module.exports.warnTo = helper.warnTo;
  }
});

// node_modules/pg/lib/client.js
var require_client = __commonJS({
  "node_modules/pg/lib/client.js"(exports, module) {
    var EventEmitter = __require("events").EventEmitter;
    var utils = require_utils();
    var nodeUtils = __require("util");
    var sasl = require_sasl();
    var TypeOverrides2 = require_type_overrides();
    var ConnectionParameters = require_connection_parameters();
    var Query2 = require_query();
    var defaults2 = require_defaults();
    var Connection2 = require_connection();
    var crypto = require_utils2();
    var activeQueryDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Client.activeQuery is deprecated and will be removed in pg@9.0"
    );
    var queryQueueDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Client.queryQueue is deprecated and will be removed in pg@9.0."
    );
    var pgPassDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "pgpass support is deprecated and will be removed in pg@9.0. You can provide an async function as the password property to the Client/Pool constructor that returns a password instead. Within this function you can call the pgpass module in your own code."
    );
    var byoPromiseDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Passing a custom Promise implementation to the Client/Pool constructor is deprecated and will be removed in pg@9.0."
    );
    var queryQueueLengthDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Calling client.query() when the client is already executing a query is deprecated and will be removed in pg@9.0. Use async/await or an external async flow control mechanism instead."
    );
    function coerceNumberOrDefault(value, defaultValue) {
      if (typeof value === "number") {
        return Number.isFinite(value) ? value : defaultValue;
      }
      if (typeof value === "string" && value.trim() !== "") {
        const n = Number(value);
        return Number.isFinite(n) ? n : defaultValue;
      }
      return defaultValue;
    }
    var Client2 = class extends EventEmitter {
      constructor(config) {
        super();
        this.connectionParameters = new ConnectionParameters(config);
        this.user = this.connectionParameters.user;
        this.database = this.connectionParameters.database;
        this.port = this.connectionParameters.port;
        this.host = this.connectionParameters.host;
        Object.defineProperty(this, "password", {
          configurable: true,
          enumerable: false,
          writable: true,
          value: this.connectionParameters.password
        });
        this.replication = this.connectionParameters.replication;
        const c = config || {};
        if (c.Promise) {
          byoPromiseDeprecationNotice();
        }
        this._Promise = c.Promise || global.Promise;
        this._types = new TypeOverrides2(c.types);
        this._ending = false;
        this._ended = false;
        this._connecting = false;
        this._connected = false;
        this._connectionError = false;
        this._queryable = true;
        this._activeQuery = null;
        this._txStatus = null;
        this.enableChannelBinding = Boolean(c.enableChannelBinding);
        this.scramMaxIterations = coerceNumberOrDefault(c.scramMaxIterations, sasl.DEFAULT_MAX_SCRAM_ITERATIONS);
        this.connection = c.connection || new Connection2({
          stream: c.stream,
          ssl: this.connectionParameters.ssl,
          sslNegotiation: this.connectionParameters.sslnegotiation,
          keepAlive: c.keepAlive || false,
          keepAliveInitialDelayMillis: c.keepAliveInitialDelayMillis || 0,
          encoding: this.connectionParameters.client_encoding || "utf8"
        });
        this._queryQueue = [];
        this._sentQueryQueue = [];
        this.pipeline = Boolean(c.pipeline);
        this.binary = c.binary || defaults2.binary;
        this.processID = null;
        this.secretKey = null;
        this.ssl = this.connectionParameters.ssl || false;
        this.sslNegotiation = this.connectionParameters.sslnegotiation || "postgres";
        if (this.ssl && this.ssl.key) {
          Object.defineProperty(this.ssl, "key", {
            enumerable: false
          });
        }
        this._connectionTimeoutMillis = c.connectionTimeoutMillis || 0;
      }
      get activeQuery() {
        activeQueryDeprecationNotice();
        return this._activeQuery;
      }
      set activeQuery(val) {
        activeQueryDeprecationNotice();
        this._activeQuery = val;
      }
      _getActiveQuery() {
        return this._activeQuery;
      }
      _errorAllQueries(err) {
        const enqueueError = (query) => {
          process.nextTick(() => {
            query.handleError(err, this.connection);
          });
        };
        const activeQuery = this._getActiveQuery();
        if (activeQuery) {
          enqueueError(activeQuery);
          this._activeQuery = null;
        }
        this._sentQueryQueue.forEach(enqueueError);
        this._sentQueryQueue.length = 0;
        this._queryQueue.forEach(enqueueError);
        this._queryQueue.length = 0;
      }
      _connect(callback) {
        const self = this;
        const con = this.connection;
        this._connectionCallback = callback;
        if (this._connecting || this._connected) {
          const err = new Error("Client has already been connected. You cannot reuse a client.");
          process.nextTick(() => {
            callback(err);
          });
          return;
        }
        this._connecting = true;
        if (this._connectionTimeoutMillis > 0) {
          this.connectionTimeoutHandle = setTimeout(() => {
            con._ending = true;
            con.stream.destroy(new Error("timeout expired"));
          }, this._connectionTimeoutMillis);
          if (this.connectionTimeoutHandle.unref) {
            this.connectionTimeoutHandle.unref();
          }
        }
        if (this.host && this.host.indexOf("/") === 0) {
          con.connect(this.host + "/.s.PGSQL." + this.port);
        } else {
          con.connect(this.port, this.host);
        }
        con.on("connect", function() {
          if (self.ssl) {
            if (self.sslNegotiation !== "direct") {
              con.requestSsl();
            }
          } else {
            con.startup(self.getStartupConf());
          }
        });
        con.on("sslconnect", function() {
          con.startup(self.getStartupConf());
        });
        this._attachListeners(con);
        con.once("end", () => {
          const error = this._ending ? new Error("Connection terminated") : new Error("Connection terminated unexpectedly");
          clearTimeout(this.connectionTimeoutHandle);
          this._errorAllQueries(error);
          this._ended = true;
          if (!this._ending) {
            if (this._connecting && !this._connectionError) {
              if (this._connectionCallback) {
                this._connectionCallback(error);
              } else {
                this._handleErrorEvent(error);
              }
            } else if (!this._connectionError) {
              this._handleErrorEvent(error);
            }
          }
          process.nextTick(() => {
            this.emit("end");
          });
        });
      }
      connect(callback) {
        if (callback) {
          this._connect(callback);
          return;
        }
        return new this._Promise((resolve2, reject) => {
          this._connect((error) => {
            if (error) {
              reject(error);
            } else {
              resolve2(this);
            }
          });
        });
      }
      _attachListeners(con) {
        con.on("authenticationCleartextPassword", this._handleAuthCleartextPassword.bind(this));
        con.on("authenticationMD5Password", this._handleAuthMD5Password.bind(this));
        con.on("authenticationSASL", this._handleAuthSASL.bind(this));
        con.on("authenticationSASLContinue", this._handleAuthSASLContinue.bind(this));
        con.on("authenticationSASLFinal", this._handleAuthSASLFinal.bind(this));
        con.on("backendKeyData", this._handleBackendKeyData.bind(this));
        con.on("error", this._handleErrorEvent.bind(this));
        con.on("errorMessage", this._handleErrorMessage.bind(this));
        con.on("readyForQuery", this._handleReadyForQuery.bind(this));
        con.on("notice", this._handleNotice.bind(this));
        con.on("rowDescription", this._handleRowDescription.bind(this));
        con.on("dataRow", this._handleDataRow.bind(this));
        con.on("portalSuspended", this._handlePortalSuspended.bind(this));
        con.on("emptyQuery", this._handleEmptyQuery.bind(this));
        con.on("commandComplete", this._handleCommandComplete.bind(this));
        con.on("parseComplete", this._handleParseComplete.bind(this));
        con.on("copyInResponse", this._handleCopyInResponse.bind(this));
        con.on("copyData", this._handleCopyData.bind(this));
        con.on("notification", this._handleNotification.bind(this));
      }
      _getPassword(cb) {
        const con = this.connection;
        if (typeof this.password === "function") {
          this._Promise.resolve().then(() => this.password(this.connectionParameters)).then((pass) => {
            if (pass !== void 0) {
              if (typeof pass !== "string") {
                con.emit("error", new TypeError("Password must be a string"));
                return;
              }
              this.connectionParameters.password = this.password = pass;
            } else {
              this.connectionParameters.password = this.password = null;
            }
            cb();
          }).catch((err) => {
            con.emit("error", err);
          });
        } else if (this.password !== null) {
          cb();
        } else {
          try {
            const pgPass = require_lib();
            pgPass(this.connectionParameters, (pass) => {
              if (void 0 !== pass) {
                pgPassDeprecationNotice();
                this.connectionParameters.password = this.password = pass;
              }
              cb();
            });
          } catch (e) {
            this.emit("error", e);
          }
        }
      }
      _handleAuthCleartextPassword(msg) {
        this._getPassword(() => {
          this.connection.password(this.password);
        });
      }
      _handleAuthMD5Password(msg) {
        this._getPassword(async () => {
          try {
            const hashedPassword = await crypto.postgresMd5PasswordHash(this.user, this.password, msg.salt);
            this.connection.password(hashedPassword);
          } catch (e) {
            this.emit("error", e);
          }
        });
      }
      _handleAuthSASL(msg) {
        this._getPassword(() => {
          try {
            this.saslSession = sasl.startSession(
              msg.mechanisms,
              this.enableChannelBinding && this.connection.stream,
              this.scramMaxIterations
            );
            this.connection.sendSASLInitialResponseMessage(this.saslSession.mechanism, this.saslSession.response);
          } catch (err) {
            this.connection.emit("error", err);
          }
        });
      }
      async _handleAuthSASLContinue(msg) {
        try {
          await sasl.continueSession(
            this.saslSession,
            this.password,
            msg.data,
            this.enableChannelBinding && this.connection.stream
          );
          this.connection.sendSCRAMClientFinalMessage(this.saslSession.response);
        } catch (err) {
          this.connection.emit("error", err);
        }
      }
      _handleAuthSASLFinal(msg) {
        try {
          sasl.finalizeSession(this.saslSession, msg.data);
          this.saslSession = null;
        } catch (err) {
          this.connection.emit("error", err);
        }
      }
      _handleBackendKeyData(msg) {
        this.processID = msg.processID;
        this.secretKey = msg.secretKey;
      }
      _handleReadyForQuery(msg) {
        if (this._connecting) {
          this._connecting = false;
          this._connected = true;
          clearTimeout(this.connectionTimeoutHandle);
          if (this._connectionCallback) {
            this._connectionCallback(null, this);
            this._connectionCallback = null;
          }
          this.emit("connect");
        }
        const activeQuery = this._getActiveQuery();
        this._activeQuery = null;
        this._txStatus = msg?.status ?? null;
        this.readyForQuery = true;
        if (activeQuery) {
          activeQuery.handleReadyForQuery(this.connection);
        }
        this._pulseQueryQueue();
      }
      // if we receive an error event or error message
      // during the connection process we handle it here
      _handleErrorWhileConnecting(err) {
        if (this._connectionError) {
          return;
        }
        this._connectionError = true;
        clearTimeout(this.connectionTimeoutHandle);
        if (this._connectionCallback) {
          return this._connectionCallback(err);
        }
        this.emit("error", err);
      }
      // if we're connected and we receive an error event from the connection
      // this means the socket is dead - do a hard abort of all queries and emit
      // the socket error on the client as well
      _handleErrorEvent(err) {
        if (this._connecting) {
          return this._handleErrorWhileConnecting(err);
        }
        this._queryable = false;
        this._errorAllQueries(err);
        this.emit("error", err);
      }
      // handle error messages from the postgres backend
      _handleErrorMessage(msg) {
        if (this._connecting) {
          return this._handleErrorWhileConnecting(msg);
        }
        const activeQuery = this._getActiveQuery();
        if (!activeQuery) {
          this._handleErrorEvent(msg);
          return;
        }
        this._activeQuery = null;
        if (activeQuery.name) {
          delete this.connection.submittedNamedStatements[activeQuery.name];
        }
        activeQuery.handleError(msg, this.connection);
      }
      _handleRowDescription(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected rowDescription message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleRowDescription(msg);
      }
      _handleDataRow(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected dataRow message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleDataRow(msg);
      }
      _handlePortalSuspended(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected portalSuspended message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handlePortalSuspended(this.connection);
      }
      _handleEmptyQuery(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected emptyQuery message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleEmptyQuery(this.connection);
      }
      _handleCommandComplete(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected commandComplete message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleCommandComplete(msg, this.connection);
      }
      _handleParseComplete() {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected parseComplete message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        if (activeQuery.name) {
          this.connection.parsedStatements[activeQuery.name] = activeQuery.text;
          delete this.connection.submittedNamedStatements[activeQuery.name];
        }
      }
      _handleCopyInResponse(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected copyInResponse message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleCopyInResponse(this.connection);
      }
      _handleCopyData(msg) {
        const activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          const error = new Error("Received unexpected copyData message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleCopyData(msg, this.connection);
      }
      _handleNotification(msg) {
        this.emit("notification", msg);
      }
      _handleNotice(msg) {
        this.emit("notice", msg);
      }
      getStartupConf() {
        const params = this.connectionParameters;
        const data = {
          user: params.user,
          database: params.database
        };
        const appName = params.application_name || params.fallback_application_name;
        if (appName) {
          data.application_name = appName;
        }
        if (params.replication) {
          data.replication = "" + params.replication;
        }
        if (params.statement_timeout) {
          data.statement_timeout = String(parseInt(params.statement_timeout, 10));
        }
        if (params.lock_timeout) {
          data.lock_timeout = String(parseInt(params.lock_timeout, 10));
        }
        if (params.idle_in_transaction_session_timeout) {
          data.idle_in_transaction_session_timeout = String(parseInt(params.idle_in_transaction_session_timeout, 10));
        }
        if (params.options) {
          data.options = params.options;
        }
        return data;
      }
      cancel(client, query) {
        if (client.activeQuery === query) {
          const con = this.connection;
          if (this.host && this.host.indexOf("/") === 0) {
            con.connect(this.host + "/.s.PGSQL." + this.port);
          } else {
            con.connect(this.port, this.host);
          }
          con.on("connect", function() {
            con.cancel(client.processID, client.secretKey);
          });
        } else if (client._queryQueue.indexOf(query) !== -1) {
          client._queryQueue.splice(client._queryQueue.indexOf(query), 1);
        } else if (client._sentQueryQueue.indexOf(query) !== -1) {
          query.callback = () => {
          };
        }
      }
      setTypeParser(oid, format, parseFn) {
        return this._types.setTypeParser(oid, format, parseFn);
      }
      getTypeParser(oid, format) {
        return this._types.getTypeParser(oid, format);
      }
      // escapeIdentifier and escapeLiteral moved to utility functions & exported
      // on PG
      // re-exported here for backwards compatibility
      escapeIdentifier(str2) {
        return utils.escapeIdentifier(str2);
      }
      escapeLiteral(str2) {
        return utils.escapeLiteral(str2);
      }
      _pulseQueryQueue() {
        if (this.pipeline) {
          this._pulsePipelinedQueryQueue();
          return;
        }
        if (this.readyForQuery === true) {
          this._activeQuery = this._queryQueue.shift();
          const activeQuery = this._getActiveQuery();
          if (activeQuery) {
            this.readyForQuery = false;
            this.hasExecuted = true;
            const queryError = activeQuery.submit(this.connection);
            if (queryError) {
              process.nextTick(() => {
                activeQuery.handleError(queryError, this.connection);
                this.readyForQuery = true;
                this._pulseQueryQueue();
              });
            }
          } else if (this.hasExecuted) {
            this._activeQuery = null;
            this.emit("drain");
          }
        }
      }
      _pulsePipelinedQueryQueue() {
        if (!this._connected || !this._queryable) {
          return;
        }
        while (this._queryQueue.length > 0) {
          const query = this._queryQueue.shift();
          this.hasExecuted = true;
          const queryError = query.submit(this.connection);
          if (queryError) {
            process.nextTick(() => {
              query.handleError(queryError, this.connection);
            });
            continue;
          }
          this._sentQueryQueue.push(query);
        }
        if (this.readyForQuery && !this._activeQuery && this._sentQueryQueue.length > 0) {
          this._activeQuery = this._sentQueryQueue.shift();
          this.readyForQuery = false;
        }
        if (!this._activeQuery && this._sentQueryQueue.length === 0 && this._queryQueue.length === 0 && this.hasExecuted) {
          this.emit("drain");
        }
      }
      query(config, values, callback) {
        let query;
        let result;
        if (config == null) {
          throw new TypeError("Client was passed a null or undefined query");
        }
        if (typeof config.submit === "function") {
          result = query = config;
          if (!query.callback) {
            if (typeof values === "function") {
              query.callback = values;
            } else if (callback) {
              query.callback = callback;
            }
          }
        } else {
          query = new Query2(config, values, callback);
          if (!query.callback) {
            result = new this._Promise((resolve2, reject) => {
              query.callback = (err, res) => err ? reject(err) : resolve2(res);
            }).catch((err) => {
              Error.captureStackTrace(err);
              throw err;
            });
          } else if (typeof query.callback !== "function") {
            throw new TypeError("callback is not a function");
          }
        }
        const readTimeout = config.query_timeout || this.connectionParameters.query_timeout;
        if (readTimeout) {
          const queryCallback = query.callback || (() => {
          });
          const readTimeoutTimer = setTimeout(() => {
            const error = new Error("Query read timeout");
            process.nextTick(() => {
              query.handleError(error, this.connection);
            });
            queryCallback(error);
            query.callback = () => {
            };
            const index = this._queryQueue.indexOf(query);
            if (index > -1) {
              this._queryQueue.splice(index, 1);
            } else if (this.pipeline) {
              this.connection.stream.destroy();
              return;
            }
            this._pulseQueryQueue();
          }, readTimeout);
          query.callback = (err, res) => {
            clearTimeout(readTimeoutTimer);
            queryCallback(err, res);
          };
        }
        if (this.binary && !query.binary) {
          query.binary = true;
        }
        if (query._result && !query._result._types) {
          query._result._types = this._types;
        }
        if (!this._queryable) {
          process.nextTick(() => {
            query.handleError(new Error("Client has encountered a connection error and is not queryable"), this.connection);
          });
          return result;
        }
        if (this._ending) {
          process.nextTick(() => {
            query.handleError(new Error("Client was closed and is not queryable"), this.connection);
          });
          return result;
        }
        if (this._queryQueue.length > 0 && !this.pipeline) {
          queryQueueLengthDeprecationNotice();
        }
        this._queryQueue.push(query);
        this._pulseQueryQueue();
        return result;
      }
      ref() {
        this.connection.ref();
      }
      unref() {
        this.connection.unref();
      }
      getTransactionStatus() {
        return this._txStatus;
      }
      end(cb) {
        this._ending = true;
        if (!this.connection._connecting || this._ended) {
          if (cb) {
            cb();
            return;
          } else {
            return this._Promise.resolve();
          }
        }
        if (!this._queryable) {
          this.connection.stream.destroy();
        } else if (this.pipeline && (this._getActiveQuery() || this._sentQueryQueue.length > 0 || this._queryQueue.length > 0)) {
          this.once("drain", () => this.connection.end());
        } else if (this._getActiveQuery()) {
          this.connection.stream.destroy();
        } else {
          this.connection.end();
        }
        if (cb) {
          this.connection.once("end", cb);
        } else {
          return new this._Promise((resolve2) => {
            this.connection.once("end", resolve2);
          });
        }
      }
      get queryQueue() {
        queryQueueDeprecationNotice();
        return this._queryQueue;
      }
    };
    Client2.Query = Query2;
    module.exports = Client2;
  }
});

// node_modules/pg-pool/index.js
var require_pg_pool = __commonJS({
  "node_modules/pg-pool/index.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events").EventEmitter;
    var NOOP = function() {
    };
    var removeWhere = (list, predicate) => {
      const i = list.findIndex(predicate);
      return i === -1 ? void 0 : list.splice(i, 1)[0];
    };
    var IdleItem = class {
      constructor(client, idleListener, timeoutId) {
        this.client = client;
        this.idleListener = idleListener;
        this.timeoutId = timeoutId;
      }
    };
    var PendingItem = class {
      constructor(callback) {
        this.callback = callback;
      }
    };
    function throwOnDoubleRelease() {
      throw new Error("Release called on client which has already been released to the pool.");
    }
    function promisify(Promise2, callback) {
      if (callback) {
        return { callback, result: void 0 };
      }
      let rej;
      let res;
      const cb = function(err, client) {
        err ? rej(err) : res(client);
      };
      const result = new Promise2(function(resolve2, reject) {
        res = resolve2;
        rej = reject;
      }).catch((err) => {
        Error.captureStackTrace(err);
        throw err;
      });
      return { callback: cb, result };
    }
    function makeIdleListener(pool, client) {
      return function idleListener(err) {
        err.client = client;
        client.removeListener("error", idleListener);
        client.on("error", () => {
          pool.log("additional client error after disconnection due to error", err);
        });
        pool._remove(client);
        pool.emit("error", err, client);
      };
    }
    var Pool2 = class extends EventEmitter {
      constructor(options, Client2) {
        super();
        this.options = Object.assign({}, options);
        if (options != null && "password" in options) {
          Object.defineProperty(this.options, "password", {
            configurable: true,
            enumerable: false,
            writable: true,
            value: options.password
          });
        }
        if (options != null && options.ssl && options.ssl.key) {
          Object.defineProperty(this.options.ssl, "key", {
            enumerable: false
          });
        }
        this.options.max = this.options.max || this.options.poolSize || 10;
        this.options.min = this.options.min || 0;
        this.options.maxUses = this.options.maxUses || Infinity;
        this.options.allowExitOnIdle = this.options.allowExitOnIdle || false;
        this.options.maxLifetimeSeconds = this.options.maxLifetimeSeconds || 0;
        this.log = this.options.log || function() {
        };
        this.Client = this.options.Client || Client2 || require_lib2().Client;
        this.Promise = this.options.Promise || global.Promise;
        if (typeof this.options.idleTimeoutMillis === "undefined") {
          this.options.idleTimeoutMillis = 1e4;
        }
        this._clients = [];
        this._idle = [];
        this._expired = /* @__PURE__ */ new WeakSet();
        this._pendingQueue = [];
        this._endCallback = void 0;
        this.ending = false;
        this.ended = false;
      }
      _promiseTry(f) {
        const Promise2 = this.Promise;
        if (typeof Promise2.try === "function") {
          return Promise2.try(f);
        }
        return new Promise2((resolve2) => resolve2(f()));
      }
      _isFull() {
        return this._clients.length >= this.options.max;
      }
      _isAboveMin() {
        return this._clients.length > this.options.min;
      }
      _pulseQueue() {
        this.log("pulse queue");
        if (this.ended) {
          this.log("pulse queue ended");
          return;
        }
        if (this.ending) {
          this.log("pulse queue on ending");
          if (this._idle.length) {
            this._idle.slice().map((item) => {
              this._remove(item.client);
            });
          }
          if (!this._clients.length) {
            this.ended = true;
            this._endCallback();
          }
          return;
        }
        if (!this._pendingQueue.length) {
          this.log("no queued requests");
          return;
        }
        if (!this._idle.length && this._isFull()) {
          return;
        }
        const pendingItem = this._pendingQueue.shift();
        if (this._idle.length) {
          const idleItem = this._idle.pop();
          clearTimeout(idleItem.timeoutId);
          const client = idleItem.client;
          client.ref && client.ref();
          const idleListener = idleItem.idleListener;
          return this._acquireClient(client, pendingItem, idleListener, false);
        }
        if (!this._isFull()) {
          return this.newClient(pendingItem);
        }
        throw new Error("unexpected condition");
      }
      _remove(client, callback) {
        const removed = removeWhere(this._idle, (item) => item.client === client);
        if (removed !== void 0) {
          clearTimeout(removed.timeoutId);
        }
        this._clients = this._clients.filter((c) => c !== client);
        const context = this;
        client.end(() => {
          context.emit("remove", client);
          if (typeof callback === "function") {
            callback();
          }
        });
      }
      connect(cb) {
        if (this.ending) {
          const err = new Error("Cannot use a pool after calling end on the pool");
          return cb ? cb(err) : this.Promise.reject(err);
        }
        const response = promisify(this.Promise, cb);
        const result = response.result;
        if (this._isFull() || this._idle.length) {
          if (this._idle.length) {
            process.nextTick(() => this._pulseQueue());
          }
          if (!this.options.connectionTimeoutMillis) {
            this._pendingQueue.push(new PendingItem(response.callback));
            return result;
          }
          const queueCallback = (err, res, done) => {
            clearTimeout(tid);
            response.callback(err, res, done);
          };
          const pendingItem = new PendingItem(queueCallback);
          const tid = setTimeout(() => {
            removeWhere(this._pendingQueue, (i) => i.callback === queueCallback);
            pendingItem.timedOut = true;
            response.callback(new Error("timeout exceeded when trying to connect"));
          }, this.options.connectionTimeoutMillis);
          if (tid.unref) {
            tid.unref();
          }
          this._pendingQueue.push(pendingItem);
          return result;
        }
        this.newClient(new PendingItem(response.callback));
        return result;
      }
      newClient(pendingItem) {
        const client = new this.Client(this.options);
        this._clients.push(client);
        const idleListener = makeIdleListener(this, client);
        this.log("checking client timeout");
        let tid;
        let timeoutHit = false;
        if (this.options.connectionTimeoutMillis) {
          tid = setTimeout(() => {
            if (client.connection) {
              this.log("ending client due to timeout");
              timeoutHit = true;
              client.connection.stream.destroy();
            } else if (!client.isConnected()) {
              this.log("ending client due to timeout");
              timeoutHit = true;
              client.end();
            }
          }, this.options.connectionTimeoutMillis);
        }
        this.log("connecting new client");
        client.connect((err) => {
          if (tid) {
            clearTimeout(tid);
          }
          client.on("error", idleListener);
          if (err) {
            this.log("client failed to connect", err);
            this._clients = this._clients.filter((c) => c !== client);
            if (timeoutHit) {
              err = new Error("Connection terminated due to connection timeout", { cause: err });
            }
            this._pulseQueue();
            if (!pendingItem.timedOut) {
              pendingItem.callback(err, void 0, NOOP);
            }
          } else {
            this.log("new client connected");
            if (this.options.onConnect) {
              this._promiseTry(() => this.options.onConnect(client)).then(
                () => {
                  this._afterConnect(client, pendingItem, idleListener);
                },
                (hookErr) => {
                  this._clients = this._clients.filter((c) => c !== client);
                  client.end(() => {
                    this._pulseQueue();
                    if (!pendingItem.timedOut) {
                      pendingItem.callback(hookErr, void 0, NOOP);
                    }
                  });
                }
              );
              return;
            }
            return this._afterConnect(client, pendingItem, idleListener);
          }
        });
      }
      _afterConnect(client, pendingItem, idleListener) {
        if (this.options.maxLifetimeSeconds !== 0) {
          const maxLifetimeTimeout = setTimeout(() => {
            this.log("ending client due to expired lifetime");
            this._expired.add(client);
            const idleIndex = this._idle.findIndex((idleItem) => idleItem.client === client);
            if (idleIndex !== -1) {
              this._acquireClient(
                client,
                new PendingItem((err, client2, clientRelease) => clientRelease()),
                idleListener,
                false
              );
            }
          }, this.options.maxLifetimeSeconds * 1e3);
          maxLifetimeTimeout.unref();
          client.once("end", () => clearTimeout(maxLifetimeTimeout));
        }
        return this._acquireClient(client, pendingItem, idleListener, true);
      }
      // acquire a client for a pending work item
      _acquireClient(client, pendingItem, idleListener, isNew) {
        if (isNew) {
          this.emit("connect", client);
        }
        this.emit("acquire", client);
        client.release = this._releaseOnce(client, idleListener);
        client.removeListener("error", idleListener);
        if (!pendingItem.timedOut) {
          if (isNew && this.options.verify) {
            this.options.verify(client, (err) => {
              if (err) {
                client.release(err);
                return pendingItem.callback(err, void 0, NOOP);
              }
              pendingItem.callback(void 0, client, client.release);
            });
          } else {
            pendingItem.callback(void 0, client, client.release);
          }
        } else {
          if (isNew && this.options.verify) {
            this.options.verify(client, client.release);
          } else {
            client.release();
          }
        }
      }
      // returns a function that wraps _release and throws if called more than once
      _releaseOnce(client, idleListener) {
        let released = false;
        return (err) => {
          if (released) {
            throwOnDoubleRelease();
          }
          released = true;
          this._release(client, idleListener, err);
        };
      }
      // release a client back to the poll, include an error
      // to remove it from the pool
      _release(client, idleListener, err) {
        client.on("error", idleListener);
        client._poolUseCount = (client._poolUseCount || 0) + 1;
        this.emit("release", err, client);
        if (err || this.ending || !client._queryable || client._ending || client._poolUseCount >= this.options.maxUses) {
          if (client._poolUseCount >= this.options.maxUses) {
            this.log("remove expended client");
          }
          return this._remove(client, this._pulseQueue.bind(this));
        }
        const isExpired = this._expired.has(client);
        if (isExpired) {
          this.log("remove expired client");
          this._expired.delete(client);
          return this._remove(client, this._pulseQueue.bind(this));
        }
        let tid;
        if (this.options.idleTimeoutMillis && this._isAboveMin()) {
          tid = setTimeout(() => {
            if (this._isAboveMin()) {
              this.log("remove idle client");
              this._remove(client, this._pulseQueue.bind(this));
            }
          }, this.options.idleTimeoutMillis);
          if (this.options.allowExitOnIdle) {
            tid.unref();
          }
        }
        if (this.options.allowExitOnIdle) {
          client.unref();
        }
        this._idle.push(new IdleItem(client, idleListener, tid));
        this._pulseQueue();
      }
      query(text, values, cb) {
        if (typeof text === "function") {
          const response2 = promisify(this.Promise, text);
          setImmediate(function() {
            return response2.callback(new Error("Passing a function as the first parameter to pool.query is not supported"));
          });
          return response2.result;
        }
        if (typeof values === "function") {
          cb = values;
          values = void 0;
        }
        const response = promisify(this.Promise, cb);
        cb = response.callback;
        this.connect((err, client) => {
          if (err) {
            return cb(err);
          }
          let clientReleased = false;
          const onError = (err2) => {
            if (clientReleased) {
              return;
            }
            clientReleased = true;
            client.release(err2);
            cb(err2);
          };
          client.once("error", onError);
          this.log("dispatching query");
          try {
            client.query(text, values, (err2, res) => {
              this.log("query dispatched");
              client.removeListener("error", onError);
              if (clientReleased) {
                return;
              }
              clientReleased = true;
              client.release(err2);
              if (err2) {
                return cb(err2);
              }
              return cb(void 0, res);
            });
          } catch (err2) {
            client.release(err2);
            return cb(err2);
          }
        });
        return response.result;
      }
      end(cb) {
        this.log("ending");
        if (this.ending) {
          const err = new Error("Called end on pool more than once");
          return cb ? cb(err) : this.Promise.reject(err);
        }
        this.ending = true;
        const promised2 = promisify(this.Promise, cb);
        this._endCallback = promised2.callback;
        this._pulseQueue();
        return promised2.result;
      }
      get waitingCount() {
        return this._pendingQueue.length;
      }
      get idleCount() {
        return this._idle.length;
      }
      get expiredCount() {
        return this._clients.reduce((acc, client) => acc + (this._expired.has(client) ? 1 : 0), 0);
      }
      get totalCount() {
        return this._clients.length;
      }
    };
    module.exports = Pool2;
  }
});

// node_modules/pg/lib/native/query.js
var require_query2 = __commonJS({
  "node_modules/pg/lib/native/query.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events").EventEmitter;
    var util = __require("util");
    var utils = require_utils();
    var NativeQuery = module.exports = function(config, values, callback) {
      EventEmitter.call(this);
      config = utils.normalizeQueryConfig(config, values, callback);
      this.text = config.text;
      this.values = config.values;
      this.name = config.name;
      this.queryMode = config.queryMode;
      this.callback = config.callback;
      this.state = "new";
      this._arrayMode = config.rowMode === "array";
      this._emitRowEvents = false;
      this.on(
        "newListener",
        function(event) {
          if (event === "row") this._emitRowEvents = true;
        }.bind(this)
      );
    };
    util.inherits(NativeQuery, EventEmitter);
    var errorFieldMap = {
      sqlState: "code",
      statementPosition: "position",
      messagePrimary: "message",
      context: "where",
      schemaName: "schema",
      tableName: "table",
      columnName: "column",
      dataTypeName: "dataType",
      constraintName: "constraint",
      sourceFile: "file",
      sourceLine: "line",
      sourceFunction: "routine"
    };
    NativeQuery.prototype.handleError = function(err) {
      const fields = this.native && this.native.pq.resultErrorFields();
      if (fields) {
        for (const key in fields) {
          const normalizedFieldName = errorFieldMap[key] || key;
          err[normalizedFieldName] = fields[key];
        }
      }
      if (this.callback) {
        this.callback(err);
      } else {
        this.emit("error", err);
      }
      this.state = "error";
    };
    NativeQuery.prototype.then = function(onSuccess, onFailure) {
      return this._getPromise().then(onSuccess, onFailure);
    };
    NativeQuery.prototype.catch = function(callback) {
      return this._getPromise().catch(callback);
    };
    NativeQuery.prototype._getPromise = function() {
      if (this._promise) return this._promise;
      this._promise = new Promise(
        function(resolve2, reject) {
          this._once("end", resolve2);
          this._once("error", reject);
        }.bind(this)
      );
      return this._promise;
    };
    NativeQuery.prototype.submit = function(client) {
      this.state = "running";
      const self = this;
      this.native = client.native;
      client.native.arrayMode = this._arrayMode;
      let after = function(err, rows, results) {
        client.native.arrayMode = false;
        setImmediate(function() {
          self.emit("_done");
        });
        if (err) {
          return self.handleError(err);
        }
        if (self._emitRowEvents) {
          if (results.length > 1) {
            rows.forEach((rowOfRows, i) => {
              rowOfRows.forEach((row) => {
                self.emit("row", row, results[i]);
              });
            });
          } else {
            rows.forEach(function(row) {
              self.emit("row", row, results);
            });
          }
        }
        self.state = "end";
        self.emit("end", results);
        if (self.callback) {
          self.callback(null, results);
        }
      };
      if (process.domain) {
        after = process.domain.bind(after);
      }
      if (this.name) {
        if (this.name.length > 63) {
          console.error("Warning! Postgres only supports 63 characters for query names.");
          console.error("You supplied %s (%s)", this.name, this.name.length);
          console.error("This can cause conflicts and silent errors executing queries");
        }
        const values = (this.values || []).map(utils.prepareValue);
        if (client.namedQueries[this.name]) {
          if (this.text && client.namedQueries[this.name] !== this.text) {
            const err = new Error(`Prepared statements must be unique - '${this.name}' was used for a different statement`);
            return after(err);
          }
          return client.native.execute(this.name, values, after);
        }
        return client.native.prepare(this.name, this.text, values.length, function(err) {
          if (err) return after(err);
          client.namedQueries[self.name] = self.text;
          return self.native.execute(self.name, values, after);
        });
      } else if (this.values) {
        if (!Array.isArray(this.values)) {
          const err = new Error("Query values must be an array");
          return after(err);
        }
        const vals = this.values.map(utils.prepareValue);
        client.native.query(this.text, vals, after);
      } else if (this.queryMode === "extended") {
        client.native.query(this.text, [], after);
      } else {
        client.native.query(this.text, after);
      }
    };
  }
});

// node_modules/pg/lib/native/client.js
var require_client2 = __commonJS({
  "node_modules/pg/lib/native/client.js"(exports, module) {
    var nodeUtils = __require("util");
    var Native;
    try {
      Native = __require("pg-native");
    } catch (e) {
      throw e;
    }
    var TypeOverrides2 = require_type_overrides();
    var EventEmitter = __require("events").EventEmitter;
    var util = __require("util");
    var ConnectionParameters = require_connection_parameters();
    var NativeQuery = require_query2();
    var queryQueueLengthDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Calling client.query() when the client is already executing a query is deprecated and will be removed in pg@9.0. Use async/await or an external async flow control mechanism instead."
    );
    var Client2 = module.exports = function(config) {
      EventEmitter.call(this);
      config = config || {};
      this._Promise = config.Promise || global.Promise;
      this._types = new TypeOverrides2(config.types);
      this.native = new Native({
        types: this._types
      });
      this._queryQueue = [];
      this._ending = false;
      this._connecting = false;
      this._connected = false;
      this._queryable = true;
      this.pipeline = Boolean(config.pipeline);
      this._pipelineInFlight = false;
      const cp = this.connectionParameters = new ConnectionParameters(config);
      if (config.nativeConnectionString) cp.nativeConnectionString = config.nativeConnectionString;
      this.user = cp.user;
      Object.defineProperty(this, "password", {
        configurable: true,
        enumerable: false,
        writable: true,
        value: cp.password
      });
      this.database = cp.database;
      this.host = cp.host;
      this.port = cp.port;
      this.namedQueries = {};
    };
    Client2.Query = NativeQuery;
    util.inherits(Client2, EventEmitter);
    Client2.prototype._errorAllQueries = function(err) {
      const enqueueError = (query) => {
        process.nextTick(() => {
          query.native = this.native;
          query.handleError(err);
        });
      };
      if (this._hasActiveQuery()) {
        enqueueError(this._activeQuery);
        this._activeQuery = null;
      }
      this._queryQueue.forEach(enqueueError);
      this._queryQueue.length = 0;
    };
    Client2.prototype._connect = function(cb) {
      const self = this;
      if (this._connecting) {
        process.nextTick(() => cb(new Error("Client has already been connected. You cannot reuse a client.")));
        return;
      }
      this._connecting = true;
      this.connectionParameters.getLibpqConnectionString(function(err, conString) {
        if (self.connectionParameters.nativeConnectionString) conString = self.connectionParameters.nativeConnectionString;
        if (err) return cb(err);
        self.native.connect(conString, function(err2) {
          if (err2) {
            self.native.end();
            return cb(err2);
          }
          self._connected = true;
          self.native.on("error", function(err3) {
            self._queryable = false;
            self._errorAllQueries(err3);
            self.emit("error", err3);
          });
          self.native.on("notification", function(msg) {
            self.emit("notification", {
              channel: msg.relname,
              payload: msg.extra
            });
          });
          self.emit("connect");
          self._pulseQueryQueue(true);
          cb(null, this);
        });
      });
    };
    Client2.prototype.connect = function(callback) {
      if (callback) {
        this._connect(callback);
        return;
      }
      return new this._Promise((resolve2, reject) => {
        this._connect((error) => {
          if (error) {
            reject(error);
          } else {
            resolve2(this);
          }
        });
      });
    };
    Client2.prototype.query = function(config, values, callback) {
      let query;
      let result;
      let readTimeout;
      let readTimeoutTimer;
      let queryCallback;
      if (config === null || config === void 0) {
        throw new TypeError("Client was passed a null or undefined query");
      } else if (typeof config.submit === "function") {
        readTimeout = config.query_timeout || this.connectionParameters.query_timeout;
        result = query = config;
        if (typeof values === "function") {
          config.callback = values;
        }
      } else {
        readTimeout = config.query_timeout || this.connectionParameters.query_timeout;
        query = new NativeQuery(config, values, callback);
        if (!query.callback) {
          let resolveOut, rejectOut;
          result = new this._Promise((resolve2, reject) => {
            resolveOut = resolve2;
            rejectOut = reject;
          }).catch((err) => {
            Error.captureStackTrace(err);
            throw err;
          });
          query.callback = (err, res) => err ? rejectOut(err) : resolveOut(res);
        }
      }
      if (readTimeout) {
        queryCallback = query.callback || (() => {
        });
        readTimeoutTimer = setTimeout(() => {
          const error = new Error("Query read timeout");
          process.nextTick(() => {
            query.handleError(error, this.connection);
          });
          queryCallback(error);
          query.callback = () => {
          };
          const index = this._queryQueue.indexOf(query);
          if (index > -1) {
            this._queryQueue.splice(index, 1);
          }
          this._pulseQueryQueue();
        }, readTimeout);
        query.callback = (err, res) => {
          clearTimeout(readTimeoutTimer);
          queryCallback(err, res);
        };
      }
      if (!this._queryable) {
        query.native = this.native;
        process.nextTick(() => {
          query.handleError(new Error("Client has encountered a connection error and is not queryable"));
        });
        return result;
      }
      if (this._ending) {
        query.native = this.native;
        process.nextTick(() => {
          query.handleError(new Error("Client was closed and is not queryable"));
        });
        return result;
      }
      if (this._queryQueue.length > 0 && !this.pipeline) {
        queryQueueLengthDeprecationNotice();
      }
      this._queryQueue.push(query);
      this._pulseQueryQueue();
      return result;
    };
    Client2.prototype.end = function(cb) {
      const self = this;
      this._ending = true;
      if (this._connecting && !this._connected) {
        this.once("connect", () => {
          this.end(() => {
          });
        });
      }
      let result;
      if (!cb) {
        result = new this._Promise(function(resolve2, reject) {
          cb = (err) => err ? reject(err) : resolve2();
        });
      }
      const doEnd = function() {
        self.native.end(function() {
          self._connected = false;
          self._errorAllQueries(new Error("Connection terminated"));
          process.nextTick(() => {
            self.emit("end");
            if (cb) cb();
          });
        });
      };
      if (this.pipeline && (this._pipelineInFlight || this._queryQueue.length > 0)) {
        this.once("drain", doEnd);
      } else {
        doEnd();
      }
      return result;
    };
    Client2.prototype._hasActiveQuery = function() {
      return this._activeQuery && this._activeQuery.state !== "error" && this._activeQuery.state !== "end";
    };
    Client2.prototype._pulseQueryQueue = function(initialConnection) {
      if (!this._connected) {
        return;
      }
      if (this.pipeline && !initialConnection) {
        return this._pulsePipelinedQueryQueue();
      }
      if (this._hasActiveQuery()) {
        return;
      }
      const query = this._queryQueue.shift();
      if (!query) {
        if (!initialConnection) {
          this.emit("drain");
        }
        return;
      }
      this._activeQuery = query;
      query.submit(this);
      const self = this;
      query.once("_done", function() {
        self._pulseQueryQueue();
      });
    };
    Client2.prototype._pulsePipelinedQueryQueue = function() {
      if (!this._connected || this._pipelineInFlight) {
        return;
      }
      if (this._queryQueue.length === 0) {
        if (this.hasExecuted) {
          this.emit("drain");
        }
        return;
      }
      this._pipelineInFlight = true;
      const self = this;
      const queries = [];
      const nativeQueries = [];
      const utils = require_utils();
      while (this._queryQueue.length > 0) {
        const query = this._queryQueue.shift();
        this.hasExecuted = true;
        nativeQueries.push(query);
        const values = query.values ? query.values.map(utils.prepareValue) : null;
        const pipelineEntry = { text: query.text, name: query.name };
        if (values) {
          pipelineEntry.values = values;
        }
        if (query.name && this.namedQueries[query.name]) {
          pipelineEntry._alreadyPrepared = true;
        }
        queries.push(pipelineEntry);
      }
      this.native.pipeline(queries, function(err, results) {
        self._pipelineInFlight = false;
        if (err) {
          for (let i = 0; i < nativeQueries.length; i++) {
            const q = nativeQueries[i];
            q.native = self.native;
            q.handleError(err);
          }
          self._pulsePipelinedQueryQueue();
          return;
        }
        for (let i = 0; i < nativeQueries.length; i++) {
          const q = nativeQueries[i];
          const r = results[i];
          q.native = self.native;
          if (r.err) {
            q.handleError(r.err);
          } else {
            if (q.name) {
              self.namedQueries[q.name] = q.text;
            }
            q.state = "end";
            q.emit("end", r.result);
            if (q.callback) {
              q.callback(null, r.result);
            }
          }
          setImmediate(function() {
            q.emit("_done");
          });
        }
        self._pulsePipelinedQueryQueue();
      });
    };
    Client2.prototype.cancel = function(query) {
      if (this._activeQuery === query) {
        this.native.cancel(function() {
        });
      } else if (this._queryQueue.indexOf(query) !== -1) {
        this._queryQueue.splice(this._queryQueue.indexOf(query), 1);
      }
    };
    Client2.prototype.ref = function() {
    };
    Client2.prototype.unref = function() {
    };
    Client2.prototype.setTypeParser = function(oid, format, parseFn) {
      return this._types.setTypeParser(oid, format, parseFn);
    };
    Client2.prototype.getTypeParser = function(oid, format) {
      return this._types.getTypeParser(oid, format);
    };
    Client2.prototype.isConnected = function() {
      return this._connected;
    };
    Client2.prototype.getTransactionStatus = function() {
      return this.native.getTransactionStatus();
    };
  }
});

// node_modules/pg/lib/native/index.js
var require_native = __commonJS({
  "node_modules/pg/lib/native/index.js"(exports, module) {
    "use strict";
    module.exports = require_client2();
  }
});

// node_modules/pg/lib/index.js
var require_lib2 = __commonJS({
  "node_modules/pg/lib/index.js"(exports, module) {
    "use strict";
    var Client2 = require_client();
    var defaults2 = require_defaults();
    var Connection2 = require_connection();
    var Result2 = require_result();
    var utils = require_utils();
    var Pool2 = require_pg_pool();
    var TypeOverrides2 = require_type_overrides();
    var { DatabaseError: DatabaseError2 } = require_dist();
    var { escapeIdentifier: escapeIdentifier2, escapeLiteral: escapeLiteral2 } = require_utils();
    var poolFactory = (Client3) => {
      return class BoundPool extends Pool2 {
        constructor(options) {
          super(options, Client3);
        }
      };
    };
    var PG = function(clientConstructor2) {
      this.defaults = defaults2;
      this.Client = clientConstructor2;
      this.Query = this.Client.Query;
      this.Pool = poolFactory(this.Client);
      this._pools = [];
      this.Connection = Connection2;
      this.types = require_pg_types();
      this.DatabaseError = DatabaseError2;
      this.TypeOverrides = TypeOverrides2;
      this.escapeIdentifier = escapeIdentifier2;
      this.escapeLiteral = escapeLiteral2;
      this.Result = Result2;
      this.utils = utils;
    };
    var clientConstructor = Client2;
    var forceNative = false;
    try {
      forceNative = !!process.env.NODE_PG_FORCE_NATIVE;
    } catch {
    }
    if (forceNative) {
      clientConstructor = require_native();
    }
    module.exports = new PG(clientConstructor);
    Object.defineProperty(module.exports, "native", {
      configurable: true,
      enumerable: false,
      get() {
        let native = null;
        try {
          native = new PG(require_native());
        } catch (err) {
          if (err.code !== "MODULE_NOT_FOUND") {
            throw err;
          }
        }
        Object.defineProperty(module.exports, "native", {
          value: native
        });
        return native;
      }
    });
  }
});

// node_modules/pg/esm/index.mjs
var esm_exports = {};
__export(esm_exports, {
  Client: () => Client,
  Connection: () => Connection,
  DatabaseError: () => DatabaseError,
  Pool: () => Pool,
  Query: () => Query,
  Result: () => Result,
  TypeOverrides: () => TypeOverrides,
  default: () => esm_default,
  defaults: () => defaults,
  escapeIdentifier: () => escapeIdentifier,
  escapeLiteral: () => escapeLiteral,
  types: () => types
});
var import_lib, Client, Pool, Connection, types, Query, DatabaseError, escapeIdentifier, escapeLiteral, Result, TypeOverrides, defaults, esm_default;
var init_esm = __esm({
  "node_modules/pg/esm/index.mjs"() {
    import_lib = __toESM(require_lib2(), 1);
    Client = import_lib.default.Client;
    Pool = import_lib.default.Pool;
    Connection = import_lib.default.Connection;
    types = import_lib.default.types;
    Query = import_lib.default.Query;
    DatabaseError = import_lib.default.DatabaseError;
    escapeIdentifier = import_lib.default.escapeIdentifier;
    escapeLiteral = import_lib.default.escapeLiteral;
    Result = import_lib.default.Result;
    TypeOverrides = import_lib.default.TypeOverrides;
    defaults = import_lib.default.defaults;
    esm_default = import_lib.default;
  }
});

// server/index.ts
import { createServer } from "node:http";
import { createReadStream, existsSync, statSync } from "node:fs";
import { createHash as createHash2, randomBytes as randomBytes3, scrypt, timingSafeEqual as timingSafeEqual2 } from "node:crypto";
import { extname, join as join2, normalize, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// node_modules/ws/wrapper.mjs
var import_stream = __toESM(require_stream(), 1);
var import_extension = __toESM(require_extension(), 1);
var import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
var import_receiver = __toESM(require_receiver(), 1);
var import_sender = __toESM(require_sender(), 1);
var import_subprotocol = __toESM(require_subprotocol(), 1);
var import_websocket = __toESM(require_websocket(), 1);
var import_websocket_server = __toESM(require_websocket_server(), 1);

// server/store.ts
import { mkdirSync, readFileSync, readdirSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
var fileOf = (key) => `${key.replace(/[^a-zA-Z0-9_.-]/g, (c) => `~${c.charCodeAt(0).toString(16)}`)}.json`;
var FileStore = class {
  constructor(dir) {
    this.dir = dir;
    mkdirSync(dir, { recursive: true });
    this.kind = `files in ${dir}`;
  }
  kind;
  async get(key) {
    try {
      return JSON.parse(readFileSync(join(this.dir, fileOf(key)), "utf8"));
    } catch {
      return null;
    }
  }
  async set(key, value) {
    const path = join(this.dir, fileOf(key)), tmp = `${path}.tmp`;
    writeFileSync(tmp, JSON.stringify(value));
    renameSync(tmp, path);
  }
  async list(prefix) {
    const want = fileOf(prefix).replace(/\.json$/, "");
    const out = [];
    for (const f of readdirSync(this.dir)) {
      if (!f.startsWith(want) || !f.endsWith(".json")) continue;
      try {
        out.push({ key: f.slice(0, -5).replace(/~([0-9a-f]{2})/g, (_, h) => String.fromCharCode(parseInt(h, 16))), value: JSON.parse(readFileSync(join(this.dir, f), "utf8")) });
      } catch {
      }
    }
    return out;
  }
  async close() {
  }
};
var PgStore = class {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(pool) {
    this.pool = pool;
  }
  kind = "Postgres";
  async get(key) {
    const r = await this.pool.query("select v from tinyfleet_kv where k = $1", [key]);
    return r.rows[0]?.v ?? null;
  }
  async set(key, value) {
    await this.pool.query(
      "insert into tinyfleet_kv (k, v, at) values ($1, $2::jsonb, now()) on conflict (k) do update set v = excluded.v, at = excluded.at",
      [key, JSON.stringify(value)]
    );
  }
  async list(prefix) {
    const r = await this.pool.query("select k, v from tinyfleet_kv where k like $1", [`${prefix}%`]);
    return r.rows.map((row) => ({ key: row.k, value: row.v }));
  }
  async close() {
    await this.pool.end();
  }
};
async function openStore() {
  const url = process.env.DATABASE_URL;
  if (url) {
    const pg2 = await Promise.resolve().then(() => (init_esm(), esm_exports));
    const Pool2 = pg2.default?.Pool ?? pg2.Pool;
    const local = /localhost|127\.0\.0\.1|\.railway\.internal/.test(url);
    const pool = new Pool2({ connectionString: url, max: 4, ssl: local || process.env.PGSSL === "off" ? void 0 : { rejectUnauthorized: false } });
    await pool.query("create table if not exists tinyfleet_kv (k text primary key, v jsonb not null, at timestamptz not null default now())");
    return new PgStore(pool);
  }
  return new FileStore(process.env.DATA_DIR || join(process.cwd(), ".online-data"));
}
var Docs = class {
  constructor(store2, prefix, fresh) {
    this.store = store2;
    this.prefix = prefix;
    this.fresh = fresh;
  }
  live = /* @__PURE__ */ new Map();
  loading = /* @__PURE__ */ new Map();
  /** The document, read from the store the first time. */
  async open(key) {
    const have = this.live.get(key);
    if (have) {
      have.idle = 0;
      return have.doc;
    }
    let p = this.loading.get(key);
    if (!p) {
      p = this.store.get(this.prefix + key).then((v) => {
        const doc = v ?? this.fresh(key);
        this.live.set(key, { doc, dirty: false, held: 0, idle: 0 });
        this.loading.delete(key);
        return doc;
      });
      this.loading.set(key, p);
    }
    return p;
  }
  /** The document if it is in memory. */
  peek(key) {
    return this.live.get(key)?.doc ?? null;
  }
  hold(key, n) {
    const e = this.live.get(key);
    if (e) {
      e.held = Math.max(0, e.held + n);
      e.idle = 0;
    }
  }
  /** Put a document in place, new or not: written at the next flush. */
  set(key, doc) {
    const e = this.live.get(key);
    if (e) {
      e.doc = doc;
      e.dirty = true;
      e.idle = 0;
    } else this.live.set(key, { doc, dirty: true, held: 0, idle: 0 });
  }
  touch(key) {
    const e = this.live.get(key);
    if (e) e.dirty = true;
  }
  /** Write everything that changed; forget what is clean and has been let go for a while. */
  async flush(evict = true) {
    let n = 0;
    for (const [key, e] of this.live) {
      if (e.dirty) {
        e.dirty = false;
        try {
          await this.store.set(this.prefix + key, e.doc);
          n++;
        } catch (err) {
          e.dirty = true;
          console.error(`store: could not write ${this.prefix}${key}:`, err.message);
        }
      } else if (evict && e.held === 0 && ++e.idle > 8) this.live.delete(key);
    }
    return n;
  }
  get size() {
    return this.live.size;
  }
};

// server/admin.ts
import { randomBytes, timingSafeEqual, createHash } from "node:crypto";

// server/adminPage.ts
var ADMIN_PAGE = String.raw`<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>TinyFleet Dev</title>
<style>
:root{--bg:#f4f6f7;--card:#fff;--ink:#1f2b33;--mute:#76848c;--line:#e2e8eb;--acc:#e2412f;--ok:#1e9e4a;--warn:#c98a12;--chip:#eef2f4}
@media (prefers-color-scheme:dark){:root{--bg:#12171b;--card:#1b2227;--ink:#e6edf0;--mute:#8d9aa2;--line:#2a343a;--chip:#232c32}}
*{box-sizing:border-box}
[hidden]{display:none!important}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
header{display:flex;align-items:center;gap:16px;padding:14px 20px;border-bottom:1px solid var(--line);background:var(--card);position:sticky;top:0;z-index:2;flex-wrap:wrap}
header h1{font-size:17px;margin:0}header h1 span{color:var(--acc)}
.tabs{display:flex;gap:4px}
.tabs button,.btn{border:1px solid var(--line);background:var(--card);color:var(--ink);padding:6px 12px;border-radius:8px;cursor:pointer;font:inherit}
.tabs button.on{background:var(--ink);color:var(--card);border-color:var(--ink)}
.btn.primary{background:var(--acc);border-color:var(--acc);color:#fff}
.grow{flex:1}
main{padding:20px;max-width:1500px;margin:0 auto}
#login{max-width:320px;margin:12vh auto;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:24px;display:flex;flex-direction:column;gap:10px}
#login h2{margin:0 0 6px}
input{font:inherit;padding:8px 10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--ink)}
.err{color:var(--acc);font-size:13px;min-height:1em}
.stats{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 14px;min-width:120px}
.stat b{display:block;font-size:20px}.stat span{color:var(--mute);font-size:12px}
.wrap{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:12px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap;vertical-align:top}
th{position:sticky;top:0;background:var(--card);color:var(--mute);font-weight:600;cursor:pointer;user-select:none;font-size:12px}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
tbody tr.row{cursor:pointer}tbody tr.row:hover{background:var(--chip)}
tr.sel{background:var(--chip)}
.mute{color:var(--mute)}
.chip{display:inline-block;padding:1px 7px;border-radius:999px;background:var(--chip);font-size:12px;margin:1px 2px 1px 0}
.chip.done{background:rgba(30,158,74,.15);color:var(--ok)}.chip.failed{background:rgba(226,65,47,.14);color:var(--acc)}.chip.active{background:rgba(201,138,18,.16);color:var(--warn)}
#detail{margin-top:18px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px;margin-bottom:14px}
.card h3{margin:0 0 10px;font-size:15px}
.kv{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:8px 18px}
.kv div span{display:block;color:var(--mute);font-size:12px}
.mission{border-top:1px solid var(--line);padding:9px 0}.mission:first-child{border-top:0}
.mission ul{margin:4px 0 0 18px;padding:0;color:var(--mute);font-size:12.5px}
.fb{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px 16px;margin-bottom:10px}
.fb h4{margin:0 0 4px;font-size:15px}.fb p{margin:8px 0 0;white-space:pre-wrap}
.fb .meta{color:var(--mute);font-size:12px}
a.link{color:var(--acc);cursor:pointer;text-decoration:underline}
#search{min-width:220px}
@media (max-width:600px){main{padding:12px}header{padding:10px 12px}}
</style></head>
<body>
<div id="login" hidden>
  <h2>TinyFleet <span style="color:var(--acc)">dev</span></h2>
  <input id="name" placeholder="Name" autocomplete="username">
  <input id="pin" placeholder="PIN" type="password" inputmode="numeric" autocomplete="current-password">
  <button class="btn primary" id="go">Sign in</button>
  <div class="err" id="lerr"></div>
</div>
<div id="app" hidden>
  <header>
    <h1>TinyFleet <span>dev</span></h1>
    <div class="tabs"><button id="t-games" class="on">Games</button><button id="t-fb">Feedback</button><button id="t-pc">Postcards</button></div>
    <a class="btn" href="/dev/missions" style="text-decoration:none">Mission text</a>
    <div class="grow"></div>
    <input id="search" placeholder="Search fleet, player, seed…">
    <button class="btn" id="reload">Refresh</button>
    <button class="btn" id="out">Sign out</button>
  </header>
  <main>
    <section id="games">
      <div class="stats" id="stats"></div>
      <div class="wrap"><table><thead id="ghead"></thead><tbody id="gbody"></tbody></table></div>
      <div id="detail"></div>
    </section>
    <section id="feedback" hidden></section>
    <section id="pictures" hidden></section>
  </main>
</div>
<script>
var $ = function (id) { return document.getElementById(id); };
function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
function num(v) { return v == null || v === '' ? '—' : Number(v).toLocaleString('en-US'); }
function money(v) { return v == null ? '—' : (v < 0 ? '-$' : '$') + Math.abs(Math.round(v)).toLocaleString('en-US'); }
function hrs(s) { if (!s) return '0m'; var h = Math.floor(s / 3600), m = Math.round((s % 3600) / 60); return h ? h + 'h ' + m + 'm' : m + 'm'; }
function when(t) { if (!t) return '—'; var d = new Date(t); return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' }) + ' ' + d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' }); }
function ago(t) { var s = (Date.now() - t) / 1000; if (s < 90) return 'just now'; if (s < 5400) return Math.round(s / 60) + ' min ago'; if (s < 129600) return Math.round(s / 3600) + ' h ago'; return Math.round(s / 86400) + ' days ago'; }
function secs(s) { if (s == null) return '—'; if (s < 90) return Math.round(s) + 's'; return hrs(s); }
function fpsTxt(v) { if (v == null) return '<span class="mute">—</span>'; var c = v < 20 ? 'var(--acc)' : v < 40 ? 'var(--warn)' : 'var(--ok)'; return '<b style="color:' + c + '">' + Math.round(v) + '</b>'; }
function cellTxt(c) { return c && c.length === 2 ? c[0] + ', ' + c[1] : '—'; }
function gameHour(h) { if (h == null) return '—'; var d = Math.floor(h / 24) + 1, hh = Math.floor(h % 24), mm = Math.round((h % 1) * 60); return 'day ' + d + ' ' + String(hh).padStart(2, '0') + ':' + String(mm).padStart(2, '0'); }

async function api(path, body) {
  var r = await fetch('/api/admin/' + path, body ? { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {});
  var j = {}; try { j = await r.json(); } catch (e) {}
  j.status = r.status;
  return j;
}

var visits = [];
var games = [], feedback = [], sortKey = 'last', sortDir = -1, openGid = null;
var COLS = [
  ['fleet', 'Fleet'], ['player', 'Player'], ['device', 'Device'], ['fps', 'Avg FPS', 1], ['low1', '1% low', 1], ['quality', 'Quality'], ['welcome', 'Welcome card'], ['firstKey', 'First drive key', 1], ['firstMove', 'First tile', 1], ['errors', 'Errors', 1],
  ['seed', 'Seed'], ['start', 'Start cell'], ['played', 'Played', 1], ['wall', 'Real time', 1], ['day', 'Game day', 1],
  ['worth', 'Net worth', 1], ['earned', '$ earned', 1], ['vehicles', 'Vehicles', 1], ['youTiles', 'Tiles (you)', 1], ['hiredTiles', 'Tiles (hired)', 1],
  ['started', 'Missions', 1], ['done', 'Done', 1], ['delivered', 'Cargo', 1], ['buildings', 'Buildings', 1], ['cells', 'Cells', 1], ['deeds', 'Deeds', 1], ['last', 'Last seen', 1]
];

function showApp(on) { $('login').hidden = on; $('app').hidden = !on; if (!on) setTimeout(function () { $('name').focus(); }, 0); }

async function login() {
  $('lerr').textContent = '';
  var j = await api('login', { name: $('name').value, pin: $('pin').value });
  if (j.ok) { $('pin').value = ''; showApp(true); load(); }
  else $('lerr').textContent = j.why || 'Could not sign in.';
}
$('go').onclick = login;
$('pin').onkeydown = $('name').onkeydown = function (e) { if (e.key === 'Enter') login(); };
$('out').onclick = async function () { await api('logout', {}); showApp(false); };
$('reload').onclick = function () { load(); };
$('search').oninput = function () { renderGames(); renderFeedback(); };
$('t-games').onclick = function () { tab('games'); };
$('t-fb').onclick = function () { tab('fb'); };
$('t-pc').onclick = function () { tab('pc'); loadPictures(); };
function tab(t) {
  $('t-games').classList.toggle('on', t === 'games'); $('t-fb').classList.toggle('on', t === 'fb'); $('t-pc').classList.toggle('on', t === 'pc');
  $('pictures').hidden = t !== 'pc';
  $('games').hidden = t !== 'games'; $('feedback').hidden = t !== 'fb';
}

async function load() {
  var a = await api('games'), b = await api('feedback'), v = await api('visits');
  visits = v.days || [];
  if (a.status === 401 || b.status === 401) { showApp(false); return; }
  games = a.games || []; feedback = b.feedback || [];
  $('t-fb').textContent = 'Feedback (' + feedback.length + ')';
  $('t-games').textContent = 'Games (' + games.length + ')';
  renderGames(); renderFeedback();
  if (openGid) openGame(openGid);
}

/** The postcard funnel (docs/postcards.html), summed over the days kept. */
function funnelTxt() {
  var k = ['card-link', 'card-read', 'card-drawn', 'card-guest', 'card-looked', 'card-car', 'card-delivered', 'card-stayed', 'card-kept'], t = {};
  visits.forEach(function (d) { k.concat(['card-drawms']).forEach(function (x) { t[x] = (t[x] || 0) + ((d.n && d.n[x]) || 0); }); });
  return k.map(function (x) { return x === 'card-drawn' && t[x] ? t[x] + ' (' + (t['card-drawms'] / t[x] / 1000).toFixed(1) + ')' : (t[x] || 0); }).join(' → ');
}

function matches(text) { var q = $('search').value.trim().toLowerCase(); return !q || text.toLowerCase().indexOf(q) >= 0; }

function renderGames() {
  var list = games.filter(function (g) { return matches([g.fleet, g.player, g.seed, g.online, g.gid].join(' ')); });
  list.sort(function (a, b) { var x = a[sortKey], y = b[sortKey]; if (x == null) return 1; if (y == null) return -1; return (typeof x === 'string' ? x.localeCompare(y) : x - y) * sortDir; });
  var tot = { play: 0, earned: 0, you: 0, hired: 0, done: 0, players: {} };
  games.forEach(function (g) { tot.play += g.played || 0; tot.earned += g.earned || 0; tot.you += g.youTiles || 0; tot.hired += g.hiredTiles || 0; tot.done += g.done || 0; tot.players[(g.player || '') + '|' + (g.online || '')] = 1; });
  var week = games.filter(function (g) { return Date.now() - g.last < 7 * 86400000; }).length;
  var vis = { page: 0, mobile: 0, anyway: 0, soft: 0, none: 0, glon: 0 };
  visits.forEach(function (d) { var n = d.n || {}; vis.page += n.page || 0; vis.mobile += n.mobile || 0; vis.anyway += n.anyway || 0; vis.soft += n['gl-soft'] || 0; vis.none += n['gl-none'] || 0; vis.glon += (n['gl-anyway'] || 0) + (n['gl-never'] || 0); });
  var perfGames = games.filter(function (g) { return g.fps != null; });
  var neverDrove = perfGames.filter(function (g) { return !g.youTiles; }).length;
  $('stats').innerHTML = [
    [games.length, 'games'], [Object.keys(tot.players).length, 'player profiles'], [week, 'played this week'], [hrs(tot.play), 'total play'],
    [money(tot.earned), 'earned, all games'], [num(tot.you), 'tiles driven by players'], [num(tot.hired), 'tiles by hired drivers'], [num(tot.done), 'missions completed'], [feedback.length, 'feedback'],
    [num(vis.page), 'page loads (desktop, 30 days)'],
    [funnelTxt(), 'postcards, 30 days: link → read → world drawn (avg s) → guest → looked → car → delivered → 10 min → kept'], [num(vis.mobile), 'phones/tablets turned away'], [num(vis.anyway), 'phones that went on anyway'],
    [num(vis.soft) + ' / ' + num(vis.none), 'software graphics / no WebGL (card shown, 30 days)'], [num(vis.glon), 'played on in software graphics'],
    [perfGames.length ? Math.round(perfGames.reduce(function (s, g) { return s + g.fps; }, 0) / perfGames.length) : '—', 'average FPS (games with perf data)'],
    [neverDrove + ' / ' + perfGames.length, 'never drove a tile (perf-tracked games)']
  ].map(function (s) { return '<div class="stat"><b>' + esc(s[0]) + '</b><span>' + esc(s[1]) + '</span></div>'; }).join('');
  $('ghead').innerHTML = '<tr>' + COLS.map(function (c) { return '<th data-k="' + c[0] + '"' + (c[2] ? ' class="n"' : '') + '>' + esc(c[1]) + (sortKey === c[0] ? (sortDir > 0 ? ' ▲' : ' ▼') : '') + '</th>'; }).join('') + '</tr>';
  Array.prototype.forEach.call($('ghead').querySelectorAll('th'), function (th) {
    th.onclick = function () { var k = th.dataset.k; if (sortKey === k) sortDir = -sortDir; else { sortKey = k; sortDir = typeof (games[0] || {})[k] === 'string' ? 1 : -1; } renderGames(); };
  });
  $('gbody').innerHTML = list.map(function (g) {
    var cells = {
      fleet: '<b>' + esc(g.fleet || '(unnamed)') + '</b>' + (g.online ? ' <span class="chip">online: ' + esc(g.online) + '</span>' : ''),
      player: esc(g.player), seed: '<span class="mute">' + esc(g.seed) + '</span>', start: cellTxt(g.start), played: hrs(g.played), day: num(g.day),
      device: g.device ? '<span class="chip' + (g.device === 'desktop' ? '' : ' failed') + '" title="' + esc(g.gpu || '') + '">' + esc(g.device) + '</span>' : '<span class="mute">—</span>',
      fps: fpsTxt(g.fps), low1: fpsTxt(g.low1), quality: g.quality ? esc(g.quality) : '<span class="mute">—</span>',
      welcome: g.welcome == null ? '<span class="mute">—</span>' : g.welcome === 'open' ? '<span class="chip active">still open</span>' : g.welcome === 'none' ? '<span class="mute">not shown</span>' : esc(Math.round(g.welcome)) + 's',
      firstKey: g.firstKey == null ? (g.fps == null ? '—' : '<span class="chip failed">never</span>') : secs(g.firstKey),
      firstMove: g.firstMove == null ? (g.fps == null ? '—' : '<span class="chip failed">never</span>') : secs(g.firstMove),
      errors: g.errors == null ? '—' : g.errors ? '<span class="chip failed">' + g.errors + '</span>' : '0',
      wall: g.wall == null ? '—' : secs(g.wall),
      worth: money(g.worth), earned: money(g.earned), vehicles: num(g.vehicles), youTiles: num(g.youTiles), hiredTiles: num(g.hiredTiles),
      started: num(g.started), done: num(g.done), delivered: num(g.delivered), buildings: num(g.buildings), cells: num(g.cells), deeds: num(g.deeds),
      last: '<span title="' + esc(when(g.last)) + '">' + esc(ago(g.last)) + '</span>'
    };
    return '<tr class="row' + (g.gid === openGid ? ' sel' : '') + '" data-gid="' + esc(g.gid) + '">' + COLS.map(function (c) { return '<td' + (c[2] ? ' class="n"' : '') + '>' + cells[c[0]] + '</td>'; }).join('') + '</tr>';
  }).join('') || '<tr><td colspan="' + COLS.length + '" class="mute">No games reported yet.</td></tr>';
  Array.prototype.forEach.call($('gbody').querySelectorAll('tr.row'), function (tr) { tr.onclick = function () { openGame(tr.dataset.gid); }; });
}

function kv(pairs) { return '<div class="kv">' + pairs.map(function (p) { return '<div><span>' + esc(p[0]) + '</span>' + p[1] + '</div>'; }).join('') + '</div>'; }
function table(head, rows) {
  if (!rows.length) return '<div class="mute">None.</div>';
  return '<div class="wrap"><table><thead><tr>' + head.map(function (h) { return '<th' + (h[1] ? ' class="n"' : '') + '>' + esc(h[0]) + '</th>'; }).join('') + '</tr></thead><tbody>' +
    rows.map(function (r) { return '<tr>' + r.map(function (c, i) { return '<td' + (head[i][1] ? ' class="n"' : '') + '>' + c + '</td>'; }).join('') + '</tr>'; }).join('') + '</tbody></table></div>';
}

async function openGame(gid) {
  openGid = gid;
  Array.prototype.forEach.call($('gbody').querySelectorAll('tr.row'), function (tr) { tr.classList.toggle('sel', tr.dataset.gid === gid); });
  var j = await api('game?gid=' + encodeURIComponent(gid));
  if (!j.ok) { $('detail').innerHTML = '<div class="card mute">Could not load that game.</div>'; return; }
  var g = j.game, m = g.money || {}, met = g.metrics || {}, dr = g.driven || {}, dl = g.delivered || {}, ex = g.explored || {};
  var missions = Object.keys(g.missions || {}).map(function (k) { return g.missions[k]; }).sort(function (a, b) { return (b.posted || 0) - (a.posted || 0); });
  var done = missions.filter(function (x) { return x.state === 'done'; }).length, failed = missions.filter(function (x) { return x.state === 'failed'; }).length;
  var h = '';
  h += '<div class="card"><h3>' + esc((g.fleet && g.fleet.name) || '(unnamed fleet)') + ' <span class="mute" style="font-weight:400">· ' + esc(g.player && g.player.name) + (g.online ? ' · online as ' + esc(g.online) : '') + '</span>' +
    ' <a class="link" style="float:right;font-weight:400;font-size:13px" onclick="openGid=null;document.getElementById(\'detail\').innerHTML=\'\';renderGames()">close</a></h3>' + kv([
    ['Seed', esc(g.seed) + ' <span class="mute">(' + esc(g.mode) + ')</span>'], ['Starting cell', cellTxt(g.start)], ['Current cell', cellTxt(g.cell)],
    ['Play time', hrs(g.played)], ['Game day', num(g.day) + ' <span class="mute">at ' + esc(g.hour) + 'h</span>'],
    ['First seen', when(g.first)], ['Last seen', when(g.last) + ' <span class="mute">(' + ago(g.last) + ')</span>'], ['Reports', num(g.reports)],
    ['Cash', money(m.cash)], ['$ earned (lifetime)', money(m.earned)], ['$ from hired drivers', money(m.fleetEarned)], ['Tow bills', money(m.towed)],
    ['Net worth', money(met.worth)], ['Vehicle value', money(met.fleetValue)], ['Property value', money(met.estateValue)], ['Takings, 7 days', money(met.week)],
    ['Tiles driven (you)', num(dr.you)], ['Tiles driven (hired drivers)', num(dr.hired)], ['Cargo delivered (you)', num(dl.you)], ['Cargo delivered (fleet)', num(dl.fleet)],
    ['Cells explored', num(ex.cells)], ['Cells visited', num(ex.seen)], ['Drivers', num(g.drivers)], ['HQ', g.hq ? 'grade ' + g.hq.grade + ' · ' + g.hq.slots + ' slots' : '—'],
    ['Towns carried', num(met.towns)], ['Territory', num(met.held)], ['◆ a day', num(met.gems)], ['Missions', missions.length + ' started · ' + done + ' done · ' + failed + ' failed'],
    ['Game id', '<span class="mute">' + esc(g.gid) + '</span>'], ['Build', 'gen ' + esc(g.gen) + ' · save ' + esc(g.save) + ' · ' + esc(g.where)]
  ]) + '</div>';
  h += perfCard(g.perf);
  h += '<div class="card"><h3>Vehicles (' + (g.vehicles || []).length + ')</h3>' + table([['Name'], ['Model'], ['Driver'], ['Where'], ['Tiles', 1], ['Deliveries', 1], ['Earned', 1]],
    (g.vehicles || []).map(function (v) { return [esc(v.name), esc(v.model), v.driver ? esc(v.driver) + ' <span class="mute">' + esc(v.rank) + '</span>' : '<span class="mute">you / none</span>', esc(v.where), num(v.tiles), num(v.deliveries), money(v.earned)]; })) + '</div>';
  h += '<div class="card"><h3>Missions (' + missions.length + ')</h3>' + (missions.length ? missions.map(function (x) {
    return '<div class="mission"><b>' + esc(x.title) + '</b> <span class="chip ' + esc(x.state) + '">' + esc(x.state) + '</span> <span class="chip">' + esc(x.thread) + '</span> <span class="chip">' + esc(x.kind) + '</span>' +
      (x.arc ? ' <span class="chip">arc ' + esc(x.arc) + '</span>' : '') +
      '<div class="mute">' + esc(x.giver) + (x.town ? ' · ' + esc(x.town) : '') + ' · offered ' + gameHour(x.posted) + (x.ended != null ? ' · ended ' + gameHour(x.ended) : '') + (x.outcome ? ' · <b>' + esc(x.outcome) + '</b>' : '') + '</div>' +
      '<div>' + esc(x.why) + '</div>' +
      '<ul>' + (x.objs || []).map(function (o) { return '<li>' + esc(o) + '</li>'; }).join('') + '</ul>' +
      ((x.reward || []).length ? '<div class="mute">Reward: ' + x.reward.map(esc).join(' · ') + '</div>' : '') + '<div class="mute" style="font-size:11px">' + esc(x.tpl) + ' #' + esc(x.id) + '</div></div>';
  }).join('') : '<div class="mute">None.</div>') + '</div>';
  h += '<div class="card"><h3>Buildings bought (' + (g.buildings || []).length + ')</h3>' + table([['Name'], ['Kind'], ['Town'], ['Bought', 1], ['Paid', 1], ['Taken in', 1]],
    (g.buildings || []).map(function (b) { return [esc(b.name), esc(b.kind), esc(b.town), 'day ' + num(b.day), money(b.paid), money(b.took)]; })) + '</div>';
  var TIER = ['', 'Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond'];
  h += '<div class="card"><h3>Achievements (' + (g.deeds || []).length + ')</h3>' + table([['Achievement'], ['Group'], ['Tier'], ['Count', 1]],
    (g.deeds || []).map(function (d) { return [esc(d.name), esc(d.group), esc(TIER[d.tier] || d.tier), num(d.value)]; })) + '</div>';
  if ((j.feedback || []).length) h += '<div class="card"><h3>Feedback from this game</h3>' + j.feedback.map(fbHtml).join('') + '</div>';
  $('detail').innerHTML = h;
  $('detail').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function perfCard(p) {
  if (!p) return '<div class="card"><h3>Performance &amp; device</h3><div class="mute">This game was reported by a build from before performance tracking (4.2).</div></div>';
  var d = p.device || {}, q = p.quality || {}, f = p.frames || {}, u = p.funnel || {}, s = p.settings || {};
  var h = '<div class="card"><h3>Performance &amp; device <span class="mute" style="font-weight:400">· last report: ' + esc(p.why) + '</span></h3>';
  h += kv([
    ['Device', (d.mobile ? '<b style="color:var(--acc)">mobile</b>' : 'desktop') + ' · ' + esc(d.platform) + (d.mobileAnyway ? ' · <b>pressed “continue anyway”</b>' : '') + (d.softwareGl ? ' · <b style="color:var(--acc)">software graphics</b>' : '')],
    ['Input', 'touch points ' + esc(d.touchPoints) + ' · ' + (d.fine ? 'mouse/trackpad' : 'no fine pointer') + (u.gamepad ? ' · gamepad' : '')],
    ['GPU', esc(d.gpu) + ' <span class="mute">' + esc(d.gpuVendor) + '</span>'], ['WebGL', (d.webgl2 ? 'WebGL 2' : 'WebGL 1') + ' · max texture ' + esc(d.maxTexture)],
    ['Screen', esc((d.screen || []).join('×')) + ' @' + esc(d.dpr) + 'x · window ' + esc((d.viewport || []).join('×'))], ['CPU / memory', esc(d.cores) + ' cores · ' + (d.memoryGb ? esc(d.memoryGb) + ' GB' : '? GB')],
    ['Quality', esc(q.label) + ' <span class="mute">(setting: ' + esc(q.setting) + ', ' + esc(q.changes) + ' Auto changes)</span>'],
    ['Time at each level', Object.keys(q.seconds || {}).map(function (k) { return esc(k) + ' ' + secs(q.seconds[k]); }).join(' · ') || '—'],
    ['Average FPS', fpsTxt(f.avgFps) + ' <span class="mute">over ' + secs(f.seconds) + ', ' + num(f.count) + ' frames</span>'],
    ['1% / 5% low', fpsTxt(f.low1) + ' / ' + fpsTxt(f.low5)], ['Worst frame', num(f.worstMs) + ' ms · ' + num(f.over50) + ' over 67 ms · ' + num(f.over250) + ' over 1 s'],
    ['Tab hidden', secs(p.hiddenSeconds)], ['Load to play', p.loadMs == null ? '—' : (p.loadMs / 1000).toFixed(1) + 's'],
    ['Welcome card', u.welcomeSeen ? 'up ' + secs(u.welcomeSeconds) + (u.welcomeClosedAt == null ? ' · <b style="color:var(--acc)">never closed</b>' : ' · closed at ' + secs(u.welcomeClosedAt)) : 'not shown'],
    ['First key / drive key', secs(u.firstKeyAt) + ' / ' + (u.firstDriveKeyAt == null ? '<b style="color:var(--acc)">never</b>' : secs(u.firstDriveKeyAt))],
    ['First tile driven', u.firstMoveAt == null ? '<b style="color:var(--acc)">never</b>' : secs(u.firstMoveAt)],
    ['Keys pressed', Object.keys(u.keys || {}).map(function (k) { return esc(k.replace(/^Key|^Arrow/, '')) + ' ' + u.keys[k]; }).join(' · ') || 'none'],
    ['Clicks / taps', num(u.clicks) + ' / ' + num(u.touches)], ['Menus', num(u.paused) + ' opened · ' + secs(u.menuSeconds) + ' in them'],
    ['Settings', Object.keys(s).map(function (k) { var v = s[k]; return esc(k) + ': ' + esc(typeof v === 'object' ? JSON.stringify(v) : v); }).join(' · ') || 'all default'],
    ['Browser', '<span class="mute" style="white-space:normal">' + esc(d.ua) + '</span>']
  ]);
  if ((f.timeline || []).length) h += '<div style="margin-top:12px"><span class="mute" style="font-size:12px">FPS every 15 s (colour = quality level)</span><div style="display:flex;align-items:flex-end;gap:2px;height:60px;margin-top:4px">' +
    f.timeline.map(function (t) { var col = { high: '#1e9e4a', medium: '#3f7fb8', low: '#c98a12', lowest: '#e2412f' }[t[2]] || '#888'; return '<i title="' + esc(t[0]) + 's · ' + esc(t[1]) + ' fps · ' + esc(t[2]) + '" style="display:block;width:6px;background:' + col + ';height:' + Math.max(2, Math.min(60, t[1])) + 'px"></i>'; }).join('') + '</div></div>';
  if ((f.hist || []).length) h += '<div class="mute" style="font-size:12px;margin-top:8px">Frame times: ' + ['≤12', '≤17', '≤20', '≤25', '≤33', '≤50', '≤67', '≤100', '≤250', '≤1000', '>1000'].map(function (b, i) { return b + 'ms ' + num(f.hist[i]); }).join(' · ') + '</div>';
  if ((p.errors || []).length) h += '<h3 style="margin-top:14px">Errors (' + p.errors.length + ')</h3>' + p.errors.map(function (e) { return '<div class="mission"><b style="color:var(--acc)">' + esc(e.msg) + '</b> <span class="mute">×' + esc(e.n) + ' · first at ' + secs(e.at) + '</span>' + (e.stack ? '<pre style="white-space:pre-wrap;font-size:11px;color:var(--mute);margin:4px 0 0">' + esc(e.stack) + '</pre>' : '') + '</div>'; }).join('');
  return h + '</div>';
}

function fbHtml(f) {
  return '<div class="fb"><h4>' + esc(f.subject) + '</h4><div class="meta">' + esc(when(f.at)) + ' · ' + esc(f.fleet || 'no fleet') + ' · ' + esc(f.player || '?') +
    (f.online ? ' · online as ' + esc(f.online) : '') + (f.seed ? ' · ' + esc(f.seed) + ' cell ' + cellTxt(f.cell) : '') + (f.day ? ' · day ' + esc(f.day) : '') +
    (f.gid ? ' · <a class="link" data-gid="' + esc(f.gid) + '">open game</a>' : '') +
    (f.work ? ' · <a class="link" data-work="' + esc(f.work) + '">clear this name</a>' : '') + '</div><p>' + esc(f.text) + '</p></div>';
}
function renderFeedback() {
  var list = feedback.filter(function (f) { return matches([f.subject, f.text, f.fleet, f.player, f.online].join(' ')); });
  $('feedback').innerHTML = list.map(fbHtml).join('') || '<div class="mute">No feedback yet.</div>';
  Array.prototype.forEach.call($('feedback').querySelectorAll('a[data-gid]'), function (a) { a.onclick = function () { tab('games'); openGame(a.dataset.gid); }; });
  Array.prototype.forEach.call($('feedback').querySelectorAll('a[data-work]'), function (a) {
    a.onclick = async function () { var j = await api('clearname', { k: a.dataset.work }); a.textContent = j.ok ? 'name cleared' : 'already cleared'; a.removeAttribute('data-work'); a.onclick = null; };
  });
}

/** Postcards' pictures (docs/postcards.html): what each link shows as its preview, reported ones first. */
async function loadPictures() {
  var j = await api('pictures');
  var list = j.pictures || [];
  $('t-pc').textContent = 'Postcards (' + list.length + ')';
  $('pictures').innerHTML = list.length ? '<div class="kv" style="grid-template-columns:repeat(auto-fill,minmax(260px,1fr))">' + list.map(function (p) {
    return '<div class="card" style="margin:0"><img src="/p/' + esc(p.id) + '.jpg?v=' + p.at + '" style="width:100%;border-radius:8px;display:block' + (p.hidden ? ';opacity:.35' : '') + '" onerror="this.style.display=\'none\'">'
      + '<h4 style="margin:8px 0 2px">' + esc(p.title) + '</h4><div class="mute">' + esc(p.fleet) + ' · ' + new Date(p.at).toLocaleString() + ' · opened ' + p.opens + ' · ' + p.rides + ' took a car</div>'
      + (p.reports ? '<div><span class="chip failed">reported ' + p.reports + (p.hidden ? ' · hidden' : '') + '</span></div>' : '')
      + '<div style="margin-top:6px">' + (p.hidden ? '<a class="link" data-show="' + esc(p.id) + '">Put it back</a> · ' : '') + '<a class="link" data-drop="' + esc(p.id) + '">Take the picture down</a> · <a class="link" href="/p/' + esc(p.id) + '" target="_blank">open</a></div></div>';
  }).join('') + '</div>' : '<div class="mute">No postcard pictures yet.</div>';
  Array.prototype.forEach.call($('pictures').querySelectorAll('a[data-drop],a[data-show]'), function (a) {
    a.onclick = async function () { await api('picture', { id: a.dataset.drop || a.dataset.show, show: !!a.dataset.show }); loadPictures(); };
  });
}

api('games').then(function (j) { if (j.ok) { showApp(true); load(); } else showApp(false); });
</script>
</body></html>`;

// server/missionsPage.ts
var MISSIONS_PAGE = String.raw`<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Mission Text</title>
<style>
:root{--bg:#f4f6f7;--card:#fff;--ink:#1f2b33;--mute:#76848c;--line:#e2e8eb;--acc:#e2412f;--ok:#1e9e4a;--warn:#b97c0c;--chip:#eef2f4;--edit:#fff8e6;--slot:#e3effa;--slotInk:#1f5f99}
@media (prefers-color-scheme:dark){:root{--bg:#12171b;--card:#1b2227;--ink:#e6edf0;--mute:#8d9aa2;--line:#2a343a;--chip:#232c32;--edit:#2a2618;--slot:#1d2f40;--slotInk:#8cc4f2;--warn:#e0a83a}}
*{box-sizing:border-box}
[hidden]{display:none!important}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
header{display:flex;align-items:center;gap:10px;padding:12px 20px;border-bottom:1px solid var(--line);background:var(--card);position:sticky;top:0;z-index:3;flex-wrap:wrap}
header h1{font-size:17px;margin:0;white-space:nowrap}header h1 a{color:inherit;text-decoration:none}header h1 span{color:var(--acc)}
.btn{border:1px solid var(--line);background:var(--card);color:var(--ink);padding:6px 12px;border-radius:8px;cursor:pointer;font:inherit;text-decoration:none;display:inline-block}
.btn:disabled{opacity:.45;cursor:default}
.btn.primary{background:var(--acc);border-color:var(--acc);color:#fff}
.btn.small{padding:3px 9px;font-size:12.5px}
.grow{flex:1}
input,select,textarea{font:inherit;padding:7px 10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--ink)}
#q{min-width:240px;flex:1;max-width:420px}
label.tog{display:flex;align-items:center;gap:5px;color:var(--mute);font-size:13px;white-space:nowrap;cursor:pointer}
.layout{display:grid;grid-template-columns:250px 1fr;gap:18px;max-width:1500px;margin:0 auto;padding:18px 20px}
nav{position:sticky;top:76px;align-self:start;max-height:calc(100vh - 96px);overflow:auto;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:8px}
nav a{display:flex;justify-content:space-between;gap:8px;padding:6px 9px;border-radius:7px;color:var(--ink);text-decoration:none;font-size:13px;cursor:pointer}
nav a:hover{background:var(--chip)}nav a.on{background:var(--ink);color:var(--card)}
nav a .n{color:var(--mute);font-variant-numeric:tabular-nums}nav a.on .n{color:inherit;opacity:.7}
nav .dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-left:4px;vertical-align:middle}
.stats{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:8px 14px}
.stat b{font-size:18px;margin-right:6px}.stat span{color:var(--mute);font-size:12.5px}
.help{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 16px;margin-bottom:14px;color:var(--mute);font-size:13px}
.help b{color:var(--ink)}
.m{background:var(--card);border:1px solid var(--line);border-radius:12px;margin-bottom:12px;overflow:hidden}
.m>h3{margin:0;padding:10px 16px;font-size:15px;display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;border-bottom:1px solid var(--line)}
.m>h3 .id{font:12px ui-monospace,Consolas,monospace;color:var(--mute);font-weight:400}
.row{display:grid;grid-template-columns:86px 1fr;gap:12px;padding:10px 16px;border-top:1px solid var(--line)}
.row:first-of-type{border-top:0}
.row.edited{background:var(--edit)}
.f{font-size:11.5px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;color:var(--mute);padding-top:3px}
.f small{display:block;font-weight:400;text-transform:none;letter-spacing:0;font:11px ui-monospace,Consolas,monospace;margin-top:2px}
.txt{white-space:pre-wrap;cursor:text;border-radius:6px;padding:2px 4px;margin:-2px -4px}
.txt:hover{background:var(--chip)}
.was{color:var(--mute);text-decoration:line-through;font-size:12.5px;white-space:pre-wrap;margin-top:4px}
.s{display:inline-block;background:var(--slot);color:var(--slotInk);border-radius:5px;padding:0 5px;font:12.5px ui-monospace,Consolas,monospace}
.meta{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:5px;font-size:12px;color:var(--mute)}
.cnt{font-variant-numeric:tabular-nums;font-weight:600}
.cnt.ok{color:var(--ok)}.cnt.near{color:var(--warn)}.cnt.over{color:var(--acc)}
.warn{color:var(--warn)}.bad{color:var(--acc)}
textarea{width:100%;min-height:62px;resize:vertical;line-height:1.45}
.bar{height:4px;border-radius:2px;background:var(--chip);overflow:hidden;width:120px}
.bar i{display:block;height:100%}
.slots{display:flex;gap:5px;flex-wrap:wrap;margin-top:6px;align-items:center}
.slots button{border:0;cursor:pointer}
.warns{margin:6px 0 0;padding-left:18px;font-size:12.5px}
.hist{font-size:12px;margin-top:6px}.hist a{color:var(--acc);cursor:pointer;text-decoration:underline}
#login{max-width:320px;margin:12vh auto;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:24px;display:flex;flex-direction:column;gap:10px}
#login h2{margin:0 0 6px}
.err{color:var(--acc);font-size:13px;min-height:1em}
#toast{position:fixed;bottom:18px;left:50%;transform:translateX(-50%);background:var(--ink);color:var(--card);padding:8px 16px;border-radius:999px;font-size:13px;z-index:9;opacity:0;transition:opacity .2s;pointer-events:none}
#toast.on{opacity:1}
.empty{padding:40px;text-align:center;color:var(--mute)}
@media (max-width:820px){.layout{grid-template-columns:1fr;padding:12px 16px}nav{position:static;max-height:none;display:flex;flex-wrap:wrap;gap:2px}.row{grid-template-columns:1fr;gap:4px}header{padding:10px 16px}#q{min-width:0;max-width:none}}
</style></head>
<body>
<div id="login" hidden>
  <h2>TinyFleet <span style="color:var(--acc)">dev</span></h2>
  <input id="name" placeholder="Name" autocomplete="username">
  <input id="pin" placeholder="PIN" type="password" inputmode="numeric" autocomplete="current-password">
  <button class="btn primary" id="go">Sign in</button>
  <div class="err" id="lerr"></div>
</div>
<div id="app" hidden>
  <header>
    <h1><a href="/dev">TinyFleet <span>dev</span></a> › Mission text</h1>
    <input id="q" placeholder="Search words, mission, id or file…">
    <select id="field"><option value="">All fields</option></select>
    <label class="tog"><input type="checkbox" id="onlyEdited"> Edited</label>
    <label class="tog"><input type="checkbox" id="onlyOver"> Over limit</label>
    <div class="grow"></div>
    <button class="btn" id="export" title="Download every edit as JSON (for npm run missiontext:bake -- --file)">Export</button>
    <a class="btn" href="/dev">Back to dev</a>
  </header>
  <div class="layout">
    <nav id="nav"></nav>
    <main>
      <div class="stats" id="stats"></div>
      <div class="help" id="help">
        Click any line to edit it. <b>Ctrl+Enter</b> saves, <b>Esc</b> cancels. Words in <span class="s">{blue}</span> are slots the game fills in
        (a name, a town, a number): keep them, move them, or drop them, but you can't add new ones. Counts marked ≈ guess about 8 characters a slot.
        A saved edit reaches players the next time they start the game, and <b>npm run missiontext:bake</b> writes it into the source for the next release.
      </div>
      <div id="list"></div>
    </main>
  </div>
</div>
<div id="toast"></div>
<script>
var $ = function (id) { return document.getElementById(id); };
function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
function ago(t) { var s = (Date.now() - t) / 1000; if (s < 90) return 'just now'; if (s < 5400) return Math.round(s / 60) + ' min ago'; if (s < 129600) return Math.round(s / 3600) + ' h ago'; return Math.round(s / 86400) + ' days ago'; }
var SLOT = /\{([\w.$]+)\}/g;
var FIELD_LABEL = { title: 'Title', why: 'Why', details: 'Details', caption: 'Caption', label: 'Objective', line: 'Reward line', effectLine: 'Effect' };
var FIELD_ORDER = ['title', 'why', 'details', 'caption', 'label', 'line', 'effectLine'];

var cat = null, edits = {}, group = '', openKey = null, toastT = 0, showOrphans = false;

async function api(path, body) {
  var r = await fetch('/api/admin/' + path, body ? { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {});
  var j = {}; try { j = await r.json(); } catch (e) {}
  j.status = r.status;
  return j;
}
function toast(s) { var t = $('toast'); t.textContent = s; t.classList.add('on'); clearTimeout(toastT); toastT = setTimeout(function () { t.classList.remove('on'); }, 2200); }
function showApp(on) { $('login').hidden = on; $('app').hidden = !on; if (!on) setTimeout(function () { $('name').focus(); }, 0); }

async function login() {
  $('lerr').textContent = '';
  var j = await api('login', { name: $('name').value, pin: $('pin').value });
  if (j.ok) { $('pin').value = ''; load(); }
  else $('lerr').textContent = j.why || 'Could not sign in.';
}
$('go').onclick = login;
$('pin').onkeydown = $('name').onkeydown = function (e) { if (e.key === 'Enter') login(); };

async function load() {
  var j = await api('missions');
  if (j.status === 401) { showApp(false); return; }
  if (!j.ok) { toast('Could not load the missions'); return; }
  cat = j.catalog; edits = j.edits || {};
  showApp(true);
  var fs = $('field');
  if (fs.options.length < 2) FIELD_ORDER.forEach(function (f) { var o = document.createElement('option'); o.value = f; o.textContent = FIELD_LABEL[f]; fs.appendChild(o); });
  render();
}

// ---- the rules: counts and warnings --------------------------------------------------------------

function slotsOf(s) { var out = [], m; SLOT.lastIndex = 0; while ((m = SLOT.exec(s))) out.push(m[1]); return out; }
/** A slot's likely width: names and places run long, counts short. */
function slotWidth(l) { return /^(n|min|need|secs|top|kmh|d|x\d|i|k|of|hours|tier|count)$/.test(l) ? 4 : /name|town|short|place|to|from|who|what|it|car|model/i.test(l) ? 10 : 8; }
function measure(s) {
  var slots = slotsOf(s), bare = s.replace(SLOT, '');
  var est = bare.length; slots.forEach(function (l) { est += slotWidth(l); });
  return { n: est, approx: slots.length > 0 };
}
function limitOf(e) { return cat.limits[e.field] || 0; }
function problems(e, s) {
  var out = [], lim = limitOf(e), m = measure(s);
  var soft = (cat.soft || []).indexOf(e.field) >= 0;
  if (lim && m.n > lim) out.push([soft || m.approx ? 'warn' : 'bad', (m.approx ? 'About ' : '') + (m.n - lim) + ' character' + (m.n - lim === 1 ? '' : 's') + ' over the ' + (soft ? 'suggested ' : '') + lim + ' limit' + (soft ? ' (the card has room for about that much)' : '')]);
  else if (lim && m.n > lim * 0.9) out.push(['warn', 'Close to the ' + lim + ' character limit']);
  if (e.field === 'title') {
    var w = s.trim().split(/\s+/).length;
    if (w > cat.titleWords) out.push(['bad', 'Titles are ' + cat.titleWords + ' words at most (this is ' + w + ')']);
  }
  if (e.field === 'label' && s.indexOf('→') < 0) {
    var first = s.trim().split(/\s+/)[0];
    if (first && first.charAt(0) !== '{' && cat.verbs.indexOf(first) < 0) out.push(['warn', 'Objectives start with a verb the card knows (' + cat.verbs.slice(0, 8).join(', ') + '…), or use the "A → B" form; "' + first + '" isn’t one']);
  }
  if ((e.field === 'title' || e.field === 'why' || e.field === 'label') && /[()·]/.test(s)) out.push(['warn', 'The card rules keep brackets and middle dots off the front of a card']);
  var had = slotsOf(e.text), now = slotsOf(s);
  var unknown = now.filter(function (l) { return had.indexOf(l) < 0; });
  if (unknown.length) out.push(['bad', 'Unknown slot: ' + unknown.map(function (l) { return '{' + l + '}'; }).join(', ') + ' (can’t be saved)']);
  var dropped = had.filter(function (l) { return now.indexOf(l) < 0; });
  if (dropped.length) out.push(['warn', 'Drops ' + dropped.map(function (l) { return '{' + l + '}'; }).join(', ') + ': the game will no longer say it']);
  if (/[{}]/.test(s.replace(SLOT, ''))) out.push(['bad', 'A stray { or } (can’t be saved)']);
  if (/\s{2,}|^\s|\s$/.test(s) && !/\n/.test(s)) out.push(['warn', 'Extra spaces']);
  return out;
}
function counter(e, s) {
  var lim = limitOf(e), m = measure(s), r = lim ? m.n / lim : 0;
  var cls = !lim ? 'ok' : r > 1 ? 'over' : r > 0.9 ? 'near' : 'ok';
  var col = cls === 'over' ? 'var(--acc)' : cls === 'near' ? 'var(--warn)' : 'var(--ok)';
  return '<span class="cnt ' + cls + '">' + (m.approx ? '≈' : '') + m.n + (lim ? ' / ' + lim : '') + '</span>' +
    (lim ? '<span class="bar"><i style="width:' + Math.min(100, r * 100) + '%;background:' + col + '"></i></span>' : '');
}
function current(e) { return edits[e.key] ? edits[e.key].to : e.text; }
function over(e) { return problems(e, current(e)).some(function (p) { return p[0] === 'bad'; }); }
function slotted(s) { return esc(s).replace(/\{([\w.$]+)\}/g, '<span class="s">{$1}</span>'); }

// ---- the list ---------------------------------------------------------------------------------

function matches(e) {
  if (group && e.group !== group) return false;
  var f = $('field').value; if (f && e.field !== f) return false;
  if ($('onlyEdited').checked && !edits[e.key]) return false;
  if ($('onlyOver').checked && !over(e)) return false;
  var q = $('q').value.trim().toLowerCase();
  if (!q) return true;
  return (current(e) + ' ' + e.text + ' ' + (e.card || '') + ' ' + e.tpl + ' ' + e.file).toLowerCase().indexOf(q) >= 0;
}

function render() {
  if (!cat) return;
  var all = cat.entries, shown = all.filter(matches);
  // the side list: every source with its count, a dot for edits, a red one for lines over a limit
  var nav = '<a data-g="" class="' + (group ? '' : 'on') + '"><span>All missions</span><span class="n">' + all.length + '</span></a>';
  cat.groups.forEach(function (g) {
    var es = all.filter(function (e) { return e.group === g; });
    var ed = es.some(function (e) { return edits[e.key]; }), ov = es.some(over);
    nav += '<a data-g="' + esc(g) + '" class="' + (group === g ? 'on' : '') + '"><span>' + esc(g) +
      (ed ? '<span class="dot" style="background:var(--warn)" title="Has edits"></span>' : '') +
      (ov ? '<span class="dot" style="background:var(--acc)" title="A line is over its limit"></span>' : '') +
      '</span><span class="n">' + es.length + '</span></a>';
  });
  $('nav').innerHTML = nav;
  Array.prototype.forEach.call($('nav').querySelectorAll('a'), function (a) { a.onclick = function () { group = a.dataset.g; openKey = null; render(); window.scrollTo(0, 0); }; });

  var nEdit = Object.keys(edits).length, nOver = all.filter(over).length;
  var orphans = Object.keys(edits).filter(function (k) { return !all.some(function (e) { return e.key === k; }); });
  $('stats').innerHTML =
    '<div class="stat"><b>' + all.length + '</b><span>lines of mission text</span></div>' +
    '<div class="stat"><b>' + nEdit + '</b><span>edited</span></div>' +
    '<div class="stat"><b style="color:' + (nOver ? 'var(--acc)' : 'var(--ok)') + '">' + nOver + '</b><span>breaking a card rule</span></div>' +
    '<div class="stat"><b>' + shown.length + '</b><span>shown</span></div>' +
    (orphans.length ? '<div class="stat" title="Edits whose original line is no longer in this build: already baked into the source, or the code changed"><b class="warn">' + orphans.length + '</b><span>edits not in this build · <a href="#" id="orph" style="color:var(--acc)">' + (showOrphans ? 'hide' : 'show') + '</a></span></div>' : '');
  if ($('orph')) $('orph').onclick = function (ev) { ev.preventDefault(); showOrphans = !showOrphans; render(); };

  // one box per mission: the source file and the template (or table row) it is written in
  var boxes = [], byId = {};
  shown.forEach(function (e) {
    var id = e.file + '|' + e.tpl;
    if (!byId[id]) { byId[id] = { file: e.file, tpl: e.tpl, card: null, line: e.line, rows: [] }; boxes.push(byId[id]); }
    var b = byId[id];
    b.rows.push(e);
    if (!b.card && e.field === 'title') b.card = current(e);
    if (!b.card && e.card) b.card = e.card;
    b.line = Math.min(b.line, e.line);
  });
  boxes.forEach(function (b) { b.rows.sort(function (x, y) { return FIELD_ORDER.indexOf(x.field) - FIELD_ORDER.indexOf(y.field) || x.line - y.line; }); });
  var h = '';
  // edits whose line this build no longer writes: baked into the source already, or the code moved on
  if (showOrphans && orphans.length) {
    h += '<section class="m"><h3>Edits not in this build<span class="id">baked already, or the original line changed in the code: they match nothing, so clearing them is safe</span></h3>';
    orphans.forEach(function (k) {
      var o = edits[k];
      h += '<div class="row edited"><div class="f">' + esc(FIELD_LABEL[o.field] || o.field) + '<small>' + esc(o.tpl) + '</small></div><div>' +
        '<div>' + slotted(o.to) + '</div><div class="was">' + slotted(o.from) + '</div>' +
        '<div class="meta"><span>' + esc(o.file) + '</span><button class="btn small" data-clear="' + k + '">Clear</button></div></div></div>';
    });
    h += '</section>';
  }
  if (!boxes.length) { $('list').innerHTML = h + '<div class="empty">Nothing matches.</div>'; wireClear(); return; }
  boxes.slice(0, 400).forEach(function (b) {
    h += '<section class="m"><h3>' + slotted(b.card || b.tpl) + '<span class="id">' + esc(b.tpl) + ' · ' + esc(b.file.replace('src/game/', '')) + ':' + b.line + '</span></h3>';
    b.rows.forEach(function (e) { h += rowHtml(e); });
    h += '</section>';
  });
  if (boxes.length > 400) h += '<div class="empty">' + (boxes.length - 400) + ' more missions: search or pick a source to narrow it down.</div>';
  $('list').innerHTML = h;
  wireClear();
  wire();
}
function wireClear() {
  Array.prototype.forEach.call($('list').querySelectorAll('[data-clear]'), function (b) {
    b.onclick = async function () {
      var j = await api('missions/edit', { key: b.dataset.clear, to: null });
      if (!j.ok) { toast(j.why || 'Could not clear'); return; }
      delete edits[b.dataset.clear]; toast('Cleared'); render();
    };
  });
}

function rowHtml(e) {
  var ed = edits[e.key], s = current(e), probs = problems(e, s);
  var h = '<div class="row' + (ed ? ' edited' : '') + '" id="r-' + e.key + '" data-k="' + e.key + '">';
  h += '<div class="f">' + esc(FIELD_LABEL[e.field] || e.field) + '<small>line ' + e.lines.join(', ') + '</small></div><div>';
  if (openKey === e.key) {
    h += '<textarea id="ta" spellcheck="true">' + esc(s) + '</textarea>';
    h += '<div class="meta" id="live"></div>';
    if (e.vars.length) {
      h += '<div class="slots"><span class="mute" style="font-size:12px">Slots:</span>' + e.vars.map(function (v) {
        return '<button class="s" data-slot="' + esc(v.label) + '" title="Filled in by: ' + esc(v.expr) + '">{' + esc(v.label) + '}</button>';
      }).join('') + '</div>';
    }
    h += '<div class="meta"><button class="btn primary small" id="save">Save</button><button class="btn small" id="cancel">Cancel</button>' +
      (ed ? '<button class="btn small" id="revert" title="Put the original line back">Revert to original</button>' : '') +
      '<span class="grow"></span><span class="mute">Original: </span></div>' +
      '<div class="was" style="text-decoration:none">' + slotted(e.text) + '</div>';
    if (ed && ed.hist && ed.hist.length) {
      h += '<div class="hist mute">Earlier versions: ' + ed.hist.slice().reverse().map(function (v, i) { return '<a data-h="' + (ed.hist.length - 1 - i) + '" title="' + esc(v.to) + '">' + ago(v.at) + '</a>'; }).join(' · ') + '</div>';
    }
  } else {
    h += '<div class="txt" data-open="1" title="Click to edit">' + slotted(s) + '</div>';
    if (ed) h += '<div class="was">' + slotted(e.text) + '</div>';
    h += '<div class="meta">' + counter(e, s) + (ed ? '<span class="warn">edited ' + ago(ed.at) + '</span>' : '') + '</div>';
    if (probs.length) h += '<ul class="warns">' + probs.map(function (p) { return '<li class="' + p[0] + '">' + esc(p[1]) + '</li>'; }).join('') + '</ul>';
  }
  return h + '</div></div>';
}

function liveCheck(e) {
  var ta = $('ta'); if (!ta) return;
  var s = ta.value, probs = problems(e, s);
  $('live').innerHTML = counter(e, s) + (probs.length ? '' : '<span class="mute">Looks good</span>') +
    (probs.length ? '<ul class="warns" style="width:100%">' + probs.map(function (p) { return '<li class="' + p[0] + '">' + esc(p[1]) + '</li>'; }).join('') + '</ul>' : '');
  var blocked = probs.some(function (p) { return /can’t be saved/.test(p[1]); }) || !s.trim();
  $('save').disabled = blocked;
}

function entryOf(k) { return cat.entries.find(function (e) { return e.key === k; }); }

function wire() {
  Array.prototype.forEach.call($('list').querySelectorAll('[data-open]'), function (el) {
    el.onclick = function () { open(el.closest('.row').dataset.k); };
  });
  if (!openKey) return;
  var e = entryOf(openKey), ta = $('ta');
  if (!e || !ta) return;
  ta.focus(); ta.setSelectionRange(ta.value.length, ta.value.length);
  ta.style.height = Math.max(62, ta.scrollHeight + 4) + 'px';
  ta.oninput = function () { ta.style.height = 'auto'; ta.style.height = Math.max(62, ta.scrollHeight + 4) + 'px'; liveCheck(e); };
  ta.onkeydown = function (ev) {
    if (ev.key === 'Enter' && (ev.ctrlKey || ev.metaKey)) { ev.preventDefault(); if (!$('save').disabled) save(e, ta.value); }
    if (ev.key === 'Escape') { ev.preventDefault(); close(); }
  };
  $('save').onclick = function () { save(e, ta.value); };
  $('cancel').onclick = close;
  if ($('revert')) $('revert').onclick = function () { if (confirm('Put the original line back?')) save(e, null); };
  Array.prototype.forEach.call($('list').querySelectorAll('[data-slot]'), function (b) {
    b.onmousedown = function (ev) { ev.preventDefault(); };
    b.onclick = function () {
      var ins = '{' + b.dataset.slot + '}', a = ta.selectionStart, z = ta.selectionEnd;
      ta.value = ta.value.slice(0, a) + ins + ta.value.slice(z);
      ta.setSelectionRange(a + ins.length, a + ins.length); ta.focus(); liveCheck(e);
    };
  });
  Array.prototype.forEach.call($('list').querySelectorAll('[data-h]'), function (a) {
    a.onclick = function () { ta.value = edits[e.key].hist[Number(a.dataset.h)].to; ta.oninput(); };
  });
  liveCheck(e);
}

function open(k) {
  if (openKey && openKey !== k && $('ta')) {
    var was = entryOf(openKey);
    if (was && $('ta').value !== current(was) && !confirm('Discard your unsaved change to the other line?')) return;
  }
  openKey = k; rerow(k);
}
function close() { var k = openKey; openKey = null; if (k) rerow(k); }
/** Redraw one mission's box only, so the page doesn't jump. */
function rerow(k) {
  Array.prototype.forEach.call($('list').querySelectorAll('.row'), function (r) {
    var e = entryOf(r.dataset.k); if (!e) return;
    if (r.dataset.k === k || r.querySelector('#ta')) { var d = document.createElement('div'); d.innerHTML = rowHtml(e); r.replaceWith(d.firstChild); }
  });
  wire();
}

async function save(e, to) {
  if (to !== null && to.replace(/\r\n?/g, '\n').trim() === e.text) to = null;
  var j = await api('missions/edit', { key: e.key, to: to });
  if (j.status === 401) { showApp(false); return; }
  if (!j.ok) { toast(j.why || 'Could not save'); return; }
  if (j.edit) edits[e.key] = j.edit; else delete edits[e.key];
  toast(j.edit ? 'Saved: players see it from their next game start' : 'Original line restored');
  openKey = null;
  render();
  var r = $('r-' + e.key); if (r) r.scrollIntoView({ block: 'nearest' });
}

var qT = 0;
$('q').oninput = function () { clearTimeout(qT); qT = setTimeout(function () { openKey = null; render(); }, 150); };
$('field').onchange = $('onlyEdited').onchange = $('onlyOver').onchange = function () { openKey = null; render(); };
$('export').onclick = function () {
  var blob = new Blob([JSON.stringify({ at: Date.now(), edits: Object.keys(edits).map(function (k) { return { key: k, from: edits[k].from, to: edits[k].to, file: edits[k].file }; }) }, null, 2)], { type: 'application/json' });
  var a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'mission-text-edits.json'; a.click();
};
window.addEventListener('beforeunload', function (ev) {
  var e = openKey && entryOf(openKey);
  if (e && $('ta') && $('ta').value !== current(e)) { ev.preventDefault(); ev.returnValue = ''; }
});
load();
</script>
</body></html>`;

// shared/missionText.json
var missionText_default = { built: "2026-10-19T20:21:15.915Z", limits: { title: 22, why: 130, label: 28, details: 500, caption: 30, line: 100, effectLine: 40 }, soft: ["caption", "line", "effectLine"], titleWords: 4, verbs: ["Deliver", "Bring", "Upgrade", "Reach", "Visit", "Cross", "Ferry", "Race", "Tow", "Fit", "Own", "Meet", "Arrive", "Take", "Buy", "Keep", "Haul", "Hitch", "Win", "Stamp", "Run", "Drift", "Find", "Light", "Follow", "Film", "Guide", "Spot", "Tag", "Cover", "Search", "Land", "Fly", "Hire", "Tail", "Shadow", "Trail", "Lead"], groups: ["Main missions", "Circuits", "Night ops", "Tails", "Faction oaths", "Faction rises", "Story chains", "Tavern jobs"], entries: [{ key: "e58dc53da5d7", group: "Main missions", file: "src/game/missions/templates.ts", line: 278, lines: [278], tpl: "market", card: "Market Day", field: "title", text: "Market Day", vars: [] }, { key: "89170620a55f", group: "Main missions", file: "src/game/missions/templates.ts", line: 278, lines: [278], tpl: "market", card: "Market Day", field: "why", text: "The market stalls are bare. Run two loads of fresh food into the town\u2019s shops and the town will know your name.", vars: [] }, { key: "f1619d096437", group: "Main missions", file: "src/game/missions/templates.ts", line: 278, lines: [278, 286, 288, 292], tpl: "market", card: "Market Day", field: "effectLine", text: "A week of trade", vars: [] }, { key: "bb6b0a6f811f", group: "Main missions", file: "src/game/missions/templates.ts", line: 280, lines: [280], tpl: "school", card: "The School Run", field: "title", text: "The School Run", vars: [] }, { key: "d7d529ad38b6", group: "Main missions", file: "src/game/missions/templates.ts", line: 280, lines: [280], tpl: "school", card: "The School Run", field: "why", text: "Get the students to class, then bring in what the school is waiting on. Do both and the town owes you one.", vars: [] }, { key: "af48f5ff4922", group: "Main missions", file: "src/game/missions/templates.ts", line: 280, lines: [280, 284], tpl: "school", card: "The School Run", field: "effectLine", text: "The town builds", vars: [] }, { key: "8682dc8766ee", group: "Main missions", file: "src/game/missions/templates.ts", line: 282, lines: [282], tpl: "reopen", card: "Reopening", field: "title", text: "Reopening", vars: [] }, { key: "cb08eb9ddc12", group: "Main missions", file: "src/game/missions/templates.ts", line: 282, lines: [282], tpl: "reopen", card: "Reopening", field: "why", text: "A boarded-up shop could trade again. Bring in the materials to fix it, then stock it, and the town will trust you.", vars: [] }, { key: "d5fd41f5932a", group: "Main missions", file: "src/game/missions/templates.ts", line: 282, lines: [282], tpl: "reopen", card: "Reopening", field: "effectLine", text: "A shop reopens", vars: [] }, { key: "2528b8a146df", group: "Main missions", file: "src/game/missions/templates.ts", line: 284, lines: [284], tpl: "ward", card: "The New Ward", field: "title", text: "The New Ward", vars: [] }, { key: "49a5eede36bb", group: "Main missions", file: "src/game/missions/templates.ts", line: 284, lines: [284], tpl: "ward", card: "The New Ward", field: "why", text: "The clinic is struggling. Bring in the patients, then the supplies to treat them, and the town will trust you.", vars: [] }, { key: "11ed7fb28790", group: "Main missions", file: "src/game/missions/templates.ts", line: 286, lines: [286], tpl: "harbour", card: "Harbour Turnaround", field: "title", text: "Harbour Turnaround", vars: [] }, { key: "af4c9b62c8b1", group: "Main missions", file: "src/game/missions/templates.ts", line: 286, lines: [286], tpl: "harbour", card: "Harbour Turnaround", field: "why", text: "A ship is waiting to sail. Clear its cargo off the quay, then run goods into town, and the port won\u2019t forget it.", vars: [] }, { key: "84351d91be8d", group: "Main missions", file: "src/game/missions/templates.ts", line: 288, lines: [288], tpl: "twoveh", card: "Sea and Road", field: "title", text: "Sea and Road", vars: [] }, { key: "54936688c90d", group: "Main missions", file: "src/game/missions/templates.ts", line: 288, lines: [288], tpl: "twoveh", card: "Sea and Road", field: "why", text: "Fetch a load off the boat at the harbour, then run supplies into town. Two runs, and the town\u2019s trust follows.", vars: [] }, { key: "c16b352f77d4", group: "Main missions", file: "src/game/missions/templates.ts", line: 290, lines: [290], tpl: "bypass", card: "The Bypass", field: "title", text: "The Bypass", vars: [] }, { key: "ef701298cd53", group: "Main missions", file: "src/game/missions/templates.ts", line: 290, lines: [290], tpl: "bypass", card: "The Bypass", field: "why", text: "The town wants a bypass. Haul in the materials for it, then goods for the works, and its levy on you drops.", vars: [] }, { key: "ea16b7fe7174", group: "Main missions", file: "src/game/missions/templates.ts", line: 290, lines: [290], tpl: "bypass", card: "The Bypass", field: "effectLine", text: "Levy down 4%", vars: [] }, { key: "919404764b85", group: "Main missions", file: "src/game/missions/templates.ts", line: 292, lines: [292], tpl: "highcountry", card: "High Country", field: "title", text: "High Country", vars: [] }, { key: "61d9a5b1f31a", group: "Main missions", file: "src/game/missions/templates.ts", line: 292, lines: [292], tpl: "highcountry", card: "High Country", field: "why", text: "The hut above the trees needs building supplies, walkers to fill it and food. Do all three and the town calls you friend.", vars: [] }, { key: "4a761455987e", group: "Main missions", file: "src/game/missions/templates.ts", line: 294, lines: [294], tpl: "graduation", card: "The Graduation", field: "title", text: "The Graduation", vars: [] }, { key: "9463b6aa2738", group: "Main missions", file: "src/game/missions/templates.ts", line: 294, lines: [294], tpl: "graduation", card: "The Graduation", field: "why", text: "Haul in materials, workers and trade goods and the town grows into a city. It will call you friend for it.", vars: [] }, { key: "a4d028881ac9", group: "Main missions", file: "src/game/missions/templates.ts", line: 294, lines: [294], tpl: "graduation", card: "The Graduation", field: "effectLine", text: "The town graduates", vars: [] }, { key: "df9b506e64f0", group: "Main missions", file: "src/game/missions/templates.ts", line: 358, lines: [358], tpl: "t:freedom", card: "Freedom of the Town", field: "title", text: "Freedom of the Town", vars: [] }, { key: "1644ce6e8ca6", group: "Main missions", file: "src/game/missions/templates.ts", line: 358, lines: [358], tpl: "t:freedom", card: "Freedom of the Town", field: "why", text: "The council wants to thank you for all your work. Drive to the town hall for a building, and the whole town will hear your name.", vars: [] }, { key: "38861a7d85e6", group: "Main missions", file: "src/game/missions/templates.ts", line: 360, lines: [360, 890], tpl: "t:freedom", card: "Freedom of the Town", field: "caption", text: "The {x1}", vars: [{ label: "x1", expr: "shortSite(gift.kind).toLowerCase()" }] }, { key: "54bea549eb59", group: "Main missions", file: "src/game/missions/templates.ts", line: 361, lines: [361], tpl: "t:freedom", card: "Freedom of the Town", field: "label", text: "Reach the town hall", vars: [] }, { key: "e71b95c76427", group: "Main missions", file: "src/game/missions/templates.ts", line: 420, lines: [420], tpl: "first:parts", card: "First Delivery", field: "title", text: "First Delivery", vars: [] }, { key: "29298d2cdc99", group: "Main missions", file: "src/game/missions/templates.ts", line: 420, lines: [420], tpl: "first:parts", card: "First Delivery", field: "why", text: "Your friend {name} at the Auto Shop needs car parts that fell off a van. Pick them up on the road ahead and drive them in.", vars: [{ label: "name", expr: "name" }] }, { key: "cef3f7a3438d", group: "Main missions", file: "src/game/missions/templates.ts", line: 421, lines: [421], tpl: "first:parts", card: "First Delivery", field: "details", text: "Your friend {name} runs the Auto Shop in town and has been waiting on these parts all week. Deliver them and {name} will fit a nitrous bottle to your car for free.", vars: [{ label: "name", expr: "name" }] }, { key: "406ace539fc8", group: "Main missions", file: "src/game/missions/templates.ts", line: 423, lines: [423], tpl: "first:parts", card: "First Delivery", field: "caption", text: "Nitrous, fitted free", vars: [] }, { key: "df7ed3a54e86", group: "Main missions", file: "src/game/missions/templates.ts", line: 425, lines: [425], tpl: "first:parts", card: "First Delivery", field: "label", text: "Car Parts \u2192 the Auto Shop", vars: [] }, { key: "1cfacff372ad", group: "Main missions", file: "src/game/missions/templates.ts", line: 501, lines: [501], tpl: "first:nitrous", card: "Feel the Rush", field: "title", text: "Feel the Rush", vars: [] }, { key: "1e026e8c9793", group: "Main missions", file: "src/game/missions/templates.ts", line: 501, lines: [501], tpl: "first:nitrous", card: "Feel the Rush", field: "why", text: "Your friend {mech} bets you $100 you can\u2019t hit {x1}. Find a long straight, hold W, then press Shift for nitrous.", vars: [{ label: "mech", expr: "f.mech" }, { label: "x1", expr: "speedText(kmh)" }] }, { key: "04dc5d040265", group: "Main missions", file: "src/game/missions/templates.ts", line: 502, lines: [502], tpl: "first:nitrous", card: "Feel the Rush", field: "details", text: "Your friend {mech} just fitted your nitrous bottle and wants to see it work. Your car can\u2019t reach {x1} on its own. A bottle gives a short burst of extra speed, fires only while you hold the throttle, and refills in about a minute.", vars: [{ label: "mech", expr: "f.mech" }, { label: "x1", expr: "speedText(kmh)" }] }, { key: "c036f75c973a", group: "Main missions", file: "src/game/missions/templates.ts", line: 504, lines: [504], tpl: "first:nitrous", card: "Feel the Rush", field: "caption", text: "Your new nitrous", vars: [] }, { key: "8191cdeeda84", group: "Main missions", file: "src/game/missions/templates.ts", line: 505, lines: [505, 986], tpl: "first:nitrous", card: "Feel the Rush", field: "label", text: "Reach {x1}", vars: [{ label: "x1", expr: "speedText(kmh)" }] }, { key: "8f21bca6e84d", group: "Main missions", file: "src/game/missions/templates.ts", line: 522, lines: [522], tpl: "first:friend", card: null, field: "label", text: "Reach {fr}\u2019s Auto Shop", vars: [{ label: "fr", expr: "fr" }] }, { key: "6bf0aad54701", group: "Main missions", file: "src/game/missions/templates.ts", line: 524, lines: [524], tpl: "first:friend", card: "A Friend\u2019s Handbrake", field: "title", text: "A Friend\u2019s Handbrake", vars: [] }, { key: "46b00f11f675", group: "Main missions", file: "src/game/missions/templates.ts", line: 528, lines: [528], tpl: "first:friend", card: "A Friend\u2019s Handbrake", field: "why", text: "{fr} has a spare handbrake for you. Drive over for a free fit.", vars: [{ label: "fr", expr: "fr" }] }, { key: "051137a4d47b", group: "Main missions", file: "src/game/missions/templates.ts", line: 529, lines: [529], tpl: "first:friend", card: "A Friend\u2019s Handbrake", field: "details", text: "{fr}, a friend of your friend {mech}, has a shop about {x1} tiles away. Follow the yellow arrows, stop in the ring at the shop's door and press Space. A handbrake locks the back wheels when you hold Space, so the tail swings round corners.", vars: [{ label: "fr", expr: "fr" }, { label: "mech", expr: "f.mech" }, { label: "x1", expr: "Math.round(q.far / 5) * 5" }] }, { key: "aa75dc5d2608", group: "Main missions", file: "src/game/missions/templates.ts", line: 531, lines: [531], tpl: "first:friend", card: "A Friend\u2019s Handbrake", field: "caption", text: "A spare handbrake", vars: [] }, { key: "3eb2dd8968ce", group: "Main missions", file: "src/game/missions/templates.ts", line: 532, lines: [532], tpl: "first:friend", card: "A Friend\u2019s Handbrake", field: "label", text: "Reach the Auto Shop", vars: [] }, { key: "f2b96e620881", group: "Main missions", file: "src/game/missions/templates.ts", line: 557, lines: [557], tpl: "first:drift", card: "Drift the Block", field: "title", text: "Drift the Block", vars: [] }, { key: "efead2ee682f", group: "Main missions", file: "src/game/missions/templates.ts", line: 557, lines: [557], tpl: "first:drift", card: "Drift the Block", field: "why", text: "{who} pays $200 to see that handbrake work. Follow the arrows and drift round one left corner, then one right.", vars: [{ label: "who", expr: "who" }] }, { key: "f86cb5d2afac", group: "Main missions", file: "src/game/missions/templates.ts", line: 558, lines: [558], tpl: "first:drift", card: "Drift the Block", field: "details", text: "Build up speed toward the corner, start turning, then hold Space: the back wheels lock and the tail slides out. Let go of Space as the car comes round and drive on. {who} is watching from the shop.", vars: [{ label: "who", expr: "who" }] }, { key: "14c21021215d", group: "Main missions", file: "src/game/missions/templates.ts", line: 560, lines: [560], tpl: "first:drift", card: "Drift the Block", field: "caption", text: "Your new handbrake", vars: [] }, { key: "6803cb348eb6", group: "Main missions", file: "src/game/missions/templates.ts", line: 562, lines: [562], tpl: "first:drift", card: "Drift the Block", field: "label", text: "Drift a left corner", vars: [] }, { key: "f8baddfb5d88", group: "Main missions", file: "src/game/missions/templates.ts", line: 563, lines: [563], tpl: "first:drift", card: "Drift the Block", field: "label", text: "Drift a right corner", vars: [] }, { key: "4ae2e2ffe954", group: "Main missions", file: "src/game/missions/templates.ts", line: 581, lines: [581], tpl: "fast:go", card: "Needs More Go", field: "title", text: "Needs More Go", vars: [] }, { key: "041e69c54e27", group: "Main missions", file: "src/game/missions/templates.ts", line: 581, lines: [581], tpl: "fast:go", card: "Needs More Go", field: "why", text: "Fit any engine upgrade from a Vehicle Shop that stocks engines; {mech} refunds {x1} of it. Then Vera wants a race.", vars: [{ label: "mech", expr: "mech" }, { label: "x1", expr: "money(refund)" }] }, { key: "39ef49d36fe5", group: "Main missions", file: "src/game/missions/templates.ts", line: 582, lines: [582], tpl: "fast:go", card: "Needs More Go", field: "details", text: "Vera Quill, the fastest driver around, won\u2019t race a car this slow. {mech} the mechanic thinks a Stage 1 engine would change her mind, and will cover more than half of what it costs.", vars: [{ label: "mech", expr: "mech" }] }, { key: "4d2f9738d300", group: "Main missions", file: "src/game/missions/templates.ts", line: 584, lines: [584], tpl: "fast:go", card: "Needs More Go", field: "caption", text: "Stage 1 engine", vars: [] }, { key: "5a0be402b4a0", group: "Main missions", file: "src/game/missions/templates.ts", line: 585, lines: [585], tpl: "fast:go", card: "Needs More Go", field: "label", text: "Fit any engine part", vars: [] }, { key: "4ac54a3bcd21", group: "Main missions", file: "src/game/missions/templates.ts", line: 603, lines: [603], tpl: "fast:turbo", card: "{name}\u2019s Turbo", field: "title", text: "{name}\u2019s Turbo", vars: [{ label: "name", expr: "name" }] }, { key: "0392cc49b616", group: "Main missions", file: "src/game/missions/templates.ts", line: 603, lines: [603], tpl: "fast:turbo", card: "{name}\u2019s Turbo", field: "title", text: "The Turbo", vars: [] }, { key: "de872afe2c72", group: "Main missions", file: "src/game/missions/templates.ts", line: 604, lines: [604], tpl: "fast:turbo", card: "{name}\u2019s Turbo", field: "why", text: "Make 3 {x1} runs in 48 hours to keep the garage going, and {name} fits a Stage 2 turbo engine free.", vars: [{ label: "x1", expr: "p.type.name.toLowerCase()" }, { label: "name", expr: "name" }] }, { key: "28a069746f6d", group: "Main missions", file: "src/game/missions/templates.ts", line: 605, lines: [605], tpl: "fast:turbo", card: "{name}\u2019s Turbo", field: "why", text: "Make 3 runs in 48 hours to keep the garage going, and {name} fits a Stage 2 turbo engine free.", vars: [{ label: "name", expr: "name" }] }, { key: "0207bc884df3", group: "Main missions", file: "src/game/missions/templates.ts", line: 607, lines: [607], tpl: "fast:turbo", card: "{name}\u2019s Turbo", field: "caption", text: "Stage 2 engine", vars: [] }, { key: "0039da8732c8", group: "Main missions", file: "src/game/missions/templates.ts", line: 635, lines: [635], tpl: "raceTemplate", card: null, field: "label", text: "Race to {name}", vars: [{ label: "name", expr: "to.name" }] }, { key: "5aaff2df5b85", group: "Main missions", file: "src/game/missions/templates.ts", line: 639, lines: [639], tpl: "raceTemplate", card: null, field: "caption", text: "{short}\u2019s {x1}", vars: [{ label: "short", expr: "r.short" }, { label: "x1", expr: "modelName(rivalModel(r, tier))" }] }, { key: "84774d44f33d", group: "Main missions", file: "src/game/missions/templates.ts", line: 641, lines: [641], tpl: "raceTemplate", card: null, field: "label", text: "Meet {short} at the square", vars: [{ label: "short", expr: "r.short" }] }, { key: "763e97e0cb0c", group: "Main missions", file: "src/game/missions/templates.ts", line: 642, lines: [642], tpl: "raceTemplate", card: null, field: "label", text: "Race to the finish", vars: [] }, { key: "4266bb7b42dd", group: "Main missions", file: "src/game/missions/templates.ts", line: 651, lines: [651], tpl: "veraOne", card: "Vera\u2019s Challenge", field: "title", text: "Vera\u2019s Challenge", vars: [] }, { key: "58379de6f628", group: "Main missions", file: "src/game/missions/templates.ts", line: 655, lines: [655], tpl: "veraTwo", card: "Pink Slip", field: "title", text: "Pink Slip", vars: [] }, { key: "e64fc4f77c95", group: "Main missions", file: "src/game/missions/templates.ts", line: 659, lines: [659], tpl: "ottoRace", card: "Otto\u2019s Shortcut", field: "title", text: "Otto\u2019s Shortcut", vars: [] }, { key: "50a614d25440", group: "Main missions", file: "src/game/missions/templates.ts", line: 663, lines: [663], tpl: "magsRace", card: "Mags\u2019 Streets", field: "title", text: "Mags\u2019 Streets", vars: [] }, { key: "718c4b763d4a", group: "Main missions", file: "src/game/missions/templates.ts", line: 667, lines: [667], tpl: "dukeRace", card: "The Duke\u2019s Slip", field: "title", text: "The Duke\u2019s Slip", vars: [] }, { key: "b4855fdb92df", group: "Main missions", file: "src/game/missions/templates.ts", line: 692, lines: [692], tpl: "heavy:van", card: "Spare Wheels", field: "title", text: "Spare Wheels", vars: [] }, { key: "88454011ee76", group: "Main missions", file: "src/game/missions/templates.ts", line: 692, lines: [692], tpl: "heavy:van", card: "Spare Wheels", field: "why", text: "Old Bram is retiring his {x1}. Run 4 loads to or from the town within 72 hours and it joins your fleet.", vars: [{ label: "x1", expr: "modelName(model)" }] }, { key: "d893a8d9107f", group: "Main missions", file: "src/game/missions/templates.ts", line: 694, lines: [694], tpl: "heavy:van", card: "Spare Wheels", field: "label", text: "Run 4 loads for the town", vars: [] }, { key: "9ee62e3259e8", group: "Main missions", file: "src/game/missions/templates.ts", line: 714, lines: [714], tpl: "first:hq", card: "Upgrade Your HQ", field: "title", text: "Upgrade Your HQ", vars: [] }, { key: "8b727c0af6c4", group: "Main missions", file: "src/game/missions/templates.ts", line: 715, lines: [715], tpl: "first:hq", card: "Upgrade Your HQ", field: "why", text: "Your house is a one-slot HQ, and your driver fills it. Move your HQ to a level 2 building, a shop or a store, for a second slot.", vars: [] }, { key: "1dfea466e074", group: "Main missions", file: "src/game/missions/templates.ts", line: 716, lines: [716], tpl: "first:hq", card: "Upgrade Your HQ", field: "details", text: "A house is a level 1 Lodging: one slot. A level 2 Shopfront, such as a shop, a store, a diner or a barn, opens two and costs from about $1,700. Your house stays yours.", vars: [] }, { key: "73d7b089a0a8", group: "Main missions", file: "src/game/missions/templates.ts", line: 717, lines: [717], tpl: "first:hq", card: "Upgrade Your HQ", field: "caption", text: "A level 2 HQ", vars: [] }, { key: "aacd4139069d", group: "Main missions", file: "src/game/missions/templates.ts", line: 718, lines: [718], tpl: "first:hq", card: "Upgrade Your HQ", field: "label", text: "Upgrade to a 2-slot HQ", vars: [] }, { key: "fefc0fbf9448", group: "Main missions", file: "src/game/missions/templates.ts", line: 732, lines: [732], tpl: "first:car", card: "Second Pair of Wheels", field: "title", text: "Second Pair of Wheels", vars: [] }, { key: "5716745f4ff6", group: "Main missions", file: "src/game/missions/templates.ts", line: 733, lines: [733], tpl: "first:car", card: "Second Pair of Wheels", field: "why", text: "One car is a job; two is a fleet. Buy a second vehicle at a dealer, or win one: Bram gives a Mule for four loads for a town.", vars: [] }, { key: "81031dc892f7", group: "Main missions", file: "src/game/missions/templates.ts", line: 734, lines: [734], tpl: "first:car", card: "Second Pair of Wheels", field: "details", text: "A Tomato costs about $300 at a Vehicle Dealer: drive to its door, stop in the ring and press Space. Your own car parks where you bought the new one. Or take Bram\u2019s \u201CSpare Wheels\u201D job when it comes up, and the Mule is yours free.", vars: [] }, { key: "609c673b4d47", group: "Main missions", file: "src/game/missions/templates.ts", line: 735, lines: [735], tpl: "first:car", card: "Second Pair of Wheels", field: "caption", text: "Vehicle number two", vars: [] }, { key: "05f76df39bd5", group: "Main missions", file: "src/game/missions/templates.ts", line: 736, lines: [736, 769], tpl: "first:car", card: "Second Pair of Wheels", field: "label", text: "Own 2 vehicles", vars: [] }, { key: "001872407124", group: "Main missions", file: "src/game/missions/templates.ts", line: 750, lines: [750], tpl: "first:crew", card: "Hire a Hand", field: "title", text: "Hire a Hand", vars: [] }, { key: "bccfdd535566", group: "Main missions", file: "src/game/missions/templates.ts", line: 751, lines: [751], tpl: "first:crew", card: "Hire a Hand", field: "why", text: "A parked car earns nothing. Hire a driver for your second vehicle: your house\u2019s one fleet slot is theirs, and they pay you a cut.", vars: [] }, { key: "4655fb3496fb", group: "Main missions", file: "src/game/missions/templates.ts", line: 752, lines: [752], tpl: "first:crew", card: "Hire a Hand", field: "details", text: "Press H and open Fleet. A rookie with a road licence costs about $300: hire one onto the bench with Hire\u2026, then drag them onto your parked vehicle\u2019s empty seat and they set off to work at once. Hover their square by your speedo to see how they are doing.", vars: [] }, { key: "287800b2cfd4", group: "Main missions", file: "src/game/missions/templates.ts", line: 753, lines: [753], tpl: "first:crew", card: "Hire a Hand", field: "caption", text: "Your first driver", vars: [] }, { key: "a4a3df2f353d", group: "Main missions", file: "src/game/missions/templates.ts", line: 754, lines: [754], tpl: "first:crew", card: "Hire a Hand", field: "label", text: "Hire a driver for it", vars: [] }, { key: "e89084044439", group: "Main missions", file: "src/game/missions/templates.ts", line: 767, lines: [767], tpl: "heavy:hand", card: "A Spare Hand", field: "title", text: "A Spare Hand", vars: [] }, { key: "906d140e8116", group: "Main missions", file: "src/game/missions/templates.ts", line: 767, lines: [767], tpl: "heavy:hand", card: "A Spare Hand", field: "why", text: "Bram knows a driver who needs work. Own two vehicles, then meet Bram at his yard, and the driver joins you free.", vars: [] }, { key: "300f18cb511f", group: "Main missions", file: "src/game/missions/templates.ts", line: 768, lines: [768], tpl: "heavy:hand", card: "A Spare Hand", field: "caption", text: "A driver for it", vars: [] }, { key: "0caecb2480f0", group: "Main missions", file: "src/game/missions/templates.ts", line: 769, lines: [769], tpl: "heavy:hand", card: "A Spare Hand", field: "label", text: "Meet Bram at the yard", vars: [] }, { key: "c987d5894afb", group: "Main missions", file: "src/game/missions/templates.ts", line: 805, lines: [805], tpl: "towTemplate", card: null, field: "label", text: "Bring {min} vehicles", vars: [{ label: "min", expr: "min" }] }, { key: "80a86b5b684a", group: "Main missions", file: "src/game/missions/templates.ts", line: 806, lines: [806], tpl: "towTemplate", card: null, field: "label", text: "Tow it to the {name}", vars: [{ label: "name", expr: "route.to.name" }] }, { key: "6868394c773a", group: "Main missions", file: "src/game/missions/templates.ts", line: 806, lines: [806], tpl: "towTemplate", card: null, field: "label", text: "Tow it home", vars: [] }, { key: "4872c2a5d9ee", group: "Main missions", file: "src/game/missions/templates.ts", line: 827, lines: [827], tpl: "pullingTogether", card: "Pulling Together", field: "title", text: "Pulling Together", vars: [] }, { key: "034b4ad95a0d", group: "Main missions", file: "src/game/missions/templates.ts", line: 827, lines: [827], tpl: "pullingTogether", card: "Pulling Together", field: "why", text: "Learn to tow on an old wreck that won\u2019t mind a scrape. Bring two vehicles, hitch up with Enter and drag it away.", vars: [] }, { key: "632e69855085", group: "Main missions", file: "src/game/missions/templates.ts", line: 827, lines: [827], tpl: "pullingTogether", card: "Pulling Together", field: "caption", text: "An old wreck", vars: [] }, { key: "1ab623de2446", group: "Main missions", file: "src/game/missions/templates.ts", line: 834, lines: [834], tpl: "twoRopes", card: "Two Ropes", field: "title", text: "Two Ropes", vars: [] }, { key: "c09d1e69d6d8", group: "Main missions", file: "src/game/missions/templates.ts", line: 834, lines: [834], tpl: "twoRopes", card: "Two Ropes", field: "why", text: "The new church bell is too heavy for one car. Tow it in with two vehicles on ropes and the town grows round it.", vars: [] }, { key: "abc768d1671e", group: "Main missions", file: "src/game/missions/templates.ts", line: 834, lines: [834], tpl: "twoRopes", card: "Two Ropes", field: "caption", text: "The church bell", vars: [] }, { key: "dd84b2207f33", group: "Main missions", file: "src/game/missions/templates.ts", line: 837, lines: [837], tpl: "twoRopes[2]", card: "Two Ropes", field: "line", text: "The town builds", vars: [] }, { key: "cb6031213689", group: "Main missions", file: "src/game/missions/templates.ts", line: 841, lines: [841], tpl: "theBoiler", card: "The Boiler", field: "title", text: "The Boiler", vars: [] }, { key: "51cdbc505fc9", group: "Main missions", file: "src/game/missions/templates.ts", line: 841, lines: [841], tpl: "theBoiler", card: "The Boiler", field: "why", text: "Bram\u2019s last job: a mill boiler that needs three vehicles on the ropes to drag it up to the works.", vars: [] }, { key: "b3cfc7d27d82", group: "Main missions", file: "src/game/missions/templates.ts", line: 841, lines: [841], tpl: "theBoiler", card: "The Boiler", field: "caption", text: "A mill boiler", vars: [] }, { key: "aba9a5d6f56c", group: "Main missions", file: "src/game/missions/templates.ts", line: 849, lines: [849], tpl: "yachtTow", card: "High and Dry", field: "title", text: "High and Dry", vars: [] }, { key: "605a407e9e43", group: "Main missions", file: "src/game/missions/templates.ts", line: 849, lines: [849], tpl: "yachtTow", card: "High and Dry", field: "why", text: "A storm left a yacht high and dry in a field. Tow it back to the harbour with two vehicles and its owner pays well.", vars: [] }, { key: "97cc64e22555", group: "Main missions", file: "src/game/missions/templates.ts", line: 849, lines: [849], tpl: "yachtTow", card: "High and Dry", field: "caption", text: "A stranded yacht", vars: [] }, { key: "3d33e18f602a", group: "Main missions", file: "src/game/missions/templates.ts", line: 855, lines: [855], tpl: "statueTow", card: "The Statue", field: "title", text: "The Statue", vars: [] }, { key: "5d22d01a2f3d", group: "Main missions", file: "src/game/missions/templates.ts", line: 855, lines: [855], tpl: "statueTow", card: "The Statue", field: "why", text: "The quarry has finished the town\u2019s new statue. Tow it into town with two vehicles and the town will thank you.", vars: [] }, { key: "ccd6f853cd75", group: "Main missions", file: "src/game/missions/templates.ts", line: 855, lines: [855], tpl: "statueTow", card: "The Statue", field: "caption", text: "A stone statue", vars: [] }, { key: "a32263e01e44", group: "Main missions", file: "src/game/missions/templates.ts", line: 885, lines: [885, 1120], tpl: "keys:bakery", card: "The Keys", field: "title", text: "The Keys", vars: [] }, { key: "3111a9ff8125", group: "Main missions", file: "src/game/missions/templates.ts", line: 886, lines: [886], tpl: "keys:bakery", card: "The Keys", field: "why", text: "Keep {who}\u2019s {x1} stocked with 3 deliveries in 72 hours and {who} hands you the keys to the building.", vars: [{ label: "who", expr: "who" }, { label: "x1", expr: "place.toLowerCase()" }] }, { key: "5cbaa205e7cb", group: "Main missions", file: "src/game/missions/templates.ts", line: 887, lines: [887], tpl: "keys:bakery", card: "The Keys", field: "why", text: "Keep the shop stocked with 3 deliveries in 72 hours and {who} hands you the keys.", vars: [{ label: "who", expr: "who" }] }, { key: "3b61611644b1", group: "Main missions", file: "src/game/missions/templates.ts", line: 888, lines: [888], tpl: "keys:bakery", card: "The Keys", field: "details", text: "{who} is ready to retire and wants the shop to go to someone who kept it running. Owning a building pays you a cut of every delivery made to its door, yours or anyone else\u2019s.", vars: [{ label: "who", expr: "who" }] }, { key: "dc37a56424e2", group: "Main missions", file: "src/game/missions/templates.ts", line: 899, lines: [899], tpl: "clearLot", card: "Clear the Lot", field: "title", text: "Clear the Lot", vars: [] }, { key: "8445c04aeeaf", group: "Main missions", file: "src/game/missions/templates.ts", line: 899, lines: [899], tpl: "clearLot", card: "Clear the Lot", field: "why", text: "A wreck is dumped on a boarded-up lot. Tow it away with two vehicles and the lot and its building are yours.", vars: [] }, { key: "271a930f77cb", group: "Main missions", file: "src/game/missions/templates.ts", line: 899, lines: [899], tpl: "clearLot", card: "Clear the Lot", field: "caption", text: "A dead van", vars: [] }, { key: "e715b07cb38e", group: "Main missions", file: "src/game/missions/templates.ts", line: 903, lines: [903], tpl: "clearLot[1]", card: "Clear the Lot", field: "line", text: "The lot reopens", vars: [] }, { key: "6b11647666f2", group: "Main missions", file: "src/game/missions/templates.ts", line: 922, lines: [922], tpl: "keys:next", card: "Next Door", field: "title", text: "Next Door", vars: [] }, { key: "f9faa18c870e", group: "Main missions", file: "src/game/missions/templates.ts", line: 922, lines: [922], tpl: "keys:next", card: "Next Door", field: "why", text: "Keep your own place busy with two deliveries in 48 hours and the neighbour next door agrees to sell up to you.", vars: [] }, { key: "cabd7f011606", group: "Main missions", file: "src/game/missions/templates.ts", line: 924, lines: [924], tpl: "keys:next", card: "Next Door", field: "caption", text: "Next door: the {x1}", vars: [{ label: "x1", expr: "shortSite(nb.kind).toLowerCase()" }] }, { key: "9fdfb543a650", group: "Main missions", file: "src/game/missions/templates.ts", line: 949, lines: [949], tpl: "trip:ferry", card: null, field: "label", text: "Ferry to {name}", vars: [{ label: "name", expr: "f.to.port.name" }] }, { key: "f4b56e35a107", group: "Main missions", file: "src/game/missions/templates.ts", line: 951, lines: [951], tpl: "trip:ferry", card: "Over the Water", field: "title", text: "Over the Water", vars: [] }, { key: "cb18cc376c2b", group: "Main missions", file: "src/game/missions/templates.ts", line: 951, lines: [951], tpl: "trip:ferry", card: "Over the Water", field: "why", text: "They\u2019ve never heard of you over the water. Take the car ferry across and drive into the far town to make your name.", vars: [] }, { key: "1b61cd37e16e", group: "Main missions", file: "src/game/missions/templates.ts", line: 953, lines: [953], tpl: "trip:ferry", card: "Over the Water", field: "caption", text: "The car ferry", vars: [] }, { key: "7537cc3abd9f", group: "Main missions", file: "src/game/missions/templates.ts", line: 955, lines: [955], tpl: "trip:ferry", card: "Over the Water", field: "label", text: "Take the ferry", vars: [] }, { key: "8a21e9212021", group: "Main missions", file: "src/game/missions/templates.ts", line: 956, lines: [956], tpl: "trip:ferry", card: "Over the Water", field: "label", text: "Reach {name}", vars: [{ label: "name", expr: "name" }] }, { key: "bbc354f67d54", group: "Main missions", file: "src/game/missions/templates.ts", line: 956, lines: [956], tpl: "trip:ferry", card: "Over the Water", field: "label", text: "Reach the far town", vars: [] }, { key: "ab4b00ef6102", group: "Main missions", file: "src/game/missions/templates.ts", line: 972, lines: [972], tpl: "freshStamps", card: null, field: "label", text: "Visit {x1}", vars: [{ label: "x1", expr: "REGIONS[b.w.region!].title" }] }, { key: "243a26cb08be", group: "Main missions", file: "src/game/missions/templates.ts", line: 973, lines: [973], tpl: "freshStamps", card: null, field: "label", text: "Visit a new biome", vars: [] }, { key: "157dbd5fe501", group: "Main missions", file: "src/game/missions/templates.ts", line: 987, lines: [987], tpl: "freshStamps", card: null, field: "label", text: "Reach a wonder", vars: [] }, { key: "2ffe39eb7589", group: "Main missions", file: "src/game/missions/templates.ts", line: 1004, lines: [1004], tpl: "trip:postcards", card: "Postcards", field: "title", text: "Postcards", vars: [] }, { key: "dd85e08d3be9", group: "Main missions", file: "src/game/missions/templates.ts", line: 1004, lines: [1004], tpl: "trip:postcards", card: "Postcards", field: "why", text: "Visit each place on the list to stamp your Passport, and your car gets a free coat of {name} paint.", vars: [{ label: "name", expr: "paint.name" }] }, { key: "f3fc2b4a5756", group: "Main missions", file: "src/game/missions/templates.ts", line: 1006, lines: [1006], tpl: "trip:postcards", card: "Postcards", field: "caption", text: "New stamps", vars: [] }, { key: "e18259908b34", group: "Main missions", file: "src/game/missions/templates.ts", line: 1078, lines: [1078], tpl: "rescue", card: null, field: "details", text: "{name} is waving from the water. Get alongside in any boat with a seat free, bring them in to the harbour, and it won\u2019t forget it.", vars: [{ label: "name", expr: "name" }] }, { key: "0bfcf20571b8", group: "Main missions", file: "src/game/missions/templates.ts", line: 1118, lines: [1118], tpl: "fast", card: "Something Fast", field: "title", text: "Something Fast", vars: [] }, { key: "d75a5a2fe33a", group: "Main missions", file: "src/game/missions/templates.ts", line: 1119, lines: [1119], tpl: "heavy", card: "Heavy Lifting", field: "title", text: "Heavy Lifting", vars: [] }, { key: "aa047fe1ad7e", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 69, lines: [69], tpl: "deliverTo", card: null, field: "label", text: "{x1}{name} \u2192 Circuit", vars: [{ label: "x1", expr: "n > 1 ? `${n}\xD7 ` : ''" }, { label: "name", expr: "p.type.name" }] }, { key: "e83275092015", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 88, lines: [88], tpl: "circuit:hotlap", card: "Hot Lap", field: "title", text: "Hot Lap", vars: [] }, { key: "62e902a44a63", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 88, lines: [88], tpl: "circuit:hotlap", card: "Hot Lap", field: "why", text: "Set a clean lap of {name} in under {x1} s in your {c.def.name}. Beat the clock and the team fits racing tyres.", vars: [{ label: "name", expr: "name" }, { label: "x1", expr: "secs.toFixed(1)" }, { label: "c.def.name", expr: "c.def.name" }] }, { key: "a5a3fb4b14cd", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 89, lines: [89], tpl: "circuit:hotlap", card: "Hot Lap", field: "details", text: "{x1} wants to see what your {name} can do on a proper track. Drive in at the gate, get up to speed and take the chequered line flying.", vars: [{ label: "x1", expr: "who.name.split(',')[0]" }, { label: "name", expr: "c.def.name" }] }, { key: "d2d64a620749", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 90, lines: [90], tpl: "circuit:hotlap", card: "Hot Lap", field: "caption", text: "Racing tyres", vars: [] }, { key: "7aa2349980df", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 91, lines: [91], tpl: "circuit:hotlap", card: "Hot Lap", field: "label", text: "Race a lap under {x1} s", vars: [{ label: "x1", expr: "secs.toFixed(1)" }] }, { key: "ed7e49465be8", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 105, lines: [105], tpl: "circuit:raceday", card: "Race Day", field: "title", text: "Race Day", vars: [] }, { key: "ded8a29ce889", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 105, lines: [105], tpl: "circuit:raceday", card: "Race Day", field: "why", text: "Take a grid slot at {x1}: {LAPS} laps against five cars just like yours. Finish in the top three.", vars: [{ label: "x1", expr: "shortName(k.name)" }, { label: "LAPS", expr: "LAPS" }] }, { key: "e1250f742386", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 106, lines: [106], tpl: "circuit:raceday", card: "Race Day", field: "details", text: "A one-make race: every car on the grid is a {name}, so what you have fitted is your edge. The stands will be full.", vars: [{ label: "name", expr: "c.def.name" }] }, { key: "c860457be6ce", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 107, lines: [107], tpl: "circuit:raceday", card: "Race Day", field: "caption", text: "The winner\u2019s cup", vars: [] }, { key: "50acabf763bc", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 108, lines: [108], tpl: "circuit:raceday", card: "Race Day", field: "label", text: "Race to a top three finish", vars: [] }, { key: "114409ead766", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 139, lines: [139], tpl: "slicks", card: "Fresh Rubber", field: "title", text: "Fresh Rubber", vars: [] }, { key: "3625ad0fdce7", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 143, lines: [143], tpl: "fuel", card: "Fuel for the Rig", field: "title", text: "Fuel for the Rig", vars: [] }, { key: "a11a32a9723d", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 147, lines: [147], tpl: "racecar", card: "Car for the Grid", field: "title", text: "Car for the Grid", vars: [] }, { key: "9dab8c5eb489", group: "Circuits", file: "src/game/missions/circuitTemplates.ts", line: 151, lines: [151], tpl: "fans", card: "Fan Bus", field: "title", text: "Fan Bus", vars: [] }, { key: "10a8df00c290", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 139, lines: [139], tpl: "text.hiker", card: "Lost on the Hill", field: "title", text: "Lost on the Hill", vars: [] }, { key: "2e8ec062c23f", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 139, lines: [139], tpl: "text.hiker", card: "Lost on the Hill", field: "why", text: "A walker never came down off the hill. Find them with your searchlight, land beside them and fly them to the hospital.", vars: [] }, { key: "7d931126e5ea", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 139, lines: [139], tpl: "text.hiker", card: "Lost on the Hill", field: "label", text: "Find the lost hiker", vars: [] }, { key: "03b2250c08fd", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 140, lines: [140], tpl: "text.child", card: "Missing Child", field: "title", text: "Missing Child", vars: [] }, { key: "37be7c616ea3", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 140, lines: [140], tpl: "text.child", card: "Missing Child", field: "why", text: "A child wandered into the fields at dusk. Sweep them with your searchlight, pick the child up and fly to the hospital.", vars: [] }, { key: "78c3f609b5f8", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 140, lines: [140], tpl: "text.child", card: "Missing Child", field: "label", text: "Find the missing child", vars: [] }, { key: "da18c48a28b9", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 141, lines: [141], tpl: "text.dog", card: "Lost Dog", field: "title", text: "Lost Dog", vars: [] }, { key: "c9aad9d66a36", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 141, lines: [141], tpl: "text.dog", card: "Lost Dog", field: "why", text: "Their dog bolted at the fireworks and hasn\u2019t come home. Sweep the fields with your searchlight until you find it.", vars: [] }, { key: "05fa8ad887b1", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 141, lines: [141], tpl: "text.dog", card: "Lost Dog", field: "label", text: "Find the lost dog", vars: [] }, { key: "e6e49b6af2e2", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 145, lines: [145], tpl: "lostHiker", card: null, field: "caption", text: "Searchlight work", vars: [] }, { key: "0502c0a72003", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 171, lines: [171], tpl: "manOverboard", card: "Man Overboard", field: "title", text: "Man Overboard", vars: [] }, { key: "f8ccdd60c1f6", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 171, lines: [171], tpl: "manOverboard", card: "Man Overboard", field: "why", text: "Someone went over the side off the harbour. Find the swimmer with your searchlight and light the lifeboat\u2019s way to them.", vars: [] }, { key: "e82596ee708a", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 173, lines: [173], tpl: "manOverboard", card: "Man Overboard", field: "caption", text: "The lifeboat", vars: [] }, { key: "f82953945643", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 197, lines: [197], tpl: "hotPursuit", card: "Hot Pursuit", field: "title", text: "Hot Pursuit", vars: [] }, { key: "5b3c60afdfa6", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 197, lines: [197], tpl: "hotPursuit", card: "Hot Pursuit", field: "why", text: "A getaway driver is about to run. Keep his car in your searchlight while the patrol chases. Lose him in the dark and he\u2019s gone.", vars: [] }, { key: "6fe4f5485212", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 199, lines: [199, 347], tpl: "hotPursuit", card: "Hot Pursuit", field: "caption", text: "Police air support", vars: [] }, { key: "e6d4d8a192e0", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 221, lines: [221], tpl: "stolenCar", card: "Stolen Car", field: "title", text: "Stolen Car", vars: [] }, { key: "72db82bccc5b", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 221, lines: [221], tpl: "stolenCar", card: "Stolen Car", field: "why", text: "A stolen car is hiding in the streets with its lights off. Find it with your searchlight, then keep it lit when it runs.", vars: [] }, { key: "9910e76e4bdf", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 223, lines: [223], tpl: "stolenCar", card: "Stolen Car", field: "caption", text: "The stolen car", vars: [] }, { key: "d29ce810bcc5", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 242, lines: [242], tpl: "liveNews", card: "Live at Eleven", field: "title", text: "Live at Eleven", vars: [] }, { key: "3382e8d9ad72", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 242, lines: [242], tpl: "liveNews", card: "Live at Eleven", field: "why", text: "A lorry has jack-knifed and the eleven o\u2019clock news wants pictures. Hold the story in your beam for a minute on air.", vars: [] }, { key: "e4f9936eb5c0", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 244, lines: [244], tpl: "liveNews", card: "Live at Eleven", field: "caption", text: "Tonight\u2019s story", vars: [] }, { key: "4e10091794fb", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 264, lines: [264], tpl: "nightShoot", card: "The Night Shoot", field: "title", text: "The Night Shoot", vars: [] }, { key: "3a67f7694282", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 264, lines: [264], tpl: "nightShoot", card: "The Night Shoot", field: "why", text: "A film crew needs a helicopter light for its big car stunt. Follow the car, hit the mark and nail the reveal.", vars: [] }, { key: "e12615a5558c", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 266, lines: [266], tpl: "nightShoot", card: "The Night Shoot", field: "caption", text: "The stunt car", vars: [] }, { key: "dff7e9e4c3be", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 291, lines: [291], tpl: "blackout", card: "Blackout", field: "title", text: "Blackout", vars: [] }, { key: "e3799ebd8ab5", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 291, lines: [291], tpl: "blackout", card: "Blackout", field: "why", text: "The power is out across town and an ambulance is crawling in the dark. Light the road ahead of it all the way to the hospital.", vars: [] }, { key: "155466c38789", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 293, lines: [293], tpl: "blackout", card: "Blackout", field: "caption", text: "The ambulance", vars: [] }, { key: "785f6fd56c72", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 316, lines: [316], tpl: "looseStock", card: "Loose Stock", field: "title", text: "Loose Stock", vars: [] }, { key: "3f69775f9449", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 316, lines: [316], tpl: "looseStock", card: "Loose Stock", field: "why", text: "The {x1} through the fence and out on the road. Light each stray so the rancher can round it up before a car hits it.", vars: [{ label: "x1", expr: "what === 'sheep' ? 'sheep are' : 'cows are'" }] }, { key: "de3eb2427354", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 318, lines: [318], tpl: "looseStock", card: "Loose Stock", field: "caption", text: "The rancher\u2019s pickup", vars: [] }, { key: "99d591cb23fe", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 345, lines: [345], tpl: "dropSpotter", card: "Drop Spotter", field: "title", text: "Drop Spotter", vars: [] }, { key: "c2e9b60d102c", group: "Night ops", file: "src/game/missions/nightTemplates.ts", line: 345, lines: [345], tpl: "dropSpotter", card: "Drop Spotter", field: "why", text: "Smugglers are meeting tonight. Sweep the area with your searchlight for their lanterns and tag each contact for the police.", vars: [] }, { key: "594bd3487196", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 244, lines: [244], tpl: "tail:spouse", card: "Working Late", field: "title", text: "Working Late", vars: [] }, { key: "35f250bb44b0", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 244, lines: [244], tpl: "tail:spouse", card: "Working Late", field: "caption", text: "The husband\u2019s convertible", vars: [] }, { key: "bf4c1731fb4f", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 244, lines: [244], tpl: "tail:spouse", card: "Working Late", field: "label", text: "Tail the red Sunny", vars: [] }, { key: "b40c098c4c08", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 257, lines: [257], tpl: "tail:claim", card: "Miracle Recovery", field: "title", text: "Miracle Recovery", vars: [] }, { key: "2eeccd24b7b7", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 257, lines: [257], tpl: "tail:claim", card: "Miracle Recovery", field: "caption", text: "The claimant\u2019s SUV", vars: [] }, { key: "10cc066b74c5", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 257, lines: [257], tpl: "tail:claim", card: "Miracle Recovery", field: "label", text: "Tail the blue Bramble", vars: [] }, { key: "fc6a66ca5763", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 270, lines: [270], tpl: "tail:scoop", card: "The Secret Party", field: "title", text: "The Secret Party", vars: [] }, { key: "4318accb17f6", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 270, lines: [270], tpl: "tail:scoop", card: "The Secret Party", field: "caption", text: "The star\u2019s limousine", vars: [] }, { key: "6fc32df74b21", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 270, lines: [270], tpl: "tail:scoop", card: "The Secret Party", field: "label", text: "Tail the black limo", vars: [] }, { key: "04e1da7f8fd6", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 283, lines: [283], tpl: "tail:rival", card: "Follow the Van", field: "title", text: "Follow the Van", vars: [] }, { key: "d6197bf2cf4f", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 283, lines: [283], tpl: "tail:rival", card: "Follow the Van", field: "caption", text: "A rival\u2019s van", vars: [] }, { key: "bc2e3e0ac4a4", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 283, lines: [283], tpl: "tail:rival", card: "Follow the Van", field: "label", text: "Tail the rival van", vars: [] }, { key: "c4cd2abfb29e", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 308, lines: [308], tpl: "tail:rival", card: "Follow the Van", field: "line", text: "{name} run their vans out of {town}: their depot is lit", vars: [{ label: "name", expr: "s.firm.name" }, { label: "town", expr: "s.firm.town" }] }, { key: "17f5f8299ba6", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 318, lines: [318], tpl: "tail:getaway", card: "Getaway Car", field: "title", text: "Getaway Car", vars: [] }, { key: "d1c562c6e728", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 318, lines: [318], tpl: "tail:getaway", card: "Getaway Car", field: "caption", text: "The getaway car", vars: [] }, { key: "5b56d8bbfc98", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 318, lines: [318], tpl: "tail:getaway", card: "Getaway Car", field: "label", text: "Tail the green Wasabi", vars: [] }, { key: "cb172e462b3e", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 338, lines: [338], tpl: "tail:getaway", card: "Getaway Car", field: "line", text: "The gang\u2019s hideout: {toName}, out past {x1}", vars: [{ label: "toName", expr: "s.toName" }, { label: "x1", expr: "s.town ? s.town.name.replace(/ City$/, '') : 'the town'" }] }, { key: "7397431a91b2", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 351, lines: [351], tpl: "tail:courier", card: "The Courier", field: "title", text: "The Courier", vars: [] }, { key: "f092ff26966d", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 351, lines: [351], tpl: "tail:courier", card: "The Courier", field: "caption", text: "The courier\u2019s runabout", vars: [] }, { key: "f0416e203bd1", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 351, lines: [351], tpl: "tail:courier", card: "The Courier", field: "label", text: "Tail the yellow Pip", vars: [] }, { key: "e79554a6deff", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 372, lines: [372], tpl: "tail:courier", card: "The Courier", field: "line", text: "{x1}{x2} nobody has claimed, at {toName}: drive up and press Tab", vars: [{ label: "x1", expr: "what.charAt(0).toUpperCase()" }, { label: "x2", expr: "what.slice(1)" }, { label: "toName", expr: "s.toName" }] }, { key: "b6ea896c0398", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 379, lines: [379], tpl: "tail:fence", card: "Who\u2019s Buying", field: "title", text: "Who\u2019s Buying", vars: [] }, { key: "091b8f28f6a7", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 379, lines: [379], tpl: "tail:fence", card: "Who\u2019s Buying", field: "caption", text: "The fence\u2019s van", vars: [] }, { key: "37be107dec4f", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 379, lines: [379], tpl: "tail:fence", card: "Who\u2019s Buying", field: "label", text: "Tail the maroon Mule", vars: [] }, { key: "369d19d0c616", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 392, lines: [392], tpl: "tail:fence", card: "Who\u2019s Buying", field: "line", text: "The fence\u2019s people at {toName} will have a night run for you after dark", vars: [{ label: "toName", expr: "s.toName" }] }, { key: "173458df6202", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 443, lines: [443], tpl: "trainTemplate", card: null, field: "caption", text: "A freight on the line", vars: [] }, { key: "2847b9065575", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 454, lines: [454], tpl: "tail:freight", card: "Missing Wagons", field: "title", text: "Missing Wagons", vars: [] }, { key: "8ca7f8abdbd3", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 454, lines: [454, 462], tpl: "tail:freight", card: "Missing Wagons", field: "label", text: "Trail the next freight", vars: [] }, { key: "8e76363a0c77", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 456, lines: [456], tpl: "tail:freight", card: "Missing Wagons", field: "why", text: "Wagons keep going missing between stations. Follow the next freight from the roads beside the line and see where it really stops.", vars: [] }, { key: "1cd44572476b", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 457, lines: [457], tpl: "tail:freight", card: "Missing Wagons", field: "details", text: "The guard rides at the back and watches the line behind. A car keeping level with the train for long gets noticed too, and a freight waits for nobody.", vars: [] }, { key: "6783d765fef8", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 462, lines: [462], tpl: "tail:wagon", card: "Off the Rails", field: "title", text: "Off the Rails", vars: [] }, { key: "e7e35b00aee4", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 464, lines: [464], tpl: "tail:wagon", card: "Off the Rails", field: "why", text: "My goods ride hidden in a freight wagon. Follow the train from the road and see where it\u2019s cut off, then we can talk business.", vars: [] }, { key: "e77c00e5bd66", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 465, lines: [465], tpl: "tail:wagon", card: "Off the Rails", field: "details", text: "If the guard sees a car shadowing his train, the wagon never comes off. Keep back, keep out of his sight, and don\u2019t lose it.", vars: [] }, { key: "ef571b6c5137", group: "Tails", file: "src/game/missions/tailTemplates.ts", line: 466, lines: [466], tpl: "tail:wagon", card: "Off the Rails", field: "line", text: "The wagon comes off at {name}: a contact will call on you some night", vars: [{ label: "name", expr: "yard.name" }] }, { key: "268385b3fd11", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 62, lines: [62], tpl: "JOBS.police[0]", card: "Before the Hearing", field: "title", text: "Before the Hearing", vars: [] }, { key: "c4e34c3d2580", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 62, lines: [62], tpl: "JOBS.police[0]", card: "Before the Hearing", field: "why", text: "The court sits at noon and the evidence is still in the station locker. Get it there sealed.", vars: [] }, { key: "356bb3efaa9a", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 63, lines: [63, 139], tpl: "JOBS.police[1]", card: "The Transfer", field: "title", text: "The Transfer", vars: [] }, { key: "a1600de74085", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 63, lines: [63], tpl: "JOBS.police[1]", card: "The Transfer", field: "why", text: "Two officers, one passenger, a route nobody publishes. Drive carefully and say nothing.", vars: [] }, { key: "5ede37bb9861", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 66, lines: [66], tpl: "JOBS.family[0]", card: "The Envelope", field: "title", text: "The Envelope", vars: [] }, { key: "788bb6e75f63", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 66, lines: [66], tpl: "JOBS.family[0]", card: "The Envelope", field: "why", text: "A shopkeeper is behind with his insurance. Remind him, politely, with this envelope.", vars: [] }, { key: "4aec44a0a6bf", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 67, lines: [67], tpl: "JOBS.family[1]", card: "Wash Day", field: "title", text: "Wash Day", vars: [] }, { key: "3c1ee50d99bf", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 67, lines: [67], tpl: "JOBS.family[1]", card: "Wash Day", field: "why", text: "Last night\u2019s takings need to be somewhere else by morning, and clean when they get there.", vars: [] }, { key: "9be50608fe14", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 70, lines: [70, 154], tpl: "JOBS.ministry[0]", card: "In Triplicate", field: "title", text: "In Triplicate", vars: [] }, { key: "2f72585eb845", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 70, lines: [70], tpl: "JOBS.ministry[0]", card: "In Triplicate", field: "why", text: "Forms that must be signed today, by somebody who has not yet heard of them.", vars: [] }, { key: "cadfd55973ac", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 71, lines: [71], tpl: "JOBS.ministry[1]", card: "Diplomatic Bag", field: "title", text: "Diplomatic Bag", vars: [] }, { key: "b99535d812bc", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 71, lines: [71], tpl: "JOBS.ministry[1]", card: "Diplomatic Bag", field: "why", text: "Two attach\xE9s and a briefcase they will not put down. On time, and no bumps.", vars: [] }, { key: "6d126fefe11f", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 74, lines: [74], tpl: "JOBS.visitors[0]", card: "It Hums", field: "title", text: "It Hums", vars: [] }, { key: "5b2de759bd34", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 74, lines: [74], tpl: "JOBS.visitors[0]", card: "It Hums", field: "why", text: "Something warm and humming lies in a field. The Envoy would like it taken somewhere quiet.", vars: [] }, { key: "02dc28315f71", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 75, lines: [75], tpl: "JOBS.visitors[1]", card: "Soil Samples", field: "title", text: "Soil Samples", vars: [] }, { key: "2b34935eb01f", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 75, lines: [75], tpl: "JOBS.visitors[1]", card: "Soil Samples", field: "why", text: "They are curious about our dirt. Bring them a sample, and do not open the jar.", vars: [] }, { key: "341f19335e59", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 78, lines: [78], tpl: "JOBS.unions[0]", card: "The Pour", field: "title", text: "The Pour", vars: [] }, { key: "e63a93f0c713", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 78, lines: [78], tpl: "JOBS.unions[0]", card: "The Pour", field: "why", text: "The pour is booked for this afternoon and the cement is still at the works.", vars: [] }, { key: "84324095144b", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 79, lines: [79, 168], tpl: "JOBS.unions[1]", card: "Morning Shift", field: "title", text: "Morning Shift", vars: [] }, { key: "e4efbd615787", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 79, lines: [79], tpl: "JOBS.unions[1]", card: "Morning Shift", field: "why", text: "A crew for the site, hard hats on and flasks full. The foreman is counting heads.", vars: [] }, { key: "a361360d4617", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 82, lines: [82], tpl: "JOBS.post[0]", card: "Before the Round", field: "title", text: "Before the Round", vars: [] }, { key: "4a9ddc81fca3", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 82, lines: [82], tpl: "JOBS.post[0]", card: "Before the Round", field: "why", text: "The morning letters are sorted and the round goes out at nine. Get them to the counter first.", vars: [] }, { key: "ca6db3a5c4d3", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 83, lines: [83, 174], tpl: "JOBS.post[1]", card: "Signed For", field: "title", text: "Signed For", vars: [] }, { key: "319dd16eb35d", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 83, lines: [83], tpl: "JOBS.post[1]", card: "Signed For", field: "why", text: "Registered, insured and not to be dropped. Somebody is waiting with a pen.", vars: [] }, { key: "154a254a7589", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 86, lines: [86, 181], tpl: "JOBS.army[0]", card: "Mess Call", field: "title", text: "Mess Call", vars: [] }, { key: "93e738bef18f", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 86, lines: [86], tpl: "JOBS.army[0]", card: "Mess Call", field: "why", text: "The base kitchen is out of everything but tea. Bring rations before the men notice.", vars: [] }, { key: "f2f79444ed0e", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 87, lines: [87], tpl: "JOBS.army[1]", card: "Handle With Care", field: "title", text: "Handle With Care", vars: [] }, { key: "374bd3d82afc", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 87, lines: [87], tpl: "JOBS.army[1]", card: "Handle With Care", field: "why", text: "Crates stencilled DO NOT DROP, for the range. Take the bumps slowly.", vars: [] }, { key: "cf19ee358220", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 90, lines: [90, 188], tpl: "JOBS.studios[0]", card: "The Dailies", field: "title", text: "The Dailies", vars: [] }, { key: "193082f44443", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 90, lines: [90], tpl: "JOBS.studios[0]", card: "The Dailies", field: "why", text: "Yesterday\u2019s film, wanted in the screening room before the director wakes up.", vars: [] }, { key: "9a1146836825", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 91, lines: [91, 187], tpl: "JOBS.studios[1]", card: "On Location", field: "title", text: "On Location", vars: [] }, { key: "1e2dfbf77acc", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 91, lines: [91], tpl: "JOBS.studios[1]", card: "On Location", field: "why", text: "A crew, a camera and one afternoon of good light. Get them to the set.", vars: [] }, { key: "de015e13e29b", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 94, lines: [94, 195], tpl: "JOBS.guild[0]", card: "Ice for the Boxes", field: "title", text: "Ice for the Boxes", vars: [] }, { key: "77c2151c9f15", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 94, lines: [94], tpl: "JOBS.guild[0]", card: "Ice for the Boxes", field: "why", text: "The boats are coming in and the market has no ice. It will not wait long.", vars: [] }, { key: "8d175a3246ec", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 95, lines: [95, 196], tpl: "JOBS.guild[1]", card: "Evening Bait", field: "title", text: "Evening Bait", vars: [] }, { key: "54dcd98858a7", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 95, lines: [95], tpl: "JOBS.guild[1]", card: "Evening Bait", field: "why", text: "A cool box of bait for the evening boats, before they cast off.", vars: [] }, { key: "98ac0c225190", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 98, lines: [98], tpl: "JOBS.press[0]", card: "Hold the Front Page", field: "title", text: "Hold the Front Page", vars: [] }, { key: "95442f123cd3", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 98, lines: [98], tpl: "JOBS.press[0]", card: "Hold the Front Page", field: "why", text: "A leaked memo. The editor wants it before the other papers do.", vars: [] }, { key: "606988cb3dfe", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 99, lines: [99], tpl: "JOBS.press[1]", card: "Hot off the Press", field: "title", text: "Hot off the Press", vars: [] }, { key: "3833bfab361a", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 99, lines: [99], tpl: "JOBS.press[1]", card: "Hot off the Press", field: "why", text: "Bundles still warm from the presses. Yesterday\u2019s news sells for nothing.", vars: [] }, { key: "b35874beeaef", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 121, lines: [121], tpl: "fac:*", card: null, field: "details", text: "{name} pays for its own work at its own rate: the friendlier you are, the better.", vars: [{ label: "name", expr: "d.name" }] }, { key: "68d2f896be1a", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 138, lines: [138], tpl: "OATHS.police[0]", card: "Chain of Evidence", field: "title", text: "Chain of Evidence", vars: [] }, { key: "47a922b9a3b5", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 138, lines: [138], tpl: "OATHS.police[0]", card: "Chain of Evidence", field: "why", text: "Two bags of evidence for the court, sealed and signed. Prove you can be trusted with them.", vars: [] }, { key: "827546f49427", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 139, lines: [139], tpl: "OATHS.police[1]", card: "The Transfer", field: "why", text: "A prisoner moved between stations, in your car, with nobody the wiser.", vars: [] }, { key: "7c05e70ec89e", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 140, lines: [140], tpl: "OATHS.police[2]", card: "Cadet Run", field: "title", text: "Cadet Run", vars: [] }, { key: "5b59d8d90490", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 140, lines: [140], tpl: "OATHS.police[2]", card: "Cadet Run", field: "why", text: "This year\u2019s cadets, from the bus station to their first day at the station.", vars: [] }, { key: "9cf4a552b833", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 141, lines: [141], tpl: "OATHS.police[3]", card: "Recovered Goods", field: "title", text: "Recovered Goods", vars: [] }, { key: "b9f3435f3689", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 141, lines: [141], tpl: "OATHS.police[3]", card: "Recovered Goods", field: "why", text: "A chop shop full of somebody else\u2019s televisions. Take them to the impound. The Family will not thank you.", vars: [] }, { key: "bd278ebebb10", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 142, lines: [142], tpl: "OATHS.police[4]", card: "On the Beat", field: "title", text: "On the Beat", vars: [] }, { key: "cbb2840a7238", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 142, lines: [142], tpl: "OATHS.police[4]", card: "On the Beat", field: "why", text: "Walk the beat with them: three stations after dark, a few minutes at each.", vars: [] }, { key: "ed62f0379e8b", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 145, lines: [145], tpl: "OATHS.family[0]", card: "The Round", field: "title", text: "The Round", vars: [] }, { key: "631c5e4f47c9", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 145, lines: [145], tpl: "OATHS.family[0]", card: "The Round", field: "why", text: "Two shops behind on their insurance. Remind them, with envelopes.", vars: [] }, { key: "35add58ccce9", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 146, lines: [146], tpl: "OATHS.family[1]", card: "Clean Money", field: "title", text: "Clean Money", vars: [] }, { key: "d6ff230c16f1", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 146, lines: [146], tpl: "OATHS.family[1]", card: "Clean Money", field: "why", text: "The casino\u2019s takings, out the back door and into the club.", vars: [] }, { key: "7246f4195205", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 147, lines: [147], tpl: "OATHS.family[2]", card: "Lost Evidence", field: "title", text: "Lost Evidence", vars: [] }, { key: "0430e02c91ec", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 147, lines: [147], tpl: "OATHS.family[2]", card: "Lost Evidence", field: "why", text: "There is evidence at the station that would be better at the chop shop. The Constabulary will not forgive this.", vars: [] }, { key: "873ac1df7c10", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 148, lines: [148], tpl: "OATHS.family[3]", card: "A Quiet Word", field: "title", text: "A Quiet Word", vars: [] }, { key: "d770970d41df", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 148, lines: [148], tpl: "OATHS.family[3]", card: "A Quiet Word", field: "why", text: "The Don would like a word, at the villa, at midnight. Come alone.", vars: [] }, { key: "dd44be780621", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 149, lines: [149], tpl: "OATHS.family[4]", card: "Family Business", field: "title", text: "Family Business", vars: [] }, { key: "3e3c4ddb73f3", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 149, lines: [149], tpl: "OATHS.family[4]", card: "Family Business", field: "why", text: "A suitcase of cash to the villa. Nobody counts it but the Don.", vars: [] }, { key: "0db30bc8207f", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 152, lines: [152], tpl: "OATHS.ministry[0]", card: "Polling Day", field: "title", text: "Polling Day", vars: [] }, { key: "4c4d3ab2fbff", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 152, lines: [152], tpl: "OATHS.ministry[0]", card: "Polling Day", field: "why", text: "Two town halls are waiting for their ballot boxes, and the polls open whether they are there or not.", vars: [] }, { key: "8dc25351c445", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 153, lines: [153], tpl: "OATHS.ministry[1]", card: "State Visit", field: "title", text: "State Visit", vars: [] }, { key: "226d70d9d39e", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 153, lines: [153], tpl: "OATHS.ministry[1]", card: "State Visit", field: "why", text: "A head of state, from the airport to the embassy. Smooth, on time, no knocks.", vars: [] }, { key: "e09ee8203e1c", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 154, lines: [154], tpl: "OATHS.ministry[2]", card: "In Triplicate", field: "why", text: "Forms for the court, signed in three places and needed in four.", vars: [] }, { key: "795fa16db9ec", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 155, lines: [155], tpl: "OATHS.ministry[3]", card: "Hangar 18", field: "title", text: "Hangar 18", vars: [] }, { key: "68d140791e3a", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 155, lines: [155], tpl: "OATHS.ministry[3]", card: "Hangar 18", field: "why", text: "Something fell out of the sky. Bring what is left of it to the hangar, and forget what you saw. The Visitors will not.", vars: [] }, { key: "34b0a5f3dafc", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 156, lines: [156], tpl: "OATHS.ministry[4]", card: "Visitors at the Door", field: "title", text: "Visitors at the Door", vars: [] }, { key: "e02bca3d3a06", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 156, lines: [156], tpl: "OATHS.ministry[4]", card: "Visitors at the Door", field: "why", text: "Two tall figures wish to meet the Minister. Bring them in quietly, before anyone takes a picture.", vars: [] }, { key: "50141b0fd5ad", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 159, lines: [159], tpl: "OATHS.visitors[0]", card: "Lights in the Sky", field: "title", text: "Lights in the Sky", vars: [] }, { key: "be829dee0eeb", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 159, lines: [159], tpl: "OATHS.visitors[0]", card: "Lights in the Sky", field: "why", text: "Wait at the ring after dark. Something will want to see you there.", vars: [] }, { key: "0402838b03d0", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 160, lines: [160], tpl: "OATHS.visitors[1]", card: "The Probe", field: "title", text: "The Probe", vars: [] }, { key: "d786aa4634d0", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 160, lines: [160], tpl: "OATHS.visitors[1]", card: "The Probe", field: "why", text: "One of theirs, lost in a field and humming. Take it home.", vars: [] }, { key: "3dd62116aa3d", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 161, lines: [161], tpl: "OATHS.visitors[2]", card: "Taken Back", field: "title", text: "Taken Back", vars: [] }, { key: "591a1c84d407", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 161, lines: [161], tpl: "OATHS.visitors[2]", card: "Taken Back", field: "why", text: "Their wreckage, before the Army takes it to the hangar. The Army will not thank you.", vars: [] }, { key: "92b72664b17c", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 162, lines: [162], tpl: "OATHS.visitors[3]", card: "Specimens", field: "title", text: "Specimens", vars: [] }, { key: "42c7c387e417", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 162, lines: [162], tpl: "OATHS.visitors[3]", card: "Specimens", field: "why", text: "They are curious about our dirt, and our researchers have plenty of it.", vars: [] }, { key: "51da35f10d73", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 163, lines: [163], tpl: "OATHS.visitors[4]", card: "First Contact", field: "title", text: "First Contact", vars: [] }, { key: "a7623b22bc44", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 163, lines: [163], tpl: "OATHS.visitors[4]", card: "First Contact", field: "why", text: "A delegation to the town hall, at an hour nobody will see them arrive.", vars: [] }, { key: "269f046b08f1", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 166, lines: [166], tpl: "OATHS.unions[0]", card: "First Pour", field: "title", text: "First Pour", vars: [] }, { key: "9f7105048903", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 166, lines: [166], tpl: "OATHS.unions[0]", card: "First Pour", field: "why", text: "Cement for the yard, two loads, before the mixer drivers go home.", vars: [] }, { key: "b2664d2fccde", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 167, lines: [167], tpl: "OATHS.unions[1]", card: "Rebar", field: "title", text: "Rebar", vars: [] }, { key: "f05076343146", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 167, lines: [167], tpl: "OATHS.unions[1]", card: "Rebar", field: "why", text: "Steel for the frame of the new tower. The pour waits for nobody.", vars: [] }, { key: "48da698129fa", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 168, lines: [168], tpl: "OATHS.unions[2]", card: "Morning Shift", field: "why", text: "A crew from the hall to the site, hard hats on.", vars: [] }, { key: "aaa36ef85d1c", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 169, lines: [169], tpl: "OATHS.unions[3]", card: "Handed In", field: "title", text: "Handed In", vars: [] }, { key: "f43f337ba772", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 169, lines: [169], tpl: "OATHS.unions[3]", card: "Handed In", field: "why", text: "The Family has been leaning on the yard. Take their envelope to the hall instead of paying it. They will remember.", vars: [] }, { key: "a284e6cfc133", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 170, lines: [170], tpl: "OATHS.unions[4]", card: "Topping Out", field: "title", text: "Topping Out", vars: [] }, { key: "5e816cc93948", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 170, lines: [170], tpl: "OATHS.unions[4]", card: "Topping Out", field: "why", text: "Roadstone for the last stretch of the new road. The whole hall will be there.", vars: [] }, { key: "2b773bdc9ca8", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 173, lines: [173], tpl: "OATHS.post[0]", card: "The Morning Post", field: "title", text: "The Morning Post", vars: [] }, { key: "d2eb1f1b44e3", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 173, lines: [173], tpl: "OATHS.post[0]", card: "The Morning Post", field: "why", text: "Two post offices waiting for the morning letters before nine.", vars: [] }, { key: "f9ed831b8df9", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 174, lines: [174], tpl: "OATHS.post[1]", card: "Signed For", field: "why", text: "Registered mail for the bank, insured, and not to be dropped.", vars: [] }, { key: "151d93dff816", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 175, lines: [175], tpl: "OATHS.post[2]", card: "The Island Bag", field: "title", text: "The Island Bag", vars: [] }, { key: "a90f39d2ed74", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 175, lines: [175], tpl: "OATHS.post[2]", card: "The Island Bag", field: "why", text: "A sack of mail for the harbour, where the boat takes it out to the islands.", vars: [] }, { key: "a87e5b58d80d", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 176, lines: [176], tpl: "OATHS.post[3]", card: "The Country Round", field: "title", text: "The Country Round", vars: [] }, { key: "87bb3846e758", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 176, lines: [176], tpl: "OATHS.post[3]", card: "The Country Round", field: "why", text: "The mailboxes at the end of the lanes, before the farmers come down for them.", vars: [] }, { key: "85244a9b48e9", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 177, lines: [177], tpl: "OATHS.post[4]", card: "Return to Sender", field: "title", text: "Return to Sender", vars: [] }, { key: "419b0140f8c6", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 177, lines: [177], tpl: "OATHS.post[4]", card: "Return to Sender", field: "why", text: "A bag from the Social Club that is not mail. Take it to the police. The Family will not like it.", vars: [] }, { key: "8a34368e7335", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 180, lines: [180], tpl: "OATHS.army[0]", card: "Recruits", field: "title", text: "Recruits", vars: [] }, { key: "fc712dff22fa", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 180, lines: [180], tpl: "OATHS.army[0]", card: "Recruits", field: "why", text: "Fresh haircuts and one bag each, from the bus station to the base.", vars: [] }, { key: "02b14ca88898", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 181, lines: [181], tpl: "OATHS.army[1]", card: "Mess Call", field: "why", text: "Rations for the base before the men notice the kitchen is empty.", vars: [] }, { key: "3f9efa1151e9", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 182, lines: [182], tpl: "OATHS.army[2]", card: "Live Rounds", field: "title", text: "Live Rounds", vars: [] }, { key: "7532618e6819", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 182, lines: [182], tpl: "OATHS.army[2]", card: "Live Rounds", field: "why", text: "Ordnance for the airbase. Every bump costs it.", vars: [] }, { key: "dfeb0ebb83b7", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 183, lines: [183], tpl: "OATHS.army[3]", card: "Secured", field: "title", text: "Secured", vars: [] }, { key: "07680cbb1cc7", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 183, lines: [183], tpl: "OATHS.army[3]", card: "Secured", field: "why", text: "The crash site is ours. Bring the wreckage in before the Visitors take it home. They will not forget.", vars: [] }, { key: "59883abc09fb", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 184, lines: [184], tpl: "OATHS.army[4]", card: "Parade Day", field: "title", text: "Parade Day", vars: [] }, { key: "0d778d4cde2f", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 184, lines: [184], tpl: "OATHS.army[4]", card: "Parade Day", field: "why", text: "The colours, the band and a vehicle polished to shine, through the middle of town.", vars: [] }, { key: "d9a0212d069d", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 187, lines: [187], tpl: "OATHS.studios[0]", card: "On Location", field: "why", text: "A crew and a camera to the landmark, while the light is good.", vars: [] }, { key: "586fab371838", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 188, lines: [188], tpl: "OATHS.studios[1]", card: "The Dailies", field: "why", text: "Yesterday\u2019s film to the screening room, before the director wakes up.", vars: [] }, { key: "b1a305f6ad2a", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 189, lines: [189], tpl: "OATHS.studios[2]", card: "Opening Night", field: "title", text: "Opening Night", vars: [] }, { key: "0838f6cbeb16", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 189, lines: [189], tpl: "OATHS.studios[2]", card: "Opening Night", field: "why", text: "The star of the picture, from the hills to the premiere. The crowd is waiting.", vars: [] }, { key: "bacae6020222", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 190, lines: [190], tpl: "OATHS.studios[3]", card: "Bought Back", field: "title", text: "Bought Back", vars: [] }, { key: "88bca1655ded", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 190, lines: [190], tpl: "OATHS.studios[3]", card: "Bought Back", field: "why", text: "The Press has footage the studio would rather it didn\u2019t. Get it back to the lot. The Press will be furious.", vars: [] }, { key: "b1139b90022d", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 191, lines: [191], tpl: "OATHS.studios[4]", card: "The Red Carpet", field: "title", text: "The Red Carpet", vars: [] }, { key: "e0b597660088", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 191, lines: [191], tpl: "OATHS.studios[4]", card: "The Red Carpet", field: "why", text: "The biggest night of the year, and you are driving.", vars: [] }, { key: "05b46cbca495", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 194, lines: [194], tpl: "OATHS.guild[0]", card: "Dawn at the Market", field: "title", text: "Dawn at the Market", vars: [] }, { key: "15903061fcb0", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 194, lines: [194], tpl: "OATHS.guild[0]", card: "Dawn at the Market", field: "why", text: "Be at the fish market at dawn, when the bell goes and the boats come in.", vars: [] }, { key: "cececfcccba4", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 195, lines: [195], tpl: "OATHS.guild[1]", card: "Ice for the Boxes", field: "why", text: "The catch is coming in and the market has no ice.", vars: [] }, { key: "4a8df81758e8", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 196, lines: [196], tpl: "OATHS.guild[2]", card: "Evening Bait", field: "why", text: "Bait for the evening boats, before they cast off.", vars: [] }, { key: "1787873a3ff4", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 197, lines: [197], tpl: "OATHS.guild[3]", card: "The Morning Catch", field: "title", text: "The Morning Catch", vars: [] }, { key: "df3abe34af4d", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 197, lines: [197], tpl: "OATHS.guild[3]", card: "The Morning Catch", field: "why", text: "Fresh fish from the harbour to the market, before the auction.", vars: [] }, { key: "2cdf7b7ea261", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 198, lines: [198], tpl: "OATHS.guild[4]", card: "In the Nets", field: "title", text: "In the Nets", vars: [] }, { key: "5a8141cc8408", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 198, lines: [198], tpl: "OATHS.guild[4]", card: "In the Nets", field: "why", text: "Something that is not fish came up in the nets. Take it to the police. The Family will want to know who did.", vars: [] }, { key: "a9318b23f3dc", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 201, lines: [201], tpl: "OATHS.press[0]", card: "Leaked", field: "title", text: "Leaked", vars: [] }, { key: "c52e2da75fd4", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 201, lines: [201], tpl: "OATHS.press[0]", card: "Leaked", field: "why", text: "A memo from the town hall, for the newsroom, before anybody notices it\u2019s gone.", vars: [] }, { key: "f14633e6d78a", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 202, lines: [202], tpl: "OATHS.press[1]", card: "Morning Edition", field: "title", text: "Morning Edition", vars: [] }, { key: "49bcfe432992", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 202, lines: [202], tpl: "OATHS.press[1]", card: "Morning Edition", field: "why", text: "Bundles for two newsstands, still warm from the presses.", vars: [] }, { key: "c870457b2cbf", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 203, lines: [203], tpl: "OATHS.press[2]", card: "Tape at Six", field: "title", text: "Tape at Six", vars: [] }, { key: "0638835cc4e0", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 203, lines: [203], tpl: "OATHS.press[2]", card: "Tape at Six", field: "why", text: "Footage from the stadium for the six o\u2019clock news.", vars: [] }, { key: "b286dbb8d3d9", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 204, lines: [204], tpl: "OATHS.press[3]", card: "The Scoop", field: "title", text: "The Scoop", vars: [] }, { key: "8151649b05a7", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 204, lines: [204], tpl: "OATHS.press[3]", card: "The Scoop", field: "why", text: "A star\u2019s secret, from the mansion to the newsroom. The Studios will be livid.", vars: [] }, { key: "442e355efb9b", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 205, lines: [205], tpl: "OATHS.press[4]", card: "Live from the Scene", field: "title", text: "Live from the Scene", vars: [] }, { key: "e18eddf7a491", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 205, lines: [205], tpl: "OATHS.press[4]", card: "Live from the Scene", field: "why", text: "Park by the stadium and go live: hold the shot for half a minute.", vars: [] }, { key: "02d958c1c6e6", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 249, lines: [249], tpl: "start.ride", card: null, field: "label", text: "the {rideName}", vars: [{ label: "rideName", expr: "rideName" }] }, { key: "0c1ebb6f42a3", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 252, lines: [252], tpl: "oathStep", card: "{title}", field: "details", text: "The Oath of {name}, step {x1} of 5, begun at their hall in the {rideName}. {x2}", vars: [{ label: "name", expr: "d.name" }, { label: "x1", expr: "i + 1" }, { label: "rideName", expr: "rideName" }, { label: "x2", expr: "i === 4 ? 'The last step: done, and they count you Sworn.' : 'Each step done moves you on to the next.'" }] }, { key: "f7bddac89651", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 253, lines: [253], tpl: "oathStep", card: "{title}", field: "caption", text: "{icon} The Oath \xB7 {x1} of 5", vars: [{ label: "icon", expr: "d.icon" }, { label: "x1", expr: "i + 1" }] }, { key: "42688a8c78ef", group: "Faction oaths", file: "src/game/missions/factionTemplates.ts", line: 263, lines: [263], tpl: "OATH_ARCS", card: "The Oath of {x1}", field: "title", text: "The Oath of {x1}", vars: [{ label: "x1", expr: "FACTIONS[f].name" }] }, { key: "a14b394e5763", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 60, lines: [60], tpl: "CHAINS.police[0]", card: "The Thin Blue Line", field: "title", text: "The Thin Blue Line", vars: [] }, { key: "182040358aac", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 60, lines: [60], tpl: "CHAINS.police[0]", card: "The Thin Blue Line", field: "why", text: "Detective Sergeant Rourke would like a word at the station, then a bag of evidence taken to the court. Sealed.", vars: [] }, { key: "aa7d48bb5494", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 61, lines: [61], tpl: "CHAINS.police[0]", card: "The Thin Blue Line", field: "details", text: "The Constabulary keep the peace in every town: the patrols, the stations, the courts and the cells. Carry their evidence and their prisoners and they will remember your plates kindly. Chief Constable Margaret Hale has run the county force for twenty years and never once taken an envelope.", vars: [] }, { key: "55d39b80b6dd", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 65, lines: [65], tpl: "CHAINS.police[1]", card: "Borrowed Cadets", field: "title", text: "Borrowed Cadets", vars: [] }, { key: "7c32f15347f6", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 65, lines: [65], tpl: "CHAINS.police[1]", card: "Borrowed Cadets", field: "why", text: "The Army lends the station its cadets every summer. Fetch this year\u2019s from the base before the drill sergeant changes his mind.", vars: [] }, { key: "5fb05f46b3b3", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 66, lines: [66], tpl: "CHAINS.police[1]", card: "Borrowed Cadets", field: "details", text: "The Constabulary and the Army are allies: Chief Hale and Colonel Ada Strand trained together, and a favour to one is a favour to both. Work with the soldiers and the police hear about it, and the other way round.", vars: [] }, { key: "a287e7a233e8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 70, lines: [70], tpl: "CHAINS.police[2]", card: "The Chop Shop", field: "title", text: "The Chop Shop", vars: [] }, { key: "efe73e0395fa", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 70, lines: [70], tpl: "CHAINS.police[2]", card: "The Chop Shop", field: "why", text: "A chop shop full of somebody else\u2019s televisions. Take them to the impound before they are sold twice.", vars: [] }, { key: "24208ab257e9", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 71, lines: [71], tpl: "CHAINS.police[2]", card: "The Chop Shop", field: "details", text: "The Family run the chop shops and the social clubs, and they take it personally when their goods go to the impound. Police and Family are opposed: what one thanks you for, the other holds against you. Mr. Benedetti will hear about this one.", vars: [] }, { key: "4a61f7ee2955", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 75, lines: [75], tpl: "CHAINS.police[3]", card: "Off the Record", field: "title", text: "Off the Record", vars: [] }, { key: "501f7071e438", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 75, lines: [75], tpl: "CHAINS.police[3]", card: "Off the Record", field: "why", text: "Somebody at the station is tipping off the Family. Get the evidence to a judge before it turns up in the papers.", vars: [] }, { key: "5ddccdfa795c", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 76, lines: [76], tpl: "CHAINS.police[3]", card: "Off the Record", field: "details", text: "A bent officer, and Rourke means to catch him quietly. The Press would love the story: Editor Cass Marlow neither likes nor hates the police, but she loves a headline, and a leak would let the man walk. Evidence first, newspapers after.", vars: [] }, { key: "189d4358a526", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 80, lines: [80], tpl: "CHAINS.police[4]", card: "The Chief\u2019s Last Case", field: "title", text: "The Chief\u2019s Last Case", vars: [] }, { key: "10a983593d41", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 80, lines: [80], tpl: "CHAINS.police[4]", card: "The Chief\u2019s Last Case", field: "why", text: "Chief Hale retires at the month\u2019s end. She wants the bent officer in court, brought in by somebody she trusts.", vars: [] }, { key: "bf54d39a7d37", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 81, lines: [81], tpl: "CHAINS.police[4]", card: "The Chief\u2019s Last Case", field: "details", text: "Margaret Hale joined as a cadet the year the Family burned the old station down, and has spent her career taking back the streets they ran. The officer who sold her out was her own sergeant. Meet her after dark, then take him in: her last conviction, and the force will remember who drove.", vars: [] }, { key: "7f4efa9d1e12", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 87, lines: [87], tpl: "CHAINS.family[0]", card: "An Introduction", field: "title", text: "An Introduction", vars: [] }, { key: "88c3b1251888", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 87, lines: [87], tpl: "CHAINS.family[0]", card: "An Introduction", field: "why", text: "Mr. Benedetti doesn\u2019t hire strangers. Come to the club, have an espresso, then run one small errand for him.", vars: [] }, { key: "8f76817457ef", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 88, lines: [88], tpl: "CHAINS.family[0]", card: "An Introduction", field: "details", text: "The Family: social clubs, chop shops, a villa on the hill and every load that moves after dark. Their work pays well and their night runs pay better. Nobody says the word. The Don, Vittorio Caruso, is old now, and they say he has not left the villa in a year.", vars: [] }, { key: "bb59be0ec52a", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 92, lines: [92], tpl: "CHAINS.family[1]", card: "Cheap Cement", field: "title", text: "Cheap Cement", vars: [] }, { key: "03ecdfe64f52", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 92, lines: [92], tpl: "CHAINS.family[1]", card: "Cheap Cement", field: "why", text: "Half the sites in the county pour Family cement. Take a load to the builders\u2019 yard, and say nothing about the price.", vars: [] }, { key: "8e5480c9dc2f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 93, lines: [93], tpl: "CHAINS.family[1]", card: "Cheap Cement", field: "details", text: "The Brotherhood of Builders pretends not to know where its cheap cement comes from. Shop Steward Mags Doyle has no love for the Family, but she likes a cheap pour, and the Family likes the Brotherhood busy. Neither side thanks you for the other, but the trucks keep rolling.", vars: [] }, { key: "bb0b51fdad14", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 97, lines: [97], tpl: "CHAINS.family[2]", card: "Fell Off a Lorry", field: "title", text: "Fell Off a Lorry", vars: [] }, { key: "e801e449cc7b", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 97, lines: [97], tpl: "CHAINS.family[2]", card: "Fell Off a Lorry", field: "why", text: "A lorry of televisions fell off the back of something. The chop shop will find them a home before the police do.", vars: [] }, { key: "c16a9b81aff3", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 98, lines: [98], tpl: "CHAINS.family[2]", card: "Fell Off a Lorry", field: "details", text: "The Constabulary are the Family\u2019s oldest enemy. Every load you carry for one costs you with the other, so choose your friends with care. Detective Sergeant Rourke keeps a list of plates, and yours has just gone on it.", vars: [] }, { key: "8af08c47e53a", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 102, lines: [102], tpl: "CHAINS.family[3]", card: "Wash Day", field: "title", text: "Wash Day", vars: [] }, { key: "8c8113ac36a4", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 102, lines: [102], tpl: "CHAINS.family[3]", card: "Wash Day", field: "why", text: "Last night\u2019s takings need to be somewhere else by morning. Out the back of the casino and into the club.", vars: [] }, { key: "5b18369629a2", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 103, lines: [103], tpl: "CHAINS.family[3]", card: "Wash Day", field: "details", text: "The Family\u2019s money comes in dirty and goes out clean, and the Fishermen\u2019s Guild is no help: Harbourmaster Brennock won\u2019t let Family crates on his boats, so it goes by road now. That is the Family\u2019s trouble this year, and the Don\u2019s heir wants it solved.", vars: [] }, { key: "dd2a63dd408c", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 107, lines: [107], tpl: "CHAINS.family[4]", card: "The Don\u2019s Birthday", field: "title", text: "The Don\u2019s Birthday", vars: [] }, { key: "a70236190357", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 107, lines: [107], tpl: "CHAINS.family[4]", card: "The Don\u2019s Birthday", field: "why", text: "Don Caruso turns ninety tonight and wants to meet the driver everybody keeps talking about. Midnight, the villa.", vars: [] }, { key: "d8f66412aae9", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 108, lines: [108], tpl: "CHAINS.family[4]", card: "The Don\u2019s Birthday", field: "details", text: "Vittorio Caruso came over with one suitcase and built the Family from a single social club. He means to name his heir tonight, and Mr. Benedetti is not sure it will be him. Pay your respects at midnight, then bring the Don his birthday present from the casino.", vars: [] }, { key: "2e15d30ffc81", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 114, lines: [114], tpl: "CHAINS.ministry[0]", card: "Proper Channels", field: "title", text: "Proper Channels", vars: [] }, { key: "1768aa39ac9f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 114, lines: [114], tpl: "CHAINS.ministry[0]", card: "Proper Channels", field: "why", text: "The Permanent Secretary will see you at the Ministry. Bring a pen. Then take these forms where they are needed.", vars: [] }, { key: "b7267c0ee68f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 115, lines: [115], tpl: "CHAINS.ministry[0]", card: "Proper Channels", field: "details", text: "The Ministry is the state above the towns: the halls, the customs posts, the embassies, the Mint, and a hangar nobody talks about. It pays on time and in triplicate. At the top sits Minister Augusta Fairweather, who has never once been surprised in public.", vars: [] }, { key: "413927878fd2", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 119, lines: [119], tpl: "CHAINS.ministry[1]", card: "By Return of Post", field: "title", text: "By Return of Post", vars: [] }, { key: "8464023582ba", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 119, lines: [119], tpl: "CHAINS.ministry[1]", card: "By Return of Post", field: "why", text: "Registered papers for the sorting office, signed for at both ends. The Post carries what the Ministry writes.", vars: [] }, { key: "bd6a4ae3c378", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 120, lines: [120], tpl: "CHAINS.ministry[1]", card: "By Return of Post", field: "details", text: "The Ministry has two old allies. The Post carries its letters, and Postmaster Ivy Penhallow has never lost one. The Army carries out its orders. Help either and the Ministry hears of it.", vars: [] }, { key: "6ade9eecde18", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 124, lines: [124], tpl: "CHAINS.ministry[2]", card: "Polling Day", field: "title", text: "Polling Day", vars: [] }, { key: "2ac0d7cc9ff8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 124, lines: [124], tpl: "CHAINS.ministry[2]", card: "Polling Day", field: "why", text: "Ballot boxes for the town halls before the polls open. The newspapers will be counting them as closely as we are.", vars: [] }, { key: "b02cbf51df89", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 125, lines: [125], tpl: "CHAINS.ministry[2]", card: "Polling Day", field: "details", text: "The Press and the Ministry are neither friends nor enemies: Editor Cass Marlow prints what the Ministry tells her and then what it didn\u2019t. The unions want a say in the vote and the Guild wants the fishing quotas changed. Nobody is pleased, and that is how the Secretary likes it.", vars: [] }, { key: "dbc88f72ce97", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 129, lines: [129], tpl: "CHAINS.ministry[3]", card: "Something Fell", field: "title", text: "Something Fell", vars: [] }, { key: "7d3b4a7e6500", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 129, lines: [129], tpl: "CHAINS.ministry[3]", card: "Something Fell", field: "why", text: "Something came down in a field last night. Take what is left to the hangar, and kindly forget what you saw.", vars: [] }, { key: "94de56acc6f1", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 130, lines: [130], tpl: "CHAINS.ministry[3]", card: "Something Fell", field: "details", text: "Hangar 18 is the Ministry\u2019s worst-kept secret. The Army guards it gladly; the Visitors would like their wreckage back, and do not forget who took it. The Ministry tries to stay on speaking terms with both, which is harder than it sounds.", vars: [] }, { key: "0deefa6fdd5e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 134, lines: [134], tpl: "CHAINS.ministry[4]", card: "The Minister\u2019s Guest", field: "title", text: "The Minister\u2019s Guest", vars: [] }, { key: "e782ad1f86c3", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 134, lines: [134], tpl: "CHAINS.ministry[4]", card: "The Minister\u2019s Guest", field: "why", text: "Minister Fairweather asks to see you at midnight. Then a guest of state from the airport, with no cameras.", vars: [] }, { key: "33ddb37e56b5", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 135, lines: [135], tpl: "CHAINS.ministry[4]", card: "The Minister\u2019s Guest", field: "details", text: "Augusta Fairweather has quietly kept the county out of three wars and one invasion. She is the only one in government who has met the Navigator, and tonight she means to arrange a second meeting. Her guest\u2019s name is on no list. Drive smoothly and ask nothing.", vars: [] }, { key: "7754fa4d1d6e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 141, lines: [141], tpl: "CHAINS.visitors[0]", card: "Lights Over the Hill", field: "title", text: "Lights Over the Hill", vars: [] }, { key: "e44c88a33523", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 141, lines: [141], tpl: "CHAINS.visitors[0]", card: "Lights Over the Hill", field: "why", text: "Wait at the ring after dark. The Envoy will find you, and has something warm and humming to send home.", vars: [] }, { key: "392fec0449b8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 142, lines: [142], tpl: "CHAINS.visitors[0]", card: "Lights Over the Hill", field: "details", text: "Lights over the mesas, rings of stones in the high country, a crash nobody reported. The Visitors are polite, patient and very far from home, and they want their things back. Bring them what is theirs and they will remember. They remember everything.", vars: [] }, { key: "5b4a9b65c4bb", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 146, lines: [146], tpl: "CHAINS.visitors[1]", card: "Special Effects", field: "title", text: "Special Effects", vars: [] }, { key: "4b60beb21441", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 146, lines: [146], tpl: "CHAINS.visitors[1]", card: "Special Effects", field: "why", text: "A film crew to the crash site, so any photographs look like a picture being made. The Studios owe us a favour.", vars: [] }, { key: "99cb80a1892d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 147, lines: [147], tpl: "CHAINS.visitors[1]", card: "Special Effects", field: "details", text: "The Studios are the Visitors\u2019 unlikely allies. Producer Dash Delano\u2019s science-fiction pictures have hidden real landings for years: who believes a flying saucer on a film set? Help one and the other is grateful.", vars: [] }, { key: "28b4eb64537f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 151, lines: [151], tpl: "CHAINS.visitors[2]", card: "Not Theirs to Keep", field: "title", text: "Not Theirs to Keep", vars: [] }, { key: "82773ab6c093", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 151, lines: [151], tpl: "CHAINS.visitors[2]", card: "Not Theirs to Keep", field: "why", text: "The Army is on its way to the crash. Take the wreckage to the ring before the trucks arrive. Quietly.", vars: [] }, { key: "1972542a88af", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 152, lines: [152], tpl: "CHAINS.visitors[2]", card: "Not Theirs to Keep", field: "details", text: "The Army and the Visitors are opposed. Colonel Ada Strand calls the wreckage spoils of war; the Envoy calls it a lifeboat. Every piece you take home is a piece the Army will not forgive, and the Army\u2019s friends in the Constabulary will notice too.", vars: [] }, { key: "1f686ad40427", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 156, lines: [156], tpl: "CHAINS.visitors[3]", card: "A Word Upstairs", field: "title", text: "A Word Upstairs", vars: [] }, { key: "d0da585eea86", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 156, lines: [156], tpl: "CHAINS.visitors[3]", card: "A Word Upstairs", field: "why", text: "The Envoy wants the Ministry to understand. Take these samples to the people who sign the papers.", vars: [] }, { key: "638379ac6c17", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 157, lines: [157], tpl: "CHAINS.visitors[3]", card: "A Word Upstairs", field: "details", text: "The Ministry watches the Visitors with one eye and the Army with the other. Minister Fairweather would rather talk than shoot, and she is the only one upstairs who will listen. These samples show what the Visitors need: nothing of ours, only the way home.", vars: [] }, { key: "a6988204fca5", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 161, lines: [161], tpl: "CHAINS.visitors[4]", card: "The Navigator", field: "title", text: "The Navigator", vars: [] }, { key: "c5d4ef83f78d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 161, lines: [161], tpl: "CHAINS.visitors[4]", card: "The Navigator", field: "why", text: "The Navigator wishes to meet you at the ring at midnight. Bring the last probe. It is the beacon home.", vars: [] }, { key: "f2eb0a26c0c7", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 162, lines: [162], tpl: "CHAINS.visitors[4]", card: "The Navigator", field: "details", text: "The Navigator brought forty of them here by accident, a wrong turn between two stars, and has spent sixty of our years putting the beacon back together, piece by piece, from fields and hangars. This is the last piece. Meet them at midnight and carry it in.", vars: [] }, { key: "7e7cfcb34503", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 168, lines: [168], tpl: "CHAINS.unions[0]", card: "Hard Hats On", field: "title", text: "Hard Hats On", vars: [] }, { key: "7e4b6c650afc", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 168, lines: [168], tpl: "CHAINS.unions[0]", card: "Hard Hats On", field: "why", text: "Shop Steward Mags Doyle wants to see you at the hall, then a crew to the site for the morning shift.", vars: [] }, { key: "aca95e6027eb", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 169, lines: [169], tpl: "CHAINS.unions[0]", card: "Hard Hats On", field: "details", text: "The Brotherhood of Builders: every building site, every road crew and every cement mixer in the world answers to the hall. Pay for roads and earthworks and the Brotherhood counts it as work done for them. General Secretary Tom Kearney has led the hall for forty years.", vars: [] }, { key: "69490fea9f9d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 173, lines: [173], tpl: "CHAINS.unions[1]", card: "Permits in Triplicate", field: "title", text: "Permits in Triplicate", vars: [] }, { key: "04dd204da977", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 173, lines: [173], tpl: "CHAINS.unions[1]", card: "Permits in Triplicate", field: "why", text: "Nothing gets built without a stamp from the Ministry. Fetch the permits so the crew can start.", vars: [] }, { key: "2de06bf35370", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 174, lines: [174], tpl: "CHAINS.unions[1]", card: "Permits in Triplicate", field: "details", text: "The Brotherhood and the Ministry need each other and say so as little as possible. The Ministry pays for the roads; the Brotherhood builds them, slowly, until the forms arrive. Doyle hates the paperwork, but she knows a job with no permit is a job with no pay.", vars: [] }, { key: "7234e18a4126", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 178, lines: [178], tpl: "CHAINS.unions[2]", card: "Not Paying", field: "title", text: "Not Paying", vars: [] }, { key: "2efcb85e72cb", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 178, lines: [178], tpl: "CHAINS.unions[2]", card: "Not Paying", field: "why", text: "The Family has been leaning on the yard again. Take their envelope to the police instead of paying it.", vars: [] }, { key: "b30093369fb4", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 179, lines: [179], tpl: "CHAINS.unions[2]", card: "Not Paying", field: "details", text: "The Family sells the Brotherhood cheap cement and then sends men round for a cut. Doyle has had enough. Benedetti will remember who carried that envelope, but the Constabulary will be pleased, and so will every builder who ever paid.", vars: [] }, { key: "998226f3ca6d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 183, lines: [183], tpl: "CHAINS.unions[3]", card: "The Big Pour", field: "title", text: "The Big Pour", vars: [] }, { key: "ec2bbaad821c", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 183, lines: [183], tpl: "CHAINS.unions[3]", card: "The Big Pour", field: "why", text: "The new tower\u2019s foundations go in today. Two loads of cement, and the whole hall is watching the clock.", vars: [] }, { key: "75ca292a6428", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 184, lines: [184], tpl: "CHAINS.unions[3]", card: "The Big Pour", field: "details", text: "The tower will be the tallest in the county, built by the Brotherhood from the first spade to the topping-out. The old hands remember the last tower, which fell because somebody saved on the cement. Not this time.", vars: [] }, { key: "334fa6a1349e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 188, lines: [188], tpl: "CHAINS.unions[4]", card: "Big Tom\u2019s Road", field: "title", text: "Big Tom\u2019s Road", vars: [] }, { key: "ade5c93d0825", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 188, lines: [188], tpl: "CHAINS.unions[4]", card: "Big Tom\u2019s Road", field: "why", text: "Big Tom Kearney wants to shake your hand at the yard at dawn. Then roadstone for the last mile of his road.", vars: [] }, { key: "40f91d9bd646", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 189, lines: [189], tpl: "CHAINS.unions[4]", card: "Big Tom\u2019s Road", field: "details", text: "Tom Kearney\u2019s father died building the old coast road, and Tom has spent forty years making sure no one in the hall dies like that again. This new road is his last before he retires, and the hall means to name it after him. Be at the yard at dawn.", vars: [] }, { key: "9575faa8322e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 195, lines: [195], tpl: "CHAINS.post[0]", card: "First Class", field: "title", text: "First Class", vars: [] }, { key: "a10c708d2d57", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 195, lines: [195], tpl: "CHAINS.post[0]", card: "First Class", field: "why", text: "Postmaster Ivy Penhallow has a round for you. Meet her at the sorting office, then take the morning letters out.", vars: [] }, { key: "8a270a02b5f5", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 196, lines: [196], tpl: "CHAINS.post[0]", card: "First Class", field: "details", text: "The Post: the sorting offices, the rounds, the island boats and the night mail plane. Every letter goes through, whatever the weather. At the top is the Postmaster General, Ambrose Quill, who still does a round himself on Christmas morning.", vars: [] }, { key: "b035ef5a1140", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 200, lines: [200], tpl: "CHAINS.post[1]", card: "The Island Boat", field: "title", text: "The Island Boat", vars: [] }, { key: "7ddf7d2522bf", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 200, lines: [200], tpl: "CHAINS.post[1]", card: "The Island Boat", field: "why", text: "A sack of mail for the harbour, where the Guild\u2019s boats take it out to the islands. They sail on the tide.", vars: [] }, { key: "88d3b6d70911", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 201, lines: [201], tpl: "CHAINS.post[1]", card: "The Island Boat", field: "details", text: "The Post and the Fishermen\u2019s Guild are allies: the Guild\u2019s boats carry the island mail, and the Post has never once docked their pay. Harbourmaster Brennock is a friend of the office. Help one and the other knows.", vars: [] }, { key: "38e9c3d29107", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 205, lines: [205], tpl: "CHAINS.post[2]", card: "Official Business", field: "title", text: "Official Business", vars: [] }, { key: "223d4d4018ae", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 205, lines: [205], tpl: "CHAINS.post[2]", card: "Official Business", field: "why", text: "Registered letters for the Ministry, insured and to be signed for. Not one goes missing.", vars: [] }, { key: "386c21e40d5e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 206, lines: [206], tpl: "CHAINS.post[2]", card: "Official Business", field: "details", text: "The Ministry is the Post\u2019s other ally and its oldest customer. Every law, permit and summons goes out in a Post van, and the Permanent Secretary counts every envelope twice.", vars: [] }, { key: "7020bda367a2", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 210, lines: [210], tpl: "CHAINS.post[3]", card: "Not Our Parcel", field: "title", text: "Not Our Parcel", vars: [] }, { key: "2d6b19dffb13", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 210, lines: [210], tpl: "CHAINS.post[3]", card: "Not Our Parcel", field: "why", text: "A bag from the Social Club that is not mail. Take it to the police and let them open it.", vars: [] }, { key: "3ce3a0fa4c38", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 211, lines: [211], tpl: "CHAINS.post[3]", card: "Not Our Parcel", field: "details", text: "The Family has been using the parcel post to move what it shouldn\u2019t, and the Post will not stand for it. Penhallow would rather lose a customer than lose her name. The Family will be angry. The Constabulary will be grateful.", vars: [] }, { key: "4c407bb36cc6", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 215, lines: [215], tpl: "CHAINS.post[4]", card: "The Last Round", field: "title", text: "The Last Round", vars: [] }, { key: "dc6ca21fdba7", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 215, lines: [215], tpl: "CHAINS.post[4]", card: "The Last Round", field: "why", text: "Postmaster General Quill does his last round at dawn and wants a driver. Every mailbox at the end of every lane.", vars: [] }, { key: "6024132b9f79", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 216, lines: [216], tpl: "CHAINS.post[4]", card: "The Last Round", field: "details", text: "Ambrose Quill started as a boy on a bicycle, fifty years ago, on the very round he will finish tomorrow. In all that time the Post has lost one letter, and he still keeps it in his desk, unopened. Meet him at dawn and drive him round.", vars: [] }, { key: "d08035a46b12", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 222, lines: [222], tpl: "CHAINS.army[0]", card: "Report for Duty", field: "title", text: "Report for Duty", vars: [] }, { key: "fb44d80a7814", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 222, lines: [222], tpl: "CHAINS.army[0]", card: "Report for Duty", field: "why", text: "Quartermaster Sergeant Okafor needs a civilian driver. Report to the depot, then get rations to the base.", vars: [] }, { key: "43265033f5de", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 223, lines: [223], tpl: "CHAINS.army[0]", card: "Report for Duty", field: "details", text: "The Army: the bases, the forts, the depots and the airbases, the convoys on the roads and the fighters in the sky. It pays on time and asks no questions it does not already know the answer to. Colonel Ada Strand commands the county\u2019s garrison.", vars: [] }, { key: "927fbd10f59f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 227, lines: [227], tpl: "CHAINS.army[1]", card: "Joint Exercise", field: "title", text: "Joint Exercise", vars: [] }, { key: "f6cc0c2cd35b", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 227, lines: [227], tpl: "CHAINS.army[1]", card: "Joint Exercise", field: "why", text: "The Constabulary\u2019s cadets come to the base for a week of drill. Pick them up from the station.", vars: [] }, { key: "3f32b4505506", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 228, lines: [228], tpl: "CHAINS.army[1]", card: "Joint Exercise", field: "details", text: "The Army and the Constabulary are allies, and Colonel Strand and Chief Hale trained together. Do good work for the soldiers and the police hear of it, and the other way round.", vars: [] }, { key: "8a4fb5dfa373", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 232, lines: [232], tpl: "CHAINS.army[2]", card: "Orders from Above", field: "title", text: "Orders from Above", vars: [] }, { key: "36f878abb992", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 232, lines: [232], tpl: "CHAINS.army[2]", card: "Orders from Above", field: "why", text: "Sealed orders from the Ministry for the base commander. Do not open them. We will know.", vars: [] }, { key: "4daf1e77b329", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 233, lines: [233], tpl: "CHAINS.army[2]", card: "Orders from Above", field: "details", text: "The Army answers to the Ministry, and Minister Fairweather has strong views on what the Army should and should not shoot at. The two are allies, mostly. Colonel Strand reads every order twice.", vars: [] }, { key: "6c35f30784da", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 237, lines: [237], tpl: "CHAINS.army[3]", card: "Recovery Team", field: "title", text: "Recovery Team", vars: [] }, { key: "8b1ec0ac57b8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 237, lines: [237], tpl: "CHAINS.army[3]", card: "Recovery Team", field: "why", text: "Something came down near the range. Bring the wreckage to the hangar before the Visitors take it home.", vars: [] }, { key: "0372a944df3b", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 238, lines: [238], tpl: "CHAINS.army[3]", card: "Recovery Team", field: "details", text: "The Army and the Visitors are opposed. Strand calls it securing the county; the Envoy calls it theft. Every piece you bring in is a piece the Visitors will hold against you, and their friends at the Studios will hear of it too.", vars: [] }, { key: "dce0b200690b", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 242, lines: [242], tpl: "CHAINS.army[4]", card: "The Colonel\u2019s Parade", field: "title", text: "The Colonel\u2019s Parade", vars: [] }, { key: "12b0e5fefce8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 242, lines: [242], tpl: "CHAINS.army[4]", card: "The Colonel\u2019s Parade", field: "why", text: "Colonel Strand takes her last salute this morning. Meet her at the base at dawn, then lead the parade through town.", vars: [] }, { key: "c05fcfdcb2f8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 243, lines: [243], tpl: "CHAINS.army[4]", card: "The Colonel\u2019s Parade", field: "details", text: "Ada Strand has commanded the garrison through two floods, a fire and something in the sky she still won\u2019t talk about. She has asked for the driver from the hangar run to lead her last parade. Be at the base at dawn, polished.", vars: [] }, { key: "05386424851e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 249, lines: [249], tpl: "CHAINS.studios[0]", card: "Lights, Camera", field: "title", text: "Lights, Camera", vars: [] }, { key: "0f74ffaef5ce", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 249, lines: [249], tpl: "CHAINS.studios[0]", card: "Lights, Camera", field: "why", text: "Producer Dash Delano wants to see you at the gate. Then yesterday\u2019s film to the screening room, fast.", vars: [] }, { key: "84a9b2d5bf4f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 250, lines: [250], tpl: "CHAINS.studios[0]", card: "Lights, Camera", field: "details", text: "The Studios: the lots, the backlots, the premieres and the stars in their hillside mansions. Every picture needs a driver, and the good ones get a credit. The studio belongs to Vivienne Lark, who was the biggest star in the world before she bought it.", vars: [] }, { key: "7004bfa38d41", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 254, lines: [254], tpl: "CHAINS.studios[1]", card: "Creature Feature", field: "title", text: "Creature Feature", vars: [] }, { key: "bcfe7d595e78", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 254, lines: [254], tpl: "CHAINS.studios[1]", card: "Creature Feature", field: "why", text: "A crew to the crop circle to shoot the saucer picture. Our friends in the sky like a good cover story.", vars: [] }, { key: "0dd876ab5728", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 255, lines: [255], tpl: "CHAINS.studios[1]", card: "Creature Feature", field: "details", text: "The Studios and the Visitors are allies. Delano\u2019s science-fiction pictures have hidden real landings for years, and in return the Envoy gives the best notes on special effects in the business. Help either and the other is grateful.", vars: [] }, { key: "d0601a7de720", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 259, lines: [259], tpl: "CHAINS.studios[2]", card: "Kill the Story", field: "title", text: "Kill the Story", vars: [] }, { key: "a2f31f86839d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 259, lines: [259], tpl: "CHAINS.studios[2]", card: "Kill the Story", field: "why", text: "The newsroom has footage the studio would rather it didn\u2019t. Get it back to the lot before six.", vars: [] }, { key: "43ee95111a81", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 260, lines: [260], tpl: "CHAINS.studios[2]", card: "Kill the Story", field: "details", text: "The Press and the Studios are rivals. The papers sell scandal and the studio sells dreams, and both want the same front page. The newsroom will be furious, and Editor Cass Marlow does not forget a stolen tape.", vars: [] }, { key: "d63a2bc6c214", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 264, lines: [264], tpl: "CHAINS.studios[3]", card: "On Location", field: "title", text: "On Location", vars: [] }, { key: "658d03b94816", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 264, lines: [264], tpl: "CHAINS.studios[3]", card: "On Location", field: "why", text: "Vivienne Lark\u2019s comeback picture shoots today. A crew to the landmark while the light holds.", vars: [] }, { key: "8c62a3784683", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 265, lines: [265], tpl: "CHAINS.studios[3]", card: "On Location", field: "details", text: "Lark has not stepped in front of a camera in twenty years, and the whole studio has bet the lot on her return. The papers say she is finished. Delano says she has never been better. The light goes at four.", vars: [] }, { key: "34722dcae4c6", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 269, lines: [269], tpl: "CHAINS.studios[4]", card: "Premiere Night", field: "title", text: "Premiere Night", vars: [] }, { key: "8ae0cb268db1", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 269, lines: [269], tpl: "CHAINS.studios[4]", card: "Premiere Night", field: "why", text: "Vivienne Lark wants to meet her driver at the theatre after dark, then ride in from the hills herself.", vars: [] }, { key: "244e3300d597", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 270, lines: [270], tpl: "CHAINS.studios[4]", card: "Premiere Night", field: "details", text: "Vivienne Lark bought the studio with her last pay cheque so nobody could ever tell her what to play again. Tonight is the premiere of the picture that will make her or break her. Meet her at the theatre, then bring her down from the mansion in style.", vars: [] }, { key: "038a1e2d8d21", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 276, lines: [276], tpl: "CHAINS.guild[0]", card: "Fair Winds", field: "title", text: "Fair Winds", vars: [] }, { key: "dc9172db2883", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 276, lines: [276], tpl: "CHAINS.guild[0]", card: "Fair Winds", field: "why", text: "Harbourmaster Brennock would like to look you over at the market. Then ice for the boxes before the boats come in.", vars: [] }, { key: "373242c3a274", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 277, lines: [277], tpl: "CHAINS.guild[0]", card: "Fair Winds", field: "details", text: "The Fishermen\u2019s Guild: the boats, the net lofts, the fish markets and the lifeboats. A Guild skipper never leaves a crew in the water. Their eldest is Mother Corran, who has fished these waters for sixty years and still takes the first boat out.", vars: [] }, { key: "b779ccd474b1", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 281, lines: [281], tpl: "CHAINS.guild[1]", card: "The Mail Boat", field: "title", text: "The Mail Boat", vars: [] }, { key: "f1b0794f6219", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 281, lines: [281], tpl: "CHAINS.guild[1]", card: "The Mail Boat", field: "why", text: "The island post goes out with the evening boats. Bring the sacks to the harbour before they cast off.", vars: [] }, { key: "8f5f7addc805", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 282, lines: [282], tpl: "CHAINS.guild[1]", card: "The Mail Boat", field: "details", text: "The Guild and the Post are allies: the Guild\u2019s boats carry the island mail, and the Post pays on the nail. Postmaster Ivy Penhallow and Brennock have shared a pot of tea every Friday for thirty years.", vars: [] }, { key: "31cec57aec33", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 286, lines: [286], tpl: "CHAINS.guild[2]", card: "Quota Forms", field: "title", text: "Quota Forms", vars: [] }, { key: "7b4a78de7980", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 286, lines: [286], tpl: "CHAINS.guild[2]", card: "Quota Forms", field: "why", text: "The Ministry has changed the quotas again. Fetch the new forms before the auction, or nobody sells a thing.", vars: [] }, { key: "b16d2237286d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 287, lines: [287], tpl: "CHAINS.guild[2]", card: "Quota Forms", field: "details", text: "The Ministry sets the Guild\u2019s quotas from an office a long way from the sea. Brennock calls them landlubbers; the Permanent Secretary calls the Guild difficult. Neither is wrong, and neither can do without the other.", vars: [] }, { key: "7882211776b4", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 291, lines: [291], tpl: "CHAINS.guild[3]", card: "Only Fish", field: "title", text: "Only Fish", vars: [] }, { key: "d5c61484794f", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 291, lines: [291], tpl: "CHAINS.guild[3]", card: "Only Fish", field: "why", text: "The Family wanted their crates on our boats. We said no. Take the honest catch to market and let them see it.", vars: [] }, { key: "db7eb91ad907", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 292, lines: [292], tpl: "CHAINS.guild[3]", card: "Only Fish", field: "details", text: "The Family has been offering skippers money to carry crates with no fish in them. The Guild refused, and the market has been quiet since. Brennock wants the morning catch landed in plain sight: only fish, every box.", vars: [] }, { key: "b78371bf5b39", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 296, lines: [296], tpl: "CHAINS.guild[4]", card: "Corran\u2019s Boat", field: "title", text: "Corran\u2019s Boat", vars: [] }, { key: "163733d67f01", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 296, lines: [296], tpl: "CHAINS.guild[4]", card: "Corran\u2019s Boat", field: "why", text: "Mother Corran wants to meet you at the harbour at dawn. Then her catch to market, the first of the season.", vars: [] }, { key: "a623f5364d85", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 297, lines: [297], tpl: "CHAINS.guild[4]", card: "Corran\u2019s Boat", field: "details", text: "Corran lost her husband\u2019s boat, the Merrow, in the great storm forty years ago, and has taken the first boat out every season since, in case. She gave the Guild its lifeboats and the rule that nobody is left in the water. Meet her at dawn, and carry her catch in.", vars: [] }, { key: "1188b86d8a3a", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 303, lines: [303], tpl: "CHAINS.press[0]", card: "Hot off the Press", field: "title", text: "Hot off the Press", vars: [] }, { key: "6f5debbc4dc6", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 303, lines: [303], tpl: "CHAINS.press[0]", card: "Hot off the Press", field: "why", text: "Editor Cass Marlow will see you in the newsroom. Then bundles for the newsstands, still warm.", vars: [] }, { key: "168f4b440fbf", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 304, lines: [304], tpl: "CHAINS.press[0]", card: "Hot off the Press", field: "details", text: "The Press: the papers, the radio and television stations, the printworks and the newsstands. They sell the truth, mostly, and a scoop is worth more than gold. The paper belongs to Proprietor Hollis Vane, whom nobody in the newsroom has ever seen smile.", vars: [] }, { key: "42afe6154153", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 308, lines: [308], tpl: "CHAINS.press[1]", card: "Behind the Gates", field: "title", text: "Behind the Gates", vars: [] }, { key: "05bf3a4ae812", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 308, lines: [308], tpl: "CHAINS.press[1]", card: "Behind the Gates", field: "why", text: "A star\u2019s secret, out of the mansion and into the newsroom. The Studios will be livid.", vars: [] }, { key: "7683dbb7282e", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 309, lines: [309], tpl: "CHAINS.press[1]", card: "Behind the Gates", field: "details", text: "The Press and the Studios are rivals: the papers sell scandal and the studio sells dreams. Every scoop you carry for Marlow is one that Producer Dash Delano wanted buried. He will remember.", vars: [] }, { key: "666deda1cbf7", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 313, lines: [313], tpl: "CHAINS.press[2]", card: "Leaked", field: "title", text: "Leaked", vars: [] }, { key: "8b19380965c8", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 313, lines: [313], tpl: "CHAINS.press[2]", card: "Leaked", field: "why", text: "A memo from the town hall that somebody left on a photocopier. Get it here before anybody notices.", vars: [] }, { key: "643e8fc43d2a", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 314, lines: [314], tpl: "CHAINS.press[2]", card: "Leaked", field: "details", text: "The Ministry and the Press are neither friends nor enemies. The Ministry needs the papers to say what it wants, and the papers need the Ministry to keep secrets worth printing. The Permanent Secretary reads Marlow first every morning.", vars: [] }, { key: "c3c63a28cbe4", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 318, lines: [318], tpl: "CHAINS.press[3]", card: "The Bent Sergeant", field: "title", text: "The Bent Sergeant", vars: [] }, { key: "ee1a53b71a3b", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 318, lines: [318], tpl: "CHAINS.press[3]", card: "The Bent Sergeant", field: "why", text: "There is a bent officer at the station and Marlow has the tape. Get it to the studio for the six o\u2019clock news.", vars: [] }, { key: "457908575ec9", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 319, lines: [319], tpl: "CHAINS.press[3]", card: "The Bent Sergeant", field: "details", text: "The Press and the Constabulary circle each other: Chief Hale wants the story kept quiet until the trial, and Marlow wants it out tonight. Somebody is going to be angry whatever you do.", vars: [] }, { key: "49400eb26212", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 323, lines: [323], tpl: "CHAINS.press[4]", card: "Stop the Presses", field: "title", text: "Stop the Presses", vars: [] }, { key: "846df84426ea", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 323, lines: [323], tpl: "CHAINS.press[4]", card: "Stop the Presses", field: "why", text: "Vane means to sell the paper tonight. Meet him at the printworks at midnight, then get the true edition out.", vars: [] }, { key: "780f18cc8aa9", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 324, lines: [324], tpl: "CHAINS.press[4]", card: "Stop the Presses", field: "details", text: "Hollis Vane built the paper from one printing press and a grudge. Lark Pictures has offered him a fortune for it, to bury every story it doesn\u2019t like. Marlow has written the front page that says so. Meet Vane at midnight: he will print it, if you can deliver it by morning.", vars: [] }, { key: "cd2717ef6ae9", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 359, lines: [359], tpl: "riseStep", card: null, field: "caption", text: "{icon} {name} \xB7 {x1} of {RISE_STEPS}", vars: [{ label: "icon", expr: "d.icon" }, { label: "name", expr: "d.name" }, { label: "x1", expr: "i + 1" }, { label: "RISE_STEPS", expr: "RISE_STEPS" }] }, { key: "fc5a7b1ff58d", group: "Faction rises", file: "src/game/missions/factionChains.ts", line: 369, lines: [369], tpl: "RISE_ARCS", card: "Meet {x1}", field: "title", text: "Meet {x1}", vars: [{ label: "x1", expr: "FACTIONS[f].name" }] }, { key: "c7eb7562aaab", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 78, lines: [78], tpl: "STEPS[0]", card: "Fresh Catch", field: "title", text: "Fresh Catch", vars: [] }, { key: "5812a42b8531", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 78, lines: [78], tpl: "STEPS[0]", card: "Fresh Catch", field: "why", text: "Skipper Holt has a crate for the aquarium and no van. Westerly, dropping. Bring it in before the weather turns.", vars: [] }, { key: "83ee7ff3ee04", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 79, lines: [79], tpl: "STEPS[0]", card: "Fresh Catch", field: "details", text: "Maren Holt gives the weather before hello. One fish in the crate has no eyes at all, and Dr Lind at the aquarium will want to see it.", vars: [] }, { key: "bcb5461cd9b6", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 80, lines: [80], tpl: "STEPS[0]", card: "Fresh Catch", field: "caption", text: "An odd catch", vars: [] }, { key: "c8623f758d83", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 91, lines: [91], tpl: "STEPS[1]", card: "Night Nets", field: "title", text: "Night Nets", vars: [] }, { key: "90e1f92a952f", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 91, lines: [91], tpl: "STEPS[1]", card: "Night Nets", field: "why", text: "Something came up in Holt\u2019s nets that she won\u2019t talk about by day. Meet her on the harbour after dark.", vars: [] }, { key: "4faf2185f3e1", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 92, lines: [92], tpl: "STEPS[1]", card: "Night Nets", field: "details", text: "\u201CNortherly, rising,\u201D she says, and opens a crate. A flat tile, the size of a hand, warm as tea and humming faintly.", vars: [] }, { key: "b82f1946ffa7", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 93, lines: [93], tpl: "STEPS[1]", card: "Night Nets", field: "caption", text: "Holt\u2019s boat", vars: [] }, { key: "8ab7477dd5a4", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 95, lines: [95], tpl: "STEPS[1]", card: "Night Nets", field: "line", text: "A warm tile came up in Holt\u2019s net off the cape. It hums.", vars: [] }, { key: "00c520758268", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 108, lines: [108], tpl: "STEPS[2]", card: "Specimen", field: "title", text: "Specimen", vars: [] }, { key: "ffe215385b35", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 108, lines: [108], tpl: "STEPS[2]", card: "Specimen", field: "why", text: "Holt wants the tile off her boat. Dr Lind at the aquarium wants it under a microscope. Take it over.", vars: [] }, { key: "e3e8944591a2", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 109, lines: [109], tpl: "STEPS[2]", card: "Specimen", field: "details", text: "Teodor Lind has a Latin name for everything that swims. For the tile he has a shrug. \u201CNot glass. Not stone. Not anything,\u201D he says. \u201CYet.\u201D", vars: [] }, { key: "e668a2ed8b83", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 110, lines: [110], tpl: "STEPS[2]", card: "Specimen", field: "caption", text: "The humming tile", vars: [] }, { key: "5a06fc67f81e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 124, lines: [124], tpl: "STEPS[3]", card: "Keeper\u2019s Light", field: "title", text: "Keeper\u2019s Light", vars: [] }, { key: "fbbe5b81d979", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 124, lines: [124], tpl: "STEPS[3]", card: "Keeper\u2019s Light", field: "why", text: "Lind has heard of a keeper out on the coast who has seen it all before. Nobody gives directions. Find the light.", vars: [] }, { key: "e3ba990ccba9", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 125, lines: [125], tpl: "STEPS[3]", card: "Keeper\u2019s Light", field: "details", text: "It stands {x1} of the aquarium, about {away} tiles off, where the land runs out. \u201CForty years. Sevens. Nobody came up the hill till you,\u201D says Fen.", vars: [{ label: "x1", expr: "bearing(a, l)" }, { label: "away", expr: "away" }] }, { key: "c9a7ef99ece4", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 126, lines: [126], tpl: "STEPS[3]", card: "Keeper\u2019s Light", field: "caption", text: "The lighthouse", vars: [] }, { key: "5b7102cb07f5", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 128, lines: [128], tpl: "STEPS[3]", card: "Keeper\u2019s Light", field: "line", text: "Old Fen the keeper has seen lights under the sea for forty years. Always in sevens.", vars: [] }, { key: "a4161f227b8e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 143, lines: [143], tpl: "STEPS[4]", card: "Coastguard Call", field: "title", text: "Coastguard Call", vars: [] }, { key: "3663a0876831", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 143, lines: [143], tpl: "STEPS[4]", card: "Coastguard Call", field: "why", text: "The coastguard looks after the keeper. CPO Rook needs his supplies run up to the light, and a word with you after.", vars: [] }, { key: "970994c17f3e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 144, lines: [144], tpl: "STEPS[4]", card: "Coastguard Call", field: "details", text: "\u201CLogged. Noted. Next,\u201D says Rook. The coastguard has had eleven reports of lights under the water this month, and filed every one.", vars: [] }, { key: "ffc471852a5c", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 145, lines: [145], tpl: "STEPS[4]", card: "Coastguard Call", field: "caption", text: "The coastguard", vars: [] }, { key: "d0716c6633ac", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 163, lines: [163], tpl: "STEPS[5]", card: "Salvage Rights", field: "title", text: "Salvage Rights", vars: [] }, { key: "e95112347b39", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 163, lines: [163], tpl: "STEPS[5]", card: "Salvage Rights", field: "why", text: "A salvage firm called Greywater is after a wreck that shed the same tiles. First there sets the buoy. Beat their van.", vars: [] }, { key: "a55f1f6a62e7", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 164, lines: [164], tpl: "STEPS[5]", card: "Salvage Rights", field: "details", text: "Nobody has ever seen a Greywater driver\u2019s face. Rook will be watching from the coastguard launch.", vars: [] }, { key: "3f75a152701a", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 165, lines: [165], tpl: "STEPS[5]", card: "Salvage Rights", field: "caption", text: "Greywater\u2019s van", vars: [] }, { key: "a81eedd06c26", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 172, lines: [172], tpl: "STEPS[6].beat", card: null, field: "line", text: "The men on the rig say its legs hum at night. The coastguard has logged it.", vars: [] }, { key: "a7aa5792763e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 183, lines: [183], tpl: "STEPS[6]", card: "Rig Supply", field: "title", text: "Rig Supply", vars: [] }, { key: "3b934754e938", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 183, lines: [183], tpl: "STEPS[6]", card: "Rig Supply", field: "why", text: "The rig needs spares and the coastguard wants an excuse to be out there. Two loads to the rig, and keep your ears open.", vars: [] }, { key: "d11c55dc746c", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 184, lines: [184], tpl: "STEPS[6]", card: "Rig Supply", field: "details", text: "The rig crew don\u2019t like the nights any more. They say the legs sing. Rook has filed it under weather.", vars: [] }, { key: "1fd3cf1f8a43", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 185, lines: [185], tpl: "STEPS[6]", card: "Rig Supply", field: "caption", text: "The rig", vars: [] }, { key: "5369339ae8bf", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 192, lines: [192], tpl: "STEPS[7].beat", card: null, field: "line", text: "A film crew is shooting at the harbour with the free-diver Lio Brandt.", vars: [] }, { key: "4ab48e615a52", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 200, lines: [200], tpl: "STEPS[7]", card: "Screen Test", field: "title", text: "Screen Test", vars: [] }, { key: "c6d341fb065b", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 200, lines: [200], tpl: "STEPS[7]", card: "Screen Test", field: "why", text: "Starlight Pictures is making a film about the hum. Rafe Castellanos needs his crew and his diver at the harbour.", vars: [] }, { key: "57e1c627e336", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 201, lines: [201], tpl: "STEPS[7]", card: "Screen Test", field: "details", text: "Rafe frames everything with his hands. His star, the free-diver Lio Brandt, says \u201Cdown\u201D the way other people say \u201Chome\u201D.", vars: [] }, { key: "a7da01536f60", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 202, lines: [202], tpl: "STEPS[7]", card: "Screen Test", field: "caption", text: "Starlight Pictures", vars: [] }, { key: "1a8a8dd757f8", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 210, lines: [210], tpl: "STEPS[8].beat", card: null, field: "line", text: "The power failed with the tide. The lamps came back flickering in sevens.", vars: [] }, { key: "f17c2461f137", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 221, lines: [221], tpl: "STEPS[8]", card: "Blackout", field: "title", text: "Blackout", vars: [] }, { key: "b404d32621df", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 221, lines: [221], tpl: "STEPS[8]", card: "Blackout", field: "why", text: "The hum has blown the town\u2019s substation. Rook needs new switchgear brought in before the hospital\u2019s generator runs dry.", vars: [] }, { key: "62e855011d8b", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 222, lines: [222], tpl: "STEPS[8]", card: "Blackout", field: "details", text: "Every lamp in the harbour town went out at once, and came back flickering. Seven, a pause, seven. Rook has stopped filing it under weather.", vars: [] }, { key: "f9b2cd63c737", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 223, lines: [223], tpl: "STEPS[8]", card: "Blackout", field: "caption", text: "Switchgear", vars: [] }, { key: "f38a8d6d689d", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 230, lines: [230], tpl: "STEPS[9].beat", card: null, field: "line", text: "A warship has dropped anchor off the harbour. The Navy calls it an exercise.", vars: [] }, { key: "f0d316a98d46", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 241, lines: [241], tpl: "STEPS[9]", card: "Dry Dock", field: "title", text: "Dry Dock", vars: [] }, { key: "90a3e37a39c5", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 242, lines: [242], tpl: "STEPS[9]", card: "Dry Dock", field: "why", text: "Rook has raised the wreck Greywater wanted. Tow it into the dry dock with two vehicles, where the Navy can look at it.", vars: [] }, { key: "ce105fa3b34a", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 243, lines: [243], tpl: "STEPS[9]", card: "Dry Dock", field: "why", text: "Rook has raised the wreck Greywater wanted and cut it up. Truck it to the dry dock, where the Navy can look at it.", vars: [] }, { key: "d1f818569509", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 244, lines: [244], tpl: "STEPS[9]", card: "Dry Dock", field: "details", text: "Its hull is studded with the tiles, hundreds of them, all humming. The Navy asked for it by name.", vars: [] }, { key: "49cf09562ec2", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 245, lines: [245], tpl: "STEPS[9]", card: "Dry Dock", field: "caption", text: "The wreck", vars: [] }, { key: "d68892a17cbc", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 257, lines: [257], tpl: "STEPS[10]", card: "Flag Officer", field: "title", text: "Flag Officer", vars: [] }, { key: "c35029f44ecc", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 257, lines: [257], tpl: "STEPS[10]", card: "Flag Officer", field: "why", text: "The Navy\u2019s Commander Adeyemi wants to meet the haulier who found the wreck. At the dock, after dark, quietly.", vars: [] }, { key: "51e4406ec9ff", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 258, lines: [258], tpl: "STEPS[10]", card: "Flag Officer", field: "details", text: "\u201CI need boats. You have boats. Good.\u201D Yusra Adeyemi hands you the keys to a hovercraft. \u201CIt doesn\u2019t care what it\u2019s on. You\u2019ll see why.\u201D", vars: [] }, { key: "9eb0bd0a4456", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 259, lines: [259], tpl: "STEPS[10]", card: "Flag Officer", field: "caption", text: "The Navy\u2019s Skimmer", vars: [] }, { key: "38713c70e9a9", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 280, lines: [280], tpl: "clue", card: null, field: "line", text: "Greywater Salvage are stealing tiles off the wreck and taking them to a yard inland.", vars: [] }, { key: "45aeaa9f2969", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 284, lines: [284, 291], tpl: "STEPS[11]", card: "Night Van", field: "title", text: "Night Van", vars: [] }, { key: "47eb4c749558", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 284, lines: [284], tpl: "STEPS[11]", card: "Night Van", field: "why", text: "Greywater\u2019s van has been at the harbour every night, and it always heads {x1}. Find where it unloads.", vars: [{ label: "x1", expr: "bearing(shed, yardSpot)" }] }, { key: "11f2b346363d", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 285, lines: [285], tpl: "STEPS[11]", card: "Night Van", field: "details", text: "About {x1} tiles {x2} of the harbour, says Rook. A yard, he thinks. \u201CLogged. Noted. Found.\u201D", vars: [{ label: "x1", expr: "Math.round(Math.hypot(yardSpot.x - shed.x, yardSpot.z - shed.z) / 10) * 10" }, { label: "x2", expr: "bearing(shed, yardSpot)" }] }, { key: "6fa5a52b5c75", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 286, lines: [286, 293], tpl: "STEPS[11]", card: "Night Van", field: "caption", text: "The grey van", vars: [] }, { key: "7f352f58a54a", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 291, lines: [291], tpl: "STEPS[11]", card: "Night Van", field: "why", text: "Greywater\u2019s van has been at the harbour every night. Rook wants to know where it takes what it loads. Follow it.", vars: [] }, { key: "d4202def2615", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 292, lines: [292], tpl: "STEPS[11]", card: "Night Van", field: "details", text: "\u201CLogged,\u201D says Rook. \u201CNoted. Followed.\u201D The van is grey, and in no hurry.", vars: [] }, { key: "31c6dd4c36bb", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 301, lines: [301], tpl: "STEPS[12].beat", card: null, field: "line", text: "The coastguard has impounded a salvage firm\u2019s vans and what they carried.", vars: [] }, { key: "6acdbe93c29c", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 312, lines: [312], tpl: "STEPS[12]", card: "Hot Pursuit", field: "title", text: "Hot Pursuit", vars: [] }, { key: "be2fd981a4e3", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 312, lines: [312], tpl: "STEPS[12]", card: "Hot Pursuit", field: "why", text: "Greywater is running the tiles out of the county. Get to the border before their van does and the coastguard does the rest.", vars: [] }, { key: "295f608f222e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 313, lines: [313], tpl: "STEPS[12]", card: "Hot Pursuit", field: "details", text: "Rook has rung ahead. Greywater\u2019s best driver is at the wheel and has never lost a race. Shortcuts are allowed.", vars: [] }, { key: "a5f45e9bca28", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 314, lines: [314], tpl: "STEPS[12]", card: "Hot Pursuit", field: "caption", text: "Greywater\u2019s fastest", vars: [] }, { key: "8f6abb4e28f3", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 321, lines: [321], tpl: "STEPS[13].beat", card: null, field: "line", text: "An astronomer inland says the moon has been signalling for weeks.", vars: [] }, { key: "2646bb4326ce", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 329, lines: [329], tpl: "STEPS[13]", card: "Dead Calm", field: "title", text: "Dead Calm", vars: [] }, { key: "907e522204a4", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 329, lines: [329], tpl: "STEPS[13]", card: "Dead Calm", field: "why", text: "The fish have left the bay, every one. Holt\u2019s town has nothing to eat. Three loads of relief food to the harbour.", vars: [] }, { key: "5868c0eda168", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 330, lines: [330], tpl: "STEPS[13]", card: "Dead Calm", field: "details", text: "\u201CFlat calm,\u201D says Holt. \u201CNo wind, no fish, no gulls. Forty years and I have never seen the sea this quiet.\u201D", vars: [] }, { key: "3767288d7698", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 331, lines: [331], tpl: "STEPS[13]", card: "Dead Calm", field: "caption", text: "Relief food", vars: [] }, { key: "a529e7c80318", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 338, lines: [338], tpl: "STEPS[14].beat", card: null, field: "line", text: "Lio Brandt says she will dive to the hum, with a crew and whatever floats.", vars: [] }, { key: "61b96adec2a3", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 349, lines: [349], tpl: "STEPS[14]", card: "Premiere", field: "title", text: "Premiere", vars: [] }, { key: "7e62a70238fe", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 349, lines: [349], tpl: "STEPS[14]", card: "Premiere", field: "why", text: "Starlight\u2019s film is finished and the marquee reads THE HUM. Rafe needs his stars on the red carpet tonight.", vars: [] }, { key: "df2e19d40bc3", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 350, lines: [350], tpl: "STEPS[14]", card: "Premiere", field: "details", text: "On the carpet, Lio Brandt tells the cameras she will dive to the hum \u201Cwith a crew and whatever floats\u201D. The Navy declines to comment.", vars: [] }, { key: "996aa8e6e6e3", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 351, lines: [351], tpl: "STEPS[14]", card: "Premiere", field: "caption", text: "The premiere", vars: [] }, { key: "1c1898a96fd2", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 363, lines: [363], tpl: "STEPS[15]", card: "The Hum Answers", field: "title", text: "The Hum Answers", vars: [] }, { key: "f9c789f6f55f", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 363, lines: [363], tpl: "STEPS[15]", card: "The Hum Answers", field: "why", text: "Fen will flash the lamp in sevens at midnight, and Lind wants to see what answers. Be at the light and keep still.", vars: [] }, { key: "23130f81000e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 364, lines: [364], tpl: "STEPS[15]", card: "The Hum Answers", field: "details", text: "Fen works the lamp by hand. Out past the cape the sea lights up from underneath and answers in eights. \u201CIt\u2019s a door,\u201D says Lind. \u201CIt isn\u2019t ours.\u201D", vars: [] }, { key: "bd15aaac997e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 365, lines: [365], tpl: "STEPS[15]", card: "The Hum Answers", field: "caption", text: "The light", vars: [] }, { key: "61dc63663f63", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 367, lines: [367], tpl: "STEPS[15]", card: "The Hum Answers", field: "line", text: "Fen flashed sevens and the sea answered in eights. Lind says it is a door, and it isn\u2019t ours.", vars: [] }, { key: "f0f349c20ae6", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 382, lines: [382], tpl: "STEPS[16]", card: "Hull Plates", field: "title", text: "Hull Plates", vars: [] }, { key: "24861908e1a5", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 382, lines: [382], tpl: "STEPS[16]", card: "Hull Plates", field: "why", text: "The Navy is building something in the dry dock and needs a fleet that keeps quiet. Three loads of hull plate, and two vehicles.", vars: [] }, { key: "6acff4e1d73e", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 383, lines: [383], tpl: "STEPS[16]", card: "Hull Plates", field: "details", text: "\u201CPlates, sphere, people,\u201D says Adeyemi. \u201CThis is plates.\u201D They are curved, and very thick.", vars: [] }, { key: "6c89e94b8455", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 384, lines: [384], tpl: "STEPS[16]", card: "Hull Plates", field: "caption", text: "The dry dock", vars: [] }, { key: "bf1684e8110c", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 385, lines: [385], tpl: "STEPS[16]", card: "Hull Plates", field: "label", text: "Own 2 vehicles", vars: [] }, { key: "101df60ef51b", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 391, lines: [391], tpl: "STEPS[17].beat", card: null, field: "line", text: "A submarine is taking shape in the dry dock. The Navy calls it a research vessel.", vars: [] }, { key: "94766043bb78", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 401, lines: [401], tpl: "STEPS[17]", card: "Pressure Test", field: "title", text: "Pressure Test", vars: [] }, { key: "02f2ba12d4b0", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 402, lines: [402], tpl: "STEPS[17]", card: "Pressure Test", field: "why", text: "The pressure sphere is the heart of the boat, and too heavy for two. Tow it to the dock with three vehicles on the ropes.", vars: [] }, { key: "b8fcd8774d9b", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 403, lines: [403], tpl: "STEPS[17]", card: "Pressure Test", field: "why", text: "The pressure sphere is the heart of the boat. Bring it to the dock on the biggest truck you have, and gently.", vars: [] }, { key: "38f624af5d17", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 404, lines: [404], tpl: "STEPS[17]", card: "Pressure Test", field: "details", text: "Thick steel and four small windows. \u201CThe crew lives in that,\u201D says Adeyemi. \u201CAll the way down.\u201D", vars: [] }, { key: "bd69c91399c2", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 405, lines: [405], tpl: "STEPS[17]", card: "Pressure Test", field: "caption", text: "The pressure sphere", vars: [] }, { key: "2ced19cbdb40", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 420, lines: [420], tpl: "STEPS[18]", card: "Dive Crew", field: "title", text: "Dive Crew", vars: [] }, { key: "5e6b2b5d4567", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 420, lines: [420], tpl: "STEPS[18]", card: "Dive Crew", field: "why", text: "The Navy needs haulage on the bottom of the ocean, and Adeyemi wants your fleet. Show her four vehicles. Bring the divers.", vars: [] }, { key: "fc5ec4871134", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 421, lines: [421], tpl: "STEPS[18]", card: "Dive Crew", field: "details", text: "\u201CNot the surface. The bottom,\u201D says Adeyemi. \u201CThe door is on the floor of the trench, and doors need hauliers. Show me a fleet that floats.\u201D", vars: [] }, { key: "e7189f08e288", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 422, lines: [422], tpl: "STEPS[18]", card: "Dive Crew", field: "caption", text: "The submarine", vars: [] }, { key: "9d2837daee9f", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 423, lines: [423], tpl: "STEPS[18]", card: "Dive Crew", field: "label", text: "Own 4 vehicles", vars: [] }, { key: "ad8b882d4286", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 423, lines: [423], tpl: "STEPS[18]", card: "Dive Crew", field: "label", text: "Hire 2 drivers out", vars: [] }, { key: "0deb5e4769a6", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 430, lines: [430], tpl: "STEPS[19].beat", card: null, field: "line", text: "The Navy says she dives for real at the spring tide, and the fleet is going with her.", vars: [] }, { key: "2ae1e86c95db", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 437, lines: [437], tpl: "STEPS[19]", card: "Sea Trials", field: "title", text: "Sea Trials", vars: [] }, { key: "ddd5f1d40d61", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 437, lines: [437], tpl: "STEPS[19]", card: "Sea Trials", field: "why", text: "Dawn, the dock flooding, everyone on the quay. Lead three of your vehicles to the dock in formation and hold while she floats.", vars: [] }, { key: "e7f66a3bf29c", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 438, lines: [438], tpl: "STEPS[19]", card: "Sea Trials", field: "details", text: "Fen on the quay, Lind, Holt, Rook, Lio on camera. The gate opens and the water under her lights up in sevens. \u201CShe dives at the spring tide,\u201D says Adeyemi. \u201CBring everything that floats.\u201D", vars: [] }, { key: "8daecad91a09", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 439, lines: [439], tpl: "STEPS[19]", card: "Sea Trials", field: "caption", text: "Sea trials", vars: [] }, { key: "25d7a8d1d0b4", group: "Story chains", file: "src/game/missions/story/deep.ts", line: 442, lines: [442], tpl: "STEPS[19]", card: "Sea Trials", field: "line", text: "The submarine dives at the spring tide. The Navy wants your fleet on the ocean floor.", vars: [] }, { key: "eb1443a5dad6", group: "Story chains", file: "src/game/missions/story/kit.ts", line: 296, lines: [296], tpl: "race", card: null, field: "label", text: "Meet {short} at the start", vars: [{ label: "short", expr: "r.short" }] }, { key: "b9a58f21a0e8", group: "Story chains", file: "src/game/missions/story/kit.ts", line: 296, lines: [296], tpl: "race", card: null, field: "label", text: "Meet at the start line", vars: [] }, { key: "f5af6181ef49", group: "Story chains", file: "src/game/missions/story/kit.ts", line: 325, lines: [325], tpl: "tow", card: null, field: "label", text: "Bring {min} vehicles", vars: [{ label: "min", expr: "min" }] }, { key: "000171d8393a", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 119, lines: [119], tpl: "STEPS[0]", card: "Lens Delivery", field: "title", text: "Lens Delivery", vars: [] }, { key: "bebd251793de", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 119, lines: [119], tpl: "STEPS[0]", card: "Lens Delivery", field: "why", text: "Professor Halloway\u2019s new lens is waiting to be collected. Bring it up to her before the sky clears tonight.", vars: [] }, { key: "e5126929eb64", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 120, lines: [120], tpl: "STEPS[0]", card: "Lens Delivery", field: "details", text: "Wren Halloway has watched the same patch of the moon for thirty years, and something new has turned up on it. \u201CProbably nothing,\u201D she says. \u201CA hair over nothing.\u201D", vars: [] }, { key: "b35c7b78711d", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 132, lines: [132], tpl: "STEPS[1]", card: "Night Shift", field: "title", text: "Night Shift", vars: [] }, { key: "97a2fcdc3ca3", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 132, lines: [132], tpl: "STEPS[1]", card: "Night Shift", field: "why", text: "Halloway wants a second pair of eyes. Come up after dark and look through the telescope. She won\u2019t say at what.", vars: [] }, { key: "3ae7c0afb385", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 133, lines: [133], tpl: "STEPS[1]", card: "Night Shift", field: "details", text: "She has shown nobody else. \u201CTell me if you see it too,\u201D she says, \u201Cand then tell me I\u2019m not mad.\u201D", vars: [] }, { key: "458bf35de8bc", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 136, lines: [136], tpl: "STEPS[1]", card: "Night Shift", field: "line", text: "A light on the moon blinks in sevens: seven flashes, a pause, seven more. Every night.", vars: [] }, { key: "c298dfa59615", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 142, lines: [142], tpl: "STEPS[2].beat", card: null, field: "line", text: "A local stargazer says the moon is blinking at us. Film at six.", vars: [] }, { key: "29e00956413e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 150, lines: [150], tpl: "STEPS[2]", card: "Hot Tape", field: "title", text: "Hot Tape", vars: [] }, { key: "f61114a126a6", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 150, lines: [150], tpl: "STEPS[2]", card: "Hot Tape", field: "why", text: "Halloway has filmed the light. Take her tapes to the newsroom, where a reporter called Cass Marlow is waiting.", vars: [] }, { key: "c88bf748680f", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 151, lines: [151], tpl: "STEPS[2]", card: "Hot Tape", field: "details", text: "Cass Marlow speaks in headlines. \u201CSTARGAZER SAYS MOON BLINKS,\u201D she says when you ring. \u201CBring me the tapes.\u201D", vars: [] }, { key: "53b0ab78e05e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 152, lines: [152], tpl: "STEPS[2]", card: "Hot Tape", field: "caption", text: "Observation tapes", vars: [] }, { key: "4b6f8ee57d71", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 163, lines: [163], tpl: "reward[1]", card: null, field: "line", text: "The grey car that watches the observatory belongs to the Army.", vars: [] }, { key: "4c3a0a5ae027", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 167, lines: [167, 175], tpl: "STEPS[3]", card: "Grey Grandee", field: "title", text: "Grey Grandee", vars: [] }, { key: "c55a6cd5167d", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 167, lines: [167], tpl: "STEPS[3]", card: "Grey Grandee", field: "why", text: "A grey limousine has sat outside the observatory all week. Cass wants to know whose it is. Follow it, unseen.", vars: [] }, { key: "db59c01be8dc", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 168, lines: [168], tpl: "STEPS[3]", card: "Grey Grandee", field: "details", text: "\u201CNOBODY PARKS A LIMO OUTSIDE A TELESCOPE,\u201D says Cass. Find out where it goes.", vars: [] }, { key: "c0a790958f76", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 169, lines: [169, 177], tpl: "STEPS[3]", card: "Grey Grandee", field: "caption", text: "The grey Grandee", vars: [] }, { key: "ae0f061062b2", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 176, lines: [176], tpl: "STEPS[3]", card: "Grey Grandee", field: "details", text: "Cass lost it on the road out. It went {x1}, about {x2} tiles. Somewhere official.", vars: [{ label: "x1", expr: "bearing(o, b)" }, { label: "x2", expr: "Math.round(Math.hypot(b.x - o.x, b.z - o.z) / 10) * 10" }] }, { key: "5d70318c8ebe", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 193, lines: [193], tpl: "STEPS[4]", card: "Rations Run", field: "title", text: "Rations Run", vars: [] }, { key: "f82acf08155f", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 193, lines: [193], tpl: "STEPS[4]", card: "Rations Run", field: "why", text: "The base needs feeding and asks no questions of whoever does it. Two loads of mess supplies, and keep it to yourself.", vars: [] }, { key: "af1b72732e11", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 194, lines: [194], tpl: "STEPS[4]", card: "Rations Run", field: "details", text: "Colonel Ada Strand never asks a question. \u201CYou\u2019ll not talk about the sky,\u201D she says. \u201CYou\u2019ll be paid on time.\u201D The base stays lit all night now.", vars: [] }, { key: "4017efc499c1", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 195, lines: [195, 327], tpl: "STEPS[4]", card: "Rations Run", field: "caption", text: "The Army", vars: [] }, { key: "982fce7eec81", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 211, lines: [211], tpl: "STEPS[5]", card: "Old Ears", field: "title", text: "Old Ears", vars: [] }, { key: "43489d72c52a", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 211, lines: [211], tpl: "STEPS[5]", card: "Old Ears", field: "why", text: "Halloway needs a receiver, and there is one in a satellite that fell out of the sky years ago. Find it and bring it back.", vars: [] }, { key: "30a93a05b5df", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 212, lines: [212], tpl: "STEPS[5]", card: "Old Ears", field: "details", text: "It came down {x1} of the observatory, about {away} tiles off. The Army\u2019s grey car was there before you; mind they left the receiver.", vars: [{ label: "x1", expr: "bearing(o, sat)" }, { label: "away", expr: "away" }] }, { key: "ecb0e10cfa1a", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 213, lines: [213], tpl: "STEPS[5]", card: "Old Ears", field: "caption", text: "A satellite receiver", vars: [] }, { key: "a24eb0ebbd6e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 218, lines: [218], tpl: "STEPS[5]", card: "Old Ears", field: "line", text: "The light on the moon rides a carrier wave. It is a transmission. Somebody is sending.", vars: [] }, { key: "8a566c3f4a76", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 236, lines: [236], tpl: "STEPS[6]", card: "First to the Pad", field: "title", text: "First to the Pad", vars: [] }, { key: "1ae153c16839", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 236, lines: [236], tpl: "STEPS[6]", card: "First to the Pad", field: "why", text: "The spaceport wants one haulier, and the first to its gate gets the contract. Oswin Pike wants it too. Beat him there.", vars: [] }, { key: "4f85797e1a4e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 237, lines: [237], tpl: "STEPS[6]", card: "First to the Pad", field: "details", text: "Ilse Varga of Kestrel Launch will be at the gate with a stopwatch. Pike smiles at you the way a tow truck smiles at a ditch.", vars: [] }, { key: "97cb082df884", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 238, lines: [238], tpl: "STEPS[6]", card: "First to the Pad", field: "caption", text: "Pike\u2019s Pip", vars: [] }, { key: "63f2964aec66", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 245, lines: [245], tpl: "STEPS[7].beat", card: null, field: "line", text: "A commercial payload, says Kestrel Launch. Nobody will say what.", vars: [] }, { key: "f1ccd5ebac70", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 253, lines: [253], tpl: "STEPS[7]", card: "Pad Supplies", field: "title", text: "Pad Supplies", vars: [] }, { key: "f7ccdc93df18", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 253, lines: [253], tpl: "STEPS[7]", card: "Pad Supplies", field: "why", text: "Kestrel Launch has a client and a date. Ilse Varga needs three loads of supplies at the pad, and no questions asked.", vars: [] }, { key: "edbd39fe6580", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 254, lines: [254], tpl: "STEPS[7]", card: "Pad Supplies", field: "details", text: "Ilse Varga counts down in ordinary sentences. \u201CThree things. Valves, seals, cable. In that order.\u201D The pad\u2019s floodlights are on all night now.", vars: [] }, { key: "132a6c93e81c", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 255, lines: [255, 414], tpl: "STEPS[7]", card: "Pad Supplies", field: "caption", text: "The launch pad", vars: [] }, { key: "12aff4c96bfd", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 263, lines: [263], tpl: "STEPS[8].beat", card: null, field: "line", text: "The pop star told a full stadium she is going to space.", vars: [] }, { key: "3ce8b4703dc8", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 273, lines: [273], tpl: "STEPS[8]", card: "Star Power", field: "title", text: "Star Power", vars: [] }, { key: "a999ec42e351", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 273, lines: [273], tpl: "STEPS[8]", card: "Star Power", field: "why", text: "Pop star Juno Vale plays the stadium tonight and wants the haulier the spaceport uses. Bring her gear, then her.", vars: [] }, { key: "9be31bffcc76", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 274, lines: [274], tpl: "STEPS[8]", card: "Star Power", field: "details", text: "Juno thinks everything is iconic. \u201CA haulier for a rocket. That\u2019s iconic.\u201D She has heard about the light on the moon, like everybody.", vars: [] }, { key: "7c9a3bfb8dbb", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 275, lines: [275], tpl: "STEPS[8]", card: "Star Power", field: "caption", text: "Tonight\u2019s show", vars: [] }, { key: "be0c146e8842", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 283, lines: [283], tpl: "STEPS[9].beat", card: null, field: "line", text: "Soldiers turned reporters away from the launch pad today. Kestrel says it was not asked.", vars: [] }, { key: "e51ba135fccb", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 291, lines: [291], tpl: "STEPS[9]", card: "Press Pool", field: "title", text: "Press Pool", vars: [] }, { key: "792f8a3c63cf", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 291, lines: [291], tpl: "STEPS[9]", card: "Press Pool", field: "why", text: "The Army has shut the spaceport to the press. Cass has a crew and a way in round the back. Get them to the pad.", vars: [] }, { key: "7361bc572eb6", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 292, lines: [292], tpl: "STEPS[9]", card: "Press Pool", field: "details", text: "\u201CARMY SHUTS GATE,\u201D says Cass. \u201CHAULIER OPENS IT.\u201D Ilse Varga will let a contractor\u2019s vehicle through. She and the colonel are no longer speaking.", vars: [] }, { key: "31ff8b731dd6", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 293, lines: [293], tpl: "STEPS[9]", card: "Press Pool", field: "caption", text: "Channel 6", vars: [] }, { key: "52a22ebda516", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 307, lines: [307], tpl: "STEPS[10]", card: "Quiet Word", field: "title", text: "Quiet Word", vars: [] }, { key: "c5f6db015be3", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 307, lines: [307], tpl: "STEPS[10]", card: "Quiet Word", field: "why", text: "Colonel Strand wants a word, somewhere quiet, after dark. She says to come alone. She doesn\u2019t say why.", vars: [] }, { key: "fa1d0a5b849c", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 308, lines: [308], tpl: "STEPS[10]", card: "Quiet Word", field: "details", text: "\u201CWe sent it sevens on Tuesday. It sent eight on Wednesday.\u201D The colonel tells you things; she never asks. \u201CYou\u2019ll want something that doesn\u2019t mind a dirt road.\u201D", vars: [] }, { key: "1d47d7c1075a", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 309, lines: [309], tpl: "STEPS[10]", card: "Quiet Word", field: "caption", text: "The colonel\u2019s Badger", vars: [] }, { key: "51a93a9ab5dc", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 317, lines: [317], tpl: "STEPS[11].beat", card: null, field: "line", text: "Tanks stand at the spaceport tonight. Kestrel calls them guests. Loud guests.", vars: [] }, { key: "b3e52c84bc84", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 325, lines: [325], tpl: "STEPS[11]", card: "Convoy", field: "title", text: "Convoy", vars: [] }, { key: "4bfd77993d52", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 325, lines: [325], tpl: "STEPS[11]", card: "Convoy", field: "why", text: "The Army is moving onto the pad. Two loads of soldiers from the base, before the press can count them.", vars: [] }, { key: "470ef3fab4c4", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 326, lines: [326], tpl: "STEPS[11]", card: "Convoy", field: "details", text: "\u201CYou\u2019ll not stop on the way,\u201D says the colonel. Ilse Varga will have to share her pad with tanks, and one of them is now yours: the Army is short of drivers.", vars: [] }, { key: "cabc308627b6", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 344, lines: [344], tpl: "STEPS[12]", card: "The Big Dish", field: "title", text: "The Big Dish", vars: [] }, { key: "897c28527a72", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 345, lines: [345], tpl: "STEPS[12]", card: "The Big Dish", field: "why", text: "Halloway and the colonel agree on one thing: the pad needs a big dish to answer with. Tow it in with two vehicles.", vars: [] }, { key: "81b9fffa74c8", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 346, lines: [346], tpl: "STEPS[12]", card: "The Big Dish", field: "why", text: "Halloway and the colonel agree on one thing: the pad needs a big dish to answer with. Truck it over in sections.", vars: [] }, { key: "2e1b97599f99", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 347, lines: [347], tpl: "STEPS[12]", card: "The Big Dish", field: "details", text: "It came off the observatory on a low trailer. \u201CGently,\u201D says Halloway. \u201CIt\u2019s a hair over priceless.\u201D", vars: [] }, { key: "dcd44ae5de78", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 348, lines: [348], tpl: "STEPS[12]", card: "The Big Dish", field: "caption", text: "The radio dish", vars: [] }, { key: "69783c6412a6", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 355, lines: [355], tpl: "STEPS[13].beat", card: null, field: "line", text: "Out on the coast, the boats report a hum under the water. Nobody is listening.", vars: [] }, { key: "e708e47b21b9", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 364, lines: [364], tpl: "STEPS[13]", card: "Dead Air", field: "title", text: "Dead Air", vars: [] }, { key: "519679104f06", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 364, lines: [364], tpl: "STEPS[13]", card: "Dead Air", field: "why", text: "The signal is drowning out every radio in the county. Cass knows an engineer who can tune it out. Fetch them.", vars: [] }, { key: "2bbed0b11f54", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 365, lines: [365], tpl: "STEPS[13]", card: "Dead Air", field: "details", text: "\u201CSIGNAL JAMS RADIO,\u201D says Cass, on the one channel left. The engineer says the signal isn\u2019t only coming from the sky.", vars: [] }, { key: "a9036703349e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 366, lines: [366], tpl: "STEPS[13]", card: "Dead Air", field: "caption", text: "The radio station", vars: [] }, { key: "3ade0408b6c0", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 373, lines: [373], tpl: "STEPS[14].beat", card: null, field: "line", text: "Fans camp at the spaceport gate as the pop star refuses to step down from the flight.", vars: [] }, { key: "a018a9c6c4cd", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 381, lines: [381], tpl: "STEPS[14]", card: "Fan Mail", field: "title", text: "Fan Mail", vars: [] }, { key: "17f78a20bb85", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 381, lines: [381], tpl: "STEPS[14]", card: "Fan Mail", field: "why", text: "Juno\u2019s manager wants her off the flight. Juno wants her fans at the gate. Two loads of them, banners and all.", vars: [] }, { key: "2afb4441af37", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 382, lines: [382], tpl: "STEPS[14]", card: "Fan Mail", field: "details", text: "\u201CIf they won\u2019t let me go up,\u201D says Juno, \u201Cthey can tell my fans why.\u201D The fans bring tents.", vars: [] }, { key: "63b5d30bbefd", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 383, lines: [383], tpl: "STEPS[14]", card: "Fan Mail", field: "caption", text: "The fan club", vars: [] }, { key: "06996fd5e91d", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 394, lines: [394], tpl: "STEPS[15]", card: "The Answer", field: "title", text: "The Answer", vars: [] }, { key: "7847251cd140", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 394, lines: [394], tpl: "STEPS[15]", card: "The Answer", field: "why", text: "The dish is up. At midnight Halloway will answer the signal, and she wants a witness. Be at the pad and keep still.", vars: [] }, { key: "72e4f655cc88", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 395, lines: [395], tpl: "STEPS[15]", card: "The Answer", field: "details", text: "The dish turns on its own. \u201CIt\u2019s counting in our count now,\u201D Halloway whispers. \u201CIt\u2019s sending a place. The far side. Nobody\u2019s been.\u201D", vars: [] }, { key: "c0def1ded6c9", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 396, lines: [396], tpl: "STEPS[15]", card: "The Answer", field: "caption", text: "The dish, listening", vars: [] }, { key: "838536b05e81", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 398, lines: [398], tpl: "STEPS[15]", card: "The Answer", field: "line", text: "The signal sent coordinates on the far side of the moon. Halloway said it: it isn\u2019t ours.", vars: [] }, { key: "3a8a198c474c", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 412, lines: [412], tpl: "STEPS[16]", card: "Rocket Parts", field: "title", text: "Rocket Parts", vars: [] }, { key: "bdf12862c617", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 412, lines: [412], tpl: "STEPS[16]", card: "Rocket Parts", field: "why", text: "Kestrel has a new client, and it isn\u2019t a satellite. Three rocket sections to the pad, and bring a second vehicle in.", vars: [] }, { key: "8616bc57e772", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 413, lines: [413], tpl: "STEPS[16]", card: "Rocket Parts", field: "details", text: "\u201CThree things,\u201D says Ilse Varga. \u201CParts, fuel, people. This is parts.\u201D The sections are bigger than anything Kestrel has flown.", vars: [] }, { key: "1ea4dc185785", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 415, lines: [415], tpl: "STEPS[16]", card: "Rocket Parts", field: "label", text: "Own 2 vehicles", vars: [] }, { key: "3b502ff2fc77", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 421, lines: [421], tpl: "STEPS[17].beat", card: null, field: "line", text: "Kestrel names a launch window. The Army says it will not stand in the way.", vars: [] }, { key: "64d1d102984c", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 429, lines: [429], tpl: "STEPS[17]", card: "Hot Load", field: "title", text: "Hot Load", vars: [] }, { key: "14b33352740a", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 429, lines: [429], tpl: "STEPS[17]", card: "Hot Load", field: "why", text: "The fuel boils off if it waits. Get the cryo tanks to the pad before they warm up. The Army will clear the road.", vars: [] }, { key: "0dd919477a01", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 430, lines: [430], tpl: "STEPS[17]", card: "Hot Load", field: "details", text: "\u201CYou\u2019ll not stop,\u201D says the colonel. \u201CWe\u2019ll not stop you.\u201D Two of her Badgers will be on the road. This is fuel.", vars: [] }, { key: "21644461e8c0", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 431, lines: [431], tpl: "STEPS[17]", card: "Hot Load", field: "caption", text: "Cryo fuel", vars: [] }, { key: "f94d1c387124", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 445, lines: [445], tpl: "STEPS[18]", card: "Flight Crew", field: "title", text: "Flight Crew", vars: [] }, { key: "7598d2c241a9", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 445, lines: [445], tpl: "STEPS[18]", card: "Flight Crew", field: "why", text: "Kestrel needs haulage on the Moon\u2019s surface, and Varga wants your fleet. Show her four vehicles that work. Bring the crew.", vars: [] }, { key: "6f4ab6c2191f", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 446, lines: [446], tpl: "STEPS[18]", card: "Flight Crew", field: "details", text: "\u201CRovers, cargo, people, on the surface,\u201D says Ilse Varga. \u201CThe Moon\u2019s surface. The coordinates are a landing site, and a landing site needs a haulier.\u201D", vars: [] }, { key: "bfd2bcd872d8", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 447, lines: [447], tpl: "STEPS[18]", card: "Flight Crew", field: "caption", text: "Kestrel Launch", vars: [] }, { key: "ee9aaee2f8e6", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 448, lines: [448], tpl: "STEPS[18]", card: "Flight Crew", field: "label", text: "Own 4 vehicles", vars: [] }, { key: "117847f07d9e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 448, lines: [448], tpl: "STEPS[18]", card: "Flight Crew", field: "label", text: "Hire 2 drivers out", vars: [] }, { key: "f1dc8bb0be1e", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 455, lines: [455], tpl: "STEPS[19].beat", card: null, field: "line", text: "Kestrel Launch says the window opens at the next full moon, and the fleet is going.", vars: [] }, { key: "247f77d30a8b", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 463, lines: [463], tpl: "STEPS[19]", card: "T-Minus", field: "title", text: "T-Minus", vars: [] }, { key: "a468b6ce132d", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 463, lines: [463], tpl: "STEPS[19]", card: "T-Minus", field: "why", text: "Dawn, the count running, everyone watching. Lead three of your vehicles to the pad in formation and hold for the count.", vars: [] }, { key: "156124d10c4d", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 464, lines: [464], tpl: "STEPS[19]", card: "T-Minus", field: "details", text: "Halloway, the colonel, Cass on air, Juno in a flight suit and Pike in his lorry. The signal pulses once. \u201CThe window opens at the next full moon,\u201D says Varga. \u201CBring the fleet.\u201D", vars: [] }, { key: "5897f1db7eab", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 465, lines: [465], tpl: "STEPS[19]", card: "T-Minus", field: "caption", text: "The moon rocket", vars: [] }, { key: "ac5916c2bad0", group: "Story chains", file: "src/game/missions/story/moon.ts", line: 467, lines: [467], tpl: "STEPS[19]", card: "T-Minus", field: "line", text: "The launch window opens at the next full moon. Kestrel wants your fleet on the Moon.", vars: [] }, { key: "35dde787f17b", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 474, lines: [474], tpl: "bind \u203A treasure", card: null, field: "title", text: "Hidden Treasure", vars: [] }, { key: "1c8524cc33bd", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 478, lines: [478], tpl: "bind \u203A treasure", card: null, field: "why", text: "Find it from the clue alone. No marker until you\u2019re within {HIDDEN_REVEAL} tiles; then stop in the ring.", vars: [{ label: "HIDDEN_REVEAL", expr: "HIDDEN_REVEAL" }] }, { key: "7a48cec2608a", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 479, lines: [479], tpl: "bind \u203A treasure", card: null, field: "details", text: "{x1} at {name} in {t.t.name} says it\u2019s {say}. Nobody will point the way: not the map, not the tracker.", vars: [{ label: "x1", expr: "this.visit?.teller.name ?? 'Someone'" }, { label: "name", expr: "t.name" }, { label: "t.t.name", expr: "t.t.name" }, { label: "say", expr: "spot.say" }] }, { key: "f1c3030ebd01", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 480, lines: [480], tpl: "bind \u203A treasure", card: null, field: "caption", text: "A chest, if it\u2019s there", vars: [] }, { key: "19986c7e2bb7", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 481, lines: [481], tpl: "bind \u203A treasure", card: null, field: "label", text: "Find the spot", vars: [] }, { key: "f5dee149afc2", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 489, lines: [489], tpl: "bind \u203A smuggle", card: "A Quiet Load", field: "title", text: "A Quiet Load", vars: [] }, { key: "faa7fc69899a", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 503, lines: [503], tpl: "bind \u203A explore", card: null, field: "title", text: "See {name}", vars: [{ label: "name", expr: "spot.at.name" }] }, { key: "fbe69dd22abf", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 503, lines: [503], tpl: "bind \u203A explore", card: null, field: "title", text: "The Big City", vars: [] }, { key: "b5c89f546f58", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 503, lines: [503], tpl: "bind \u203A explore", card: null, field: "title", text: "Another Country", vars: [] }, { key: "a61e62d78708", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 503, lines: [503], tpl: "bind \u203A explore", card: null, field: "title", text: "Off the Map", vars: [] }, { key: "51c3331d5c59", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 507, lines: [507], tpl: "bind \u203A explore", card: null, field: "why", text: "Somewhere you\u2019ve never been: {name}, {x1} tiles {x2}. Get there and look.", vars: [{ label: "name", expr: "spot.at.name" }, { label: "x1", expr: "Math.round(d)" }, { label: "x2", expr: "way(t.x, t.z, spot.at.x, spot.at.z)" }] }, { key: "5ea8b91f18af", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 508, lines: [508], tpl: "bind \u203A explore", card: null, field: "details", text: "Told at {name} in {t.t.name}. Your map is dark over it: go and see it for yourself.", vars: [{ label: "name", expr: "t.name" }, { label: "t.t.name", expr: "t.t.name" }] }, { key: "dd7b1fa0829e", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 510, lines: [510, 527], tpl: "bind \u203A explore", card: null, field: "label", text: "Reach {x1}", vars: [{ label: "x1", expr: "short(spot.at.name)" }] }, { key: "3a5127e848ae", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 520, lines: [520], tpl: "bind \u203A timed", card: null, field: "title", text: "Against the Clock", vars: [] }, { key: "f29ef6fae591", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 524, lines: [524], tpl: "bind \u203A timed", card: null, field: "why", text: "{name}, {x1} tiles {x2}, inside {x3}. Stop in the ring there.", vars: [{ label: "name", expr: "to.name" }, { label: "x1", expr: "Math.round(d)" }, { label: "x2", expr: "way(t.x, t.z, to.x, to.z)" }, { label: "x3", expr: "minutes(secs)" }] }, { key: "064dd1d2d821", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 525, lines: [525], tpl: "bind \u203A timed", card: null, field: "details", text: "Asked at {name} in {t.t.name}. The clock started when you said yes.", vars: [{ label: "name", expr: "t.name" }, { label: "t.t.name", expr: "t.t.name" }] }, { key: "f6044cbafea2", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 526, lines: [526], tpl: "bind \u203A timed", card: null, field: "caption", text: "An envelope that can\u2019t be late", vars: [] }, { key: "e3d5309b4f5d", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 537, lines: [537], tpl: "bind \u203A fetch", card: null, field: "title", text: "There and Back", vars: [] }, { key: "cfbf62441ffb", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 541, lines: [541], tpl: "bind \u203A fetch", card: null, field: "why", text: "Collect it at {name} and bring it back to {t.name}\u2019s door, all inside {x1}.", vars: [{ label: "name", expr: "from.name" }, { label: "t.name", expr: "t.name" }, { label: "x1", expr: "minutes(secs)" }] }, { key: "d4c5bace6d68", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 542, lines: [542], tpl: "bind \u203A fetch", card: null, field: "details", text: "Asked at {name} in {t.t.name}. Stop in the ring at {from.name}, then in the tavern\u2019s ring and press Space.", vars: [{ label: "name", expr: "t.name" }, { label: "t.t.name", expr: "t.t.name" }, { label: "from.name", expr: "from.name" }] }, { key: "79b3579c4152", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 543, lines: [543], tpl: "bind \u203A fetch", card: null, field: "caption", text: "Handle with care", vars: [] }, { key: "dd9dbb775592", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 545, lines: [545], tpl: "bind \u203A fetch", card: null, field: "label", text: "Collect at {x1}", vars: [{ label: "x1", expr: "short(from.name)" }] }, { key: "a15476a56d8a", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 546, lines: [546], tpl: "bind \u203A fetch", card: null, field: "label", text: "Back to {x1}", vars: [{ label: "x1", expr: "short(t.name)" }] }, { key: "c112d8ab2446", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 563, lines: [563], tpl: "want.wrecker", card: null, field: "label", text: "a tow truck", vars: [] }, { key: "4b9f933083d0", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 564, lines: [564], tpl: "want.ambulance", card: null, field: "label", text: "an ambulance", vars: [] }, { key: "f25ba8466a8e", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 565, lines: [565], tpl: "want.bus", card: null, field: "label", text: "a bus", vars: [] }, { key: "3ffd193ba620", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 566, lines: [566], tpl: "want.limo", card: null, field: "label", text: "a limousine", vars: [] }, { key: "b877393dd9ae", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 567, lines: [567], tpl: "want.heli", card: null, field: "label", text: "a helicopter", vars: [] }, { key: "fbaa94da465e", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 568, lines: [568], tpl: "want.boat", card: null, field: "label", text: "a boat", vars: [] }, { key: "69b3d626b203", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 569, lines: [569], tpl: "want.quad", card: null, field: "label", text: "a quad bike", vars: [] }, { key: "0405778f15a8", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 638, lines: [638], tpl: "rideJob", card: null, field: "title", text: "A Particular Job", vars: [] }, { key: "551240d87ddb", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 644, lines: [644], tpl: "rideJob", card: null, field: "why", text: "It has to be done in {label}: {x1}{x2}. Stop in the ring each time.", vars: [{ label: "label", expr: "r.label" }, { label: "x1", expr: "a!.name" }, { label: "x2", expr: "b ? `, then ${b.name}` : ''" }] }, { key: "9f8e0801bb08", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 645, lines: [645], tpl: "rideJob", card: null, field: "details", text: "Asked at {name} in {t.t.name}. Arriving in anything but {label} doesn\u2019t count, so fetch one first if you have to.", vars: [{ label: "name", expr: "t.name" }, { label: "t.t.name", expr: "t.t.name" }, { label: "label", expr: "r.label" }] }, { key: "d0ea810ac2a7", group: "Tavern jobs", file: "src/game/tavern/tavern.ts", line: 646, lines: [646], tpl: "rideJob", card: null, field: "caption", text: "Only {label} will do", vars: [{ label: "label", expr: "r.label" }] }] };

// server/missionText.ts
var CATALOG = missionText_default;
var BY_KEY = new Map(CATALOG.entries.map((e) => [e.key, e]));
var DOC = "missiontext";
var MAX_TEXT = 2e3;
var slots = (s) => [...s.matchAll(/\{([\w.$]+)\}/g)].map((m) => m[1]);
var cached = null;
async function editsOf(store2) {
  if (!cached) cached = await store2.get(DOC) ?? { at: 0, edits: {} };
  return cached;
}
async function publicEdits(store2) {
  const d = await editsOf(store2);
  return { at: d.at, edits: Object.values(d.edits).map((e) => ({ from: e.from, to: e.to })) };
}
async function setEdit(store2, key, to) {
  const d = await editsOf(store2);
  const e = BY_KEY.get(key);
  const had = d.edits[key];
  if (!e && !had) return "No such line.";
  const from = e?.text ?? had.from;
  const now = Date.now();
  if (to === null || to === from) {
    delete d.edits[key];
  } else {
    to = to.replace(/\r\n?/g, "\n").trim();
    if (!to) return "A line can\u2019t be empty: use Revert to put the original back.";
    if (to.length > MAX_TEXT) return `Too long (${to.length} characters).`;
    const known = new Set(slots(from));
    const bad = slots(to).filter((s) => !known.has(s));
    if (bad.length) return `Unknown slot${bad.length > 1 ? "s" : ""}: ${bad.map((s) => `{${s}}`).join(", ")}. Use only the slots the original has.`;
    if (/[{}]/.test(to.replace(/\{([\w.$]+)\}/g, ""))) return "A stray { or } \u2014 braces are only for slots like {name}.";
    const hist = had ? [...had.hist, { to: had.to, at: had.at }].slice(-10) : [];
    d.edits[key] = { from, to, file: e?.file ?? had.file, tpl: e?.tpl ?? had.tpl, card: e?.card ?? had?.card ?? null, field: e?.field ?? had.field, at: now, hist };
  }
  d.at = now;
  await store2.set(DOC, d);
  return null;
}

// server/admin.ts
var ADMIN_PATH = "/dev";
var ADMIN_NAME = (process.env.ADMIN_NAME || "Jack").toLowerCase();
var ADMIN_PIN = process.env.ADMIN_PIN || "";
var SESSION_MS = 12 * 36e5;
var REPORT_MAX = 6e5;
var FEEDBACK_MAX = 2e4;
var GID = /^[a-z0-9-]{8,48}$/i;
var sessions = /* @__PURE__ */ new Map();
var hits = /* @__PURE__ */ new Map();
function limited(key, max, windowMs) {
  const now = Date.now();
  let h = hits.get(key);
  if (!h || h.until < now) {
    h = { n: 0, until: now + windowMs };
    hits.set(key, h);
  }
  if (hits.size > 2e4) {
    for (const [k, v] of hits) if (v.until < now) hits.delete(k);
  }
  return ++h.n > max;
}
var failsAll = [];
var ipOf = (req) => String(req.headers["x-forwarded-for"] ?? req.socket.remoteAddress ?? "").split(",")[0].trim();
function readBody(req, max) {
  return new Promise((resolve2) => {
    let size = 0;
    const parts = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > max) {
        resolve2(null);
        req.destroy();
        return;
      }
      parts.push(c);
    });
    req.on("end", () => resolve2(Buffer.concat(parts).toString("utf8")));
    req.on("error", () => resolve2(null));
  });
}
var OPEN = { "access-control-allow-origin": "*", "access-control-allow-methods": "POST, OPTIONS", "access-control-allow-headers": "content-type" };
function json(res, code, body, head = {}) {
  res.writeHead(code, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...head }).end(JSON.stringify(body));
}
var str = (v, max) => (typeof v === "string" ? v : v == null ? "" : String(v)).slice(0, max);
function cookieOf(req, name) {
  const m = new RegExp(`(?:^|;\\s*)${name}=([^;]+)`).exec(String(req.headers.cookie ?? ""));
  return m ? m[1] : "";
}
function signedIn(req) {
  const t = cookieOf(req, "tfa");
  if (!t) return false;
  const until = sessions.get(t);
  if (!until || until < Date.now()) {
    sessions.delete(t);
    return false;
  }
  return true;
}
var same = (a, b) => {
  const ha = createHash("sha256").update(a).digest(), hb = createHash("sha256").update(b).digest();
  return timingSafeEqual(ha, hb);
};
function summary(gid, r) {
  const missions = Object.values(r.missions ?? {});
  const fleet = r.fleet;
  const player = r.player;
  const money = r.money ?? {};
  const metrics = r.metrics ?? {};
  const driven = r.driven ?? {};
  const delivered = r.delivered ?? {};
  const explored = r.explored ?? {};
  const perf = r.perf ?? {};
  return {
    gid,
    first: r.first,
    last: r.last,
    reports: r.reports,
    fleet: fleet?.name ?? "",
    player: player?.name ?? "",
    online: r.online ?? null,
    seed: r.seed,
    start: r.start,
    cell: r.cell,
    played: r.played,
    day: r.day,
    cash: money.cash ?? 0,
    earned: money.earned ?? 0,
    worth: metrics.worth ?? null,
    vehicles: Array.isArray(r.vehicles) ? r.vehicles.length : 0,
    youTiles: driven.you ?? 0,
    hiredTiles: driven.hired ?? 0,
    delivered: (delivered.you ?? 0) + (delivered.fleet ?? 0),
    started: missions.length,
    done: missions.filter((m) => m.state === "done").length,
    buildings: Array.isArray(r.buildings) ? r.buildings.length : 0,
    cells: explored.cells ?? 0,
    deeds: Array.isArray(r.deeds) ? r.deeds.length : 0,
    where: r.where,
    gen: r.gen,
    fps: perf.frames?.avgFps ?? null,
    low1: perf.frames?.low1 ?? null,
    wall: perf.frames?.seconds ?? null,
    quality: perf.quality?.label ?? null,
    device: perf.device ? perf.device.mobile ? "mobile" : perf.device.touchPoints && !perf.device.fine ? "touch" : "desktop" : null,
    gpu: perf.device?.gpu ?? null,
    errors: Array.isArray(perf.errors) ? perf.errors.length : null,
    firstKey: perf.funnel?.firstDriveKeyAt ?? null,
    firstMove: perf.funnel?.firstMoveAt ?? null,
    welcome: perf.funnel ? perf.funnel.welcomeSeen ? perf.funnel.welcomeClosedAt == null ? "open" : perf.funnel.welcomeSeconds ?? 0 : "none" : null,
    why: perf.why ?? null
  };
}
async function adminHttp(req, res, path, store2, hooks) {
  const ip = ipOf(req);
  if (path === "/api/visit") {
    if (req.method === "OPTIONS") {
      res.writeHead(204, OPEN).end();
      return true;
    }
    if (req.method !== "POST") {
      json(res, 405, { ok: false }, OPEN);
      return true;
    }
    if (limited(`v|${ip}`, 12, 6e5)) {
      json(res, 429, { ok: false }, OPEN);
      return true;
    }
    let m = {};
    try {
      m = JSON.parse(await readBody(req, 4e3) ?? "");
    } catch {
      json(res, 400, { ok: false }, OPEN);
      return true;
    }
    const kind = str(m.kind, 20);
    const card = kind === "card-drawn" || kind === "card-looked" || kind === "card-phone" || kind.startsWith("gl-");
    if (kind !== "page" && kind !== "mobile" && kind !== "anyway" && !card) {
      json(res, 400, { ok: false }, OPEN);
      return true;
    }
    const now = Date.now();
    const day = new Date(now).toISOString().slice(0, 10);
    const doc = await store2.get(`visits:${day}`) ?? { day, n: {}, rows: [] };
    doc.n[kind] = (doc.n[kind] ?? 0) + 1;
    if (kind === "card-drawn" && typeof m.ms === "number" && m.ms > 0 && m.ms < 6e5) doc.n["card-drawms"] = (doc.n["card-drawms"] ?? 0) + Math.round(m.ms);
    if (card) {
      await store2.set(`visits:${day}`, doc);
      json(res, 200, { ok: true }, OPEN);
      return true;
    }
    doc.rows.push({
      t: now,
      kind,
      ua: str(m.ua, 300),
      screen: Array.isArray(m.screen) ? m.screen.slice(0, 2) : null,
      touch: typeof m.touch === "number" ? m.touch : null,
      coarse: !!m.coarse,
      fine: !!m.fine,
      where: str(m.where, 80),
      ref: str(m.ref, 160)
    });
    if (doc.rows.length > 300) doc.rows.splice(0, doc.rows.length - 300);
    await store2.set(`visits:${day}`, doc);
    json(res, 200, { ok: true }, OPEN);
    return true;
  }
  if (path === "/api/report" || path === "/api/feedback") {
    if (req.method === "OPTIONS") {
      res.writeHead(204, OPEN).end();
      return true;
    }
    if (req.method !== "POST") {
      json(res, 405, { ok: false }, OPEN);
      return true;
    }
    const report = path === "/api/report";
    if (limited(`${report ? "r" : "f"}|${ip}`, report ? 30 : 6, report ? 6e4 : 6e5)) {
      json(res, 429, { ok: false }, OPEN);
      return true;
    }
    const text = await readBody(req, report ? REPORT_MAX : FEEDBACK_MAX);
    let m;
    try {
      m = JSON.parse(text ?? "");
      if (!m || typeof m !== "object" || Array.isArray(m)) throw new Error("not an object");
    } catch {
      json(res, 400, { ok: false }, OPEN);
      return true;
    }
    const now = Date.now();
    if (report) {
      const gid = str(m.gid, 64);
      if (!GID.test(gid)) {
        json(res, 400, { ok: false }, OPEN);
        return true;
      }
      const had = await store2.get(`report:${gid}`);
      const missions = { ...had?.missions ?? {} };
      if (Array.isArray(m.missions)) {
        for (const q of m.missions.slice(0, 500)) if (q && typeof q === "object" && "id" in q) missions[String(q.id)] = q;
      }
      const kept = { ...m, missions, first: had?.first ?? now, last: now, reports: (had?.reports ?? 0) + 1 };
      await store2.set(`report:${gid}`, kept);
    } else {
      const body = str(m.text, 5e3).trim();
      if (!body) {
        json(res, 400, { ok: false }, OPEN);
        return true;
      }
      const doc = {
        at: now,
        subject: str(m.subject, 120).trim() || "(no subject)",
        text: body,
        gid: GID.test(str(m.gid, 64)) ? str(m.gid, 64) : null,
        player: str(m.player, 40) || null,
        fleet: str(m.fleet, 40) || null,
        online: str(m.online, 40) || null,
        seed: str(m.seed, 80) || null,
        cell: Array.isArray(m.cell) ? m.cell.slice(0, 2) : null,
        day: typeof m.day === "number" ? m.day : null,
        gen: typeof m.gen === "number" ? m.gen : null,
        where: str(m.where, 80) || null
      };
      await store2.set(`feedback:${now.toString(36)}-${randomBytes(4).toString("hex")}`, doc);
    }
    json(res, 200, { ok: true }, OPEN);
    return true;
  }
  if (path === "/api/missiontext") {
    if (req.method === "OPTIONS") {
      res.writeHead(204, OPEN).end();
      return true;
    }
    if (limited(`mt|${ip}`, 60, 6e5)) {
      json(res, 429, { ok: false }, OPEN);
      return true;
    }
    json(res, 200, { ok: true, ...await publicEdits(store2) }, OPEN);
    return true;
  }
  if (path === `${ADMIN_PATH}/missions`) {
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "x-robots-tag": "noindex, nofollow",
      "x-frame-options": "DENY",
      "referrer-policy": "no-referrer"
    }).end(MISSIONS_PAGE);
    return true;
  }
  if (path === ADMIN_PATH || path === `${ADMIN_PATH}/`) {
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "x-robots-tag": "noindex, nofollow",
      "x-frame-options": "DENY",
      "referrer-policy": "no-referrer"
    }).end(ADMIN_PAGE);
    return true;
  }
  if (!path.startsWith("/api/admin/")) return false;
  const what = path.slice("/api/admin/".length);
  if (what === "login") {
    if (req.method !== "POST") {
      json(res, 405, { ok: false });
      return true;
    }
    const now = Date.now();
    failsAll = failsAll.filter((t) => t > now - 36e5);
    if (!ADMIN_PIN) {
      json(res, 503, { ok: false, why: "Signing in is not set up: add ADMIN_PIN to the server\u2019s variables." });
      return true;
    }
    const ipFails = hits.get(`login|${ip}`);
    if (ipFails && ipFails.until > now && ipFails.n >= 5 || failsAll.length >= 30) {
      json(res, 429, { ok: false, why: "Too many tries. Wait a while and try again." });
      return true;
    }
    let m = {};
    try {
      m = JSON.parse(await readBody(req, 2e3) ?? "");
    } catch {
    }
    const ok = same(str(m.name, 40).trim().toLowerCase(), ADMIN_NAME) && same(str(m.pin, 40).trim(), ADMIN_PIN);
    if (!ok) {
      limited(`login|${ip}`, 5, 15 * 6e4);
      failsAll.push(now);
      json(res, 401, { ok: false, why: "That name and PIN don\u2019t match." });
      return true;
    }
    const token = randomBytes(32).toString("hex");
    sessions.set(token, now + SESSION_MS);
    const secure = String(req.headers["x-forwarded-proto"] ?? "").includes("https") ? "; Secure" : "";
    json(res, 200, { ok: true }, { "set-cookie": `tfa=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${SESSION_MS / 1e3}${secure}` });
    return true;
  }
  if (what === "logout") {
    sessions.delete(cookieOf(req, "tfa"));
    json(res, 200, { ok: true }, { "set-cookie": "tfa=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0" });
    return true;
  }
  if (!signedIn(req)) {
    json(res, 401, { ok: false });
    return true;
  }
  if (what === "games") {
    const all = await store2.list("report:");
    json(res, 200, { ok: true, games: all.map((e) => summary(e.key.slice("report:".length), e.value)).sort((a, b) => Number(b.last) - Number(a.last)) });
    return true;
  }
  if (what === "game") {
    const gid = new URL(req.url ?? "", "http://x").searchParams.get("gid") ?? "";
    if (!GID.test(gid)) {
      json(res, 400, { ok: false });
      return true;
    }
    const r = await store2.get(`report:${gid}`);
    if (!r) {
      json(res, 404, { ok: false });
      return true;
    }
    const fb = (await store2.list("feedback:")).map((e) => e.value).filter((f) => f.gid === gid);
    json(res, 200, { ok: true, game: r, feedback: fb });
    return true;
  }
  if (what === "visits") {
    const all = await store2.list("visits:");
    json(res, 200, { ok: true, days: all.map((e) => e.value).sort((a, b) => String(b.day).localeCompare(String(a.day))).slice(0, 30) });
    return true;
  }
  if (what === "feedback") {
    const all = await store2.list("feedback:");
    const list = all.map((e) => ({ id: e.key.slice("feedback:".length), ...e.value }));
    json(res, 200, { ok: true, feedback: list.sort((a, b) => b.at - a.at) });
    return true;
  }
  if (what === "missions") {
    json(res, 200, { ok: true, catalog: CATALOG, edits: (await editsOf(store2)).edits });
    return true;
  }
  if (what === "missions/edit") {
    if (req.method !== "POST") {
      json(res, 405, { ok: false });
      return true;
    }
    let m = {};
    try {
      m = JSON.parse(await readBody(req, 2e4) ?? "");
    } catch {
      json(res, 400, { ok: false, why: "Bad request." });
      return true;
    }
    const why = await setEdit(store2, str(m.key, 40), typeof m.to === "string" ? m.to : null);
    json(res, why ? 400 : 200, why ? { ok: false, why } : { ok: true, edit: (await editsOf(store2)).edits[str(m.key, 40)] ?? null });
    return true;
  }
  if (what === "pictures") {
    json(res, 200, { ok: true, pictures: hooks?.pictures ? await hooks.pictures() : [] });
    return true;
  }
  if (what === "picture") {
    if (req.method !== "POST") {
      json(res, 405, { ok: false });
      return true;
    }
    let m = {};
    try {
      m = JSON.parse(await readBody(req, 2e3) ?? "");
    } catch {
    }
    json(res, 200, { ok: !!hooks?.picture && await hooks.picture(str(m.id, 12), m.show === true) });
    return true;
  }
  if (what === "clearname") {
    if (req.method !== "POST") {
      json(res, 405, { ok: false });
      return true;
    }
    let m = {};
    try {
      m = JSON.parse(await readBody(req, 2e3) ?? "");
    } catch {
    }
    json(res, 200, { ok: !!hooks && await hooks.clearName(str(m.k, 80)) });
    return true;
  }
  json(res, 404, { ok: false });
  return true;
}

// shared/protocol.ts
var PROTOCOL = 3;
var CHAT_CELLS = 8;
var SPAWN_REACH = 64;
var SPAWN_NEAR = 8;
var SEE_CELLS = 2;
var MOVE_HZ = 10;
var CHAT_MAX = 240;
var HOURS_PER_SECOND = 1 / 25;
var isPid = (s) => s.startsWith("p:");
var cellKey = (cx, cz) => `${cx},${cz}`;
var cellOfKey = (k) => k.split("#")[0].split(",").map(Number);
var FRIENDS_MAX = 100;
var GUEST = {
  /** Guests at once, beside the players (MAX_GUESTS on the server overrides it), and from one address. */
  max: 300,
  perIp: 3,
  /** Cells a guest may hold. */
  cells: 25,
  /** A guest that has said nothing for this long (ms) is let go: a game whose tab is open says `watch` every minute. */
  idle: 6e5,
  says: ["sub", "watch", "bye"]
};

// shared/marks.ts
var MARKS = {
  /** Signs one player may have standing at once; a new one past it takes the oldest down. */
  signsMax: 12,
  /** Signs one county may hold. */
  signsPerCell: 80,
  /** Hearts that carve a sign in stone; it stays then. */
  carveUp: 8,
  /** Hearts kept per sign. */
  votesMax: 64,
  /** A drift shorter than this (tiles slid) isn't a record anywhere. */
  driftMin: 8,
  /** Points in a drift's painted line, at most. */
  driftPts: 48,
  /** The longest a slide can honestly be in one go, tiles: anything more is refused. */
  driftMaxTiles: 400,
  /** Ghost samples a second, and the most a lap may hold (five minutes). */
  ghostHz: 5,
  ghostMax: 1500,
  /** The quickest a lap of any circuit can be, seconds. */
  lapMin: 12,
  /** Stamps a guestbook keeps; fleets it remembers as having signed. */
  guestKeep: 40,
  guestFleets: 200,
  /** Tiles from an HQ's ground a visitor counts within; seconds stopped there before the book is signed. */
  guestReach: 5,
  guestStay: 3,
  /** Hand-overs kept per town. */
  holdersKeep: 12,
  /** Votes that give an unnamed work the name a player suggested. */
  suggestVotes: 5,
  /** Suggestions a work keeps. */
  suggestMax: 4,
  /** Real milliseconds in a week of picks. */
  week: 7 * 864e5
};
var STAMPS = ["\u{1F44B}", "\u{1F389}", "\u2764\uFE0F", "\u{1F69A}", "\u2B50", "\u2615"];
var SIGN_PHRASES = [
  "{} ahead",
  "Beware of {}",
  "Try {}",
  "Shortcut to {}",
  "Good spot for {}",
  "Need {}",
  "Behold, {}!",
  "Could this be {}?",
  "Fast way to {}",
  "If only I had {}\u2026",
  "Praise the {}",
  "No {} here",
  "{} this way",
  "Watch for {}",
  "Nice {}",
  "Time for {}",
  "Don\u2019t forget {}",
  "Visit {}",
  "Brake for {}",
  "Floor it for {}"
];
var SIGN_WORDS = [
  ["Places", ["a town", "the port", "the airport", "a dealer", "a shop", "the tavern", "an HQ", "a bridge", "a tunnel", "the circuit", "the ferry", "the coast", "the hills", "a farm"]],
  ["Road", ["a shortcut", "a dead end", "a sharp bend", "a jump", "a toll", "traffic", "roadworks", "a straight", "a hairpin", "mud", "deep water", "a cliff"]],
  ["Driving", ["a drift", "nitrous", "the handbrake", "speed", "patience", "caution", "a tow", "a race", "a convoy", "a detour"]],
  ["Life", ["the police", "rivals", "friends", "cargo", "money", "a view", "the sunset", "coffee", "a nap", "glory", "luck", "trouble"]]
];
var SIGN_WORD_LIST = SIGN_WORDS.flatMap(([, ws]) => ws);
var SIGN_JOINS = ["and then", "but", "therefore", "or", "so"];
function cleanSign(m) {
  if (!Array.isArray(m) || m.length !== 2 && m.length !== 5 || !m.every((v) => Number.isInteger(v) && v >= 0)) return null;
  const [p, w, j, p2, w2] = m;
  if (p >= SIGN_PHRASES.length || w >= SIGN_WORD_LIST.length) return null;
  if (m.length === 2) return [p, w];
  if (j >= SIGN_JOINS.length || p2 >= SIGN_PHRASES.length || w2 >= SIGN_WORD_LIST.length) return null;
  return [p, w, j, p2, w2];
}
function signText(m) {
  const one = (p, w) => SIGN_PHRASES[p].replace("{}", SIGN_WORD_LIST[w]);
  const first = one(m[0], m[1]);
  if (m.length === 2) return first;
  const second = one(m[3], m[4]);
  return `${first}, ${SIGN_JOINS[m[2]]} ${second.charAt(0).toLowerCase()}${second.slice(1)}`;
}
function signFate(hearts) {
  return hearts >= MARKS.carveUp ? "carved" : null;
}
var driftPoints = (s) => Math.round(s * 10);
function decodeGhost(s) {
  if (!s) return [];
  let bin;
  try {
    bin = atob(s);
  } catch {
    return [];
  }
  const n = Math.floor(bin.length / 6);
  const bytes = new Uint8Array(n * 6);
  for (let k = 0; k < bytes.length; k++) bytes[k] = bin.charCodeAt(k);
  const a = new Int16Array(bytes.buffer);
  const out = [];
  for (let k = 0; k < n; k++) out.push([a[k * 3] / 16, a[k * 3 + 1] / 16, a[k * 3 + 2] / 1e3]);
  return out;
}
var PICK_LABEL = {
  loved: "Most loved",
  road: "Longest new road",
  growth: "Fastest growing",
  lap: "Lap record",
  drift: "Drift record",
  sign: "Best sign",
  guests: "Most visited HQ",
  postcard: "Postcard of the week"
};
var NEWS_KEEP = 40;

// shared/community.ts
var COMMUNITY = {
  /** A work with this many hearts is listed: only its builder may bulldoze it. */
  listedAt: 10,
  /** Minutes signed in before a fleet's hearts count (a second account can't be made just to give them). */
  heartAfterMins: 30,
  /** The most work ids a player's record keeps as hearted. */
  heartsMax: 2e3,
  /** Roads shorter than this (in nodes) are not listed anywhere, named or prompted for. */
  roadMin: 12,
  /** A bridge at least this long makes the news. */
  newsBridge: 20,
  /** Earthworks: lifts a landform needs (a hill, a lake, a canal) before it is a work, with a cairn, a name tag and hearts. */
  stakeLifts: 40,
  /** Builders a chunk of earthworks credits. */
  landByMax: 3,
  /** A work's name, at most. */
  nameMax: 24,
  /** Different fleets counted per work; past this the count stops. */
  usersMax: 64,
  /** Crossing milestones that are announced. */
  usesNews: [100, 1e3, 1e4],
  /** Each list on the World tab. */
  worldTop: 50,
  /** Seconds between path reports from a game, and the most tiles one may carry. */
  treadEvery: 20,
  treadMax: 300,
  /** km/h a land vehicle must be doing to wear the ground. */
  wearKmh: 8
};
var PATH = {
  faint: 6,
  worn: 30,
  wornFleets: 3,
  beaten: 120,
  beatenFleets: 5,
  /** Fleets remembered per chunk. */
  fleetsMax: 16,
  /** Tiles a cell may have counted at once; past it, tiles below faint are let go to make room. */
  cap: 6e3
};
function stageOf(passes, fleets) {
  if (passes >= PATH.beaten && fleets >= PATH.beatenFleets) return 3;
  if (passes >= PATH.worn && fleets >= PATH.wornFleets) return 2;
  if (passes >= PATH.faint) return 1;
  return 0;
}
var N = 256;
var CHUNK = 16;
var CHUNKS = N / CHUNK;
var chunkOfTile = (i) => Math.floor(i % N / CHUNK) + Math.floor(Math.floor(i / N) / CHUNK) * CHUNKS;
function tileOfChunk(k, t) {
  const kx = k % CHUNKS, kz = (k - kx) / CHUNKS;
  return (kz * CHUNK + Math.floor(t / CHUNK)) * N + kx * CHUNK + t % CHUNK;
}
var slotOfTile = (i) => Math.floor(i / N) % CHUNK * CHUNK + i % N % CHUNK;
function encodePasses(p) {
  const bytes = [];
  for (let k = 0; k < p.length; ) {
    if (p[k] === 0) {
      let n = 0;
      while (k < p.length && p[k] === 0 && n < 255) {
        n++;
        k++;
      }
      bytes.push(0, n);
    } else bytes.push(p[k++]);
  }
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}
function decodePasses(s) {
  const out = new Uint8Array(CHUNK * CHUNK);
  if (!s) return out;
  let bin;
  try {
    bin = atob(s);
  } catch {
    return out;
  }
  let p = 0;
  for (let b = 0; b < bin.length && p < out.length; b++) {
    const v = bin.charCodeAt(b);
    if (v === 0) {
      p += bin.charCodeAt(++b) || 0;
      continue;
    }
    out[p++] = v;
  }
  return out;
}
function roadWorkId(nodes) {
  let h = 2166136261;
  for (const n of nodes) {
    let v = n | 0;
    for (let k = 0; k < 4; k++) {
      h ^= v & 255;
      h = Math.imul(h, 16777619);
      v >>>= 8;
    }
  }
  return `r${(h >>> 0).toString(36)}`;
}
var WORK_KEY = /^(-?\d{1,5},-?\d{1,5})#(r[0-9a-z]{1,8}(?:\.\d{1,2})?|F\d{1,5}|W\d{1,3}|b\d{1,7})$/;
var WORK_LABEL = {
  road: "Road",
  bridge: "Bridge",
  suspension: "Suspension bridge",
  tunnel: "Tunnel",
  flyover: "Flyover",
  viaduct: "Viaduct",
  path: "Beaten track",
  hq: "HQ",
  mountain: "Mountain",
  hill: "Hill",
  lake: "Lake",
  canal: "Canal",
  valley: "Valley",
  reclaim: "Reclaimed land",
  land: "Earthworks"
};
function decodeLandChunk(s) {
  const raw = new Int8Array(CHUNK * CHUNK * 5);
  if (!s) return raw;
  let bin;
  try {
    bin = atob(s);
  } catch {
    return raw;
  }
  let p = 0;
  for (let b = 0; b < bin.length && p < raw.length; b++) {
    const v = bin.charCodeAt(b);
    if (v === 0) {
      p += bin.charCodeAt(++b) || 0;
      continue;
    }
    raw[p++] = v > 127 ? v - 256 : v;
  }
  return raw;
}
function landFeatures(land) {
  if (!land) return [];
  const raise = new Float32Array(N * N), dig = new Float32Array(N * N), wUp = new Float32Array(N * N), wDn = new Float32Array(N * N);
  const peak = new Int8Array(N * N);
  const changed = new Uint8Array(N * N);
  for (const [key, s] of Object.entries(land)) {
    const k = Number(key);
    if (!(k >= 0 && k < CHUNKS * CHUNKS)) continue;
    const raw = decodeLandChunk(s);
    for (let t = 0; t < CHUNK * CHUNK; t++) {
      const i = tileOfChunk(k, t);
      let up = 0, down = 0, top = 0;
      for (let q = 0; q < 4; q++) {
        const d = raw[q * 256 + t];
        if (d > 0) up += d;
        else down -= d;
        if (d > top) top = d;
      }
      const wt = raw[1024 + t];
      if (!up && !down && !wt) continue;
      changed[i] = 1;
      raise[i] = up / 4;
      dig[i] = down / 4;
      peak[i] = top;
      if (wt > 0) wUp[i] = wt;
      else wDn[i] = -wt;
    }
  }
  const out = [];
  const seen = new Uint8Array(N * N);
  const stack = [];
  for (let start = 0; start < N * N; start++) {
    if (!changed[start] || seen[start]) continue;
    const tiles = [];
    seen[start] = 1;
    stack.push(start);
    while (stack.length) {
      const i = stack.pop();
      tiles.push(i);
      const x = i % N, z = (i - x) / N;
      for (let dz = -1; dz <= 1; dz++) for (let dx = -1; dx <= 1; dx++) {
        const nx = x + dx, nz = z + dz;
        if (nx < 0 || nz < 0 || nx >= N || nz >= N) continue;
        const j = nz * N + nx;
        if (changed[j] && !seen[j]) {
          seen[j] = 1;
          stack.push(j);
        }
      }
    }
    let up = 0, down = 0, wu = 0, wd = 0, best = tiles[0], x0 = N, x1 = 0, z0 = N, z1 = 0, sx = 0, sz = 0;
    for (const i of tiles) {
      up += raise[i];
      down += dig[i];
      wu += wUp[i];
      wd += wDn[i];
      if (peak[i] > peak[best]) best = i;
      const x = i % N, z = (i - x) / N;
      x0 = Math.min(x0, x);
      x1 = Math.max(x1, x);
      z0 = Math.min(z0, z);
      z1 = Math.max(z1, z);
      sx += x;
      sz += z;
    }
    const w = x1 - x0 + 1, d = z1 - z0 + 1;
    const long = Math.max(w, d) >= 3 * Math.min(w, d) && Math.min(w, d) <= 5;
    const mx = sx / tiles.length, mz = sz / tiles.length;
    const middle = tiles.reduce((b, i) => Math.hypot(i % N - mx, Math.floor(i / N) - mz) < Math.hypot(b % N - mx, Math.floor(b / N) - mz) ? i : b, tiles[0]);
    let kind;
    if (wu > 0.5 * Math.max(up, down) && wu >= wd) kind = long ? "canal" : "lake";
    else if (wd > 0.5 * Math.max(up, down)) kind = "reclaim";
    else if (up >= 2 * down) kind = peak[best] >= 6 ? "mountain" : "hill";
    else if (down >= 2 * up) kind = long ? "canal" : "valley";
    else kind = "land";
    out.push({
      tiles,
      lifts: Math.round(up + down + wu + wd),
      kind,
      top: kind === "mountain" || kind === "hill" ? best : middle,
      chunks: [...new Set(tiles.map(chunkOfTile))]
    });
  }
  return out;
}
var sampleTiles = (tiles, n = 24) => tiles.length <= n ? tiles.slice() : Array.from({ length: n }, (_, k) => tiles[Math.floor(k * tiles.length / n)]);
var spanKind = (flags) => flags & 8 ? "tunnel" : flags & 64 ? "viaduct" : flags & 32 ? flags & 1 ? "suspension" : "bridge" : flags & 4 ? "flyover" : flags & 1 ? "suspension" : "bridge";

// server/community.ts
var N2 = 256;
var AWAY_ITEMS = 12;
var HEART_AFTER = Number(process.env.HEART_AFTER_MINS ?? COMMUNITY.heartAfterMins);
var blankStats = () => ({ loved: 0, uses: 0, blazed: 0, gave: 0, listed: 0, works: 0 });
var blankAway = () => ({ hearts: 0, uses: 0, listed: 0, tracks: 0 });
var clean = (s, max) => s.replace(/[<>&"`\u0000-\u001f]/g, "").trim().slice(0, max);
var roadIndex = /* @__PURE__ */ new WeakMap();
function roadWorks(doc) {
  const had = roadIndex.get(doc);
  if (had && had.n === doc.edits.length) return had;
  const works = /* @__PURE__ */ new Map(), at = /* @__PURE__ */ new Map();
  const put = (w) => {
    works.set(w.id, w);
    for (const t of w.tiles) {
      const a = at.get(t);
      if (a) a.push(w);
      else at.set(t, [w]);
    }
  };
  for (const raw of doc.edits) {
    const e = raw;
    if (e.k === "raze" && typeof e.x === "number" && typeof e.z === "number") {
      for (const w of at.get(e.z * N2 + e.x) ?? []) w.tiles.delete(e.z * N2 + e.x);
      continue;
    }
    if (e.k !== "road" || e.g || typeof e.by !== "string" || !Array.isArray(e.n) || !e.n.length) continue;
    const wide = e.t === 5, tri = e.t === 8;
    const tilesOf = (nodes) => {
      const s = /* @__PURE__ */ new Set();
      for (const n of nodes) for (const t of tri ? [n - N2 - 1, n - N2, n - N2 + 1, n - 1, n, n + 1, n + N2 - 1, n + N2, n + N2 + 1] : wide ? [n - N2 - 1, n - N2, n - 1, n] : [n]) if (t >= 0 && t < N2 * N2) s.add(t);
      return s;
    };
    const id = roadWorkId(e.n);
    put({ id, by: e.by, at: e.at ?? 0, kind: "road", len: e.n.length, tiles: tilesOf(e.n) });
    (e.b ?? []).forEach((b, j) => {
      const [from, to, , , flags = 0] = b;
      if (!(to > from)) return;
      put({ id: `${id}.${j}`, by: e.by, at: e.at ?? 0, kind: spanKind(flags), len: to - from, tiles: tilesOf(e.n.slice(from, to + 1)) });
    });
  }
  const out = { n: doc.edits.length, works, at };
  roadIndex.set(doc, out);
  return out;
}
var passIndex = /* @__PURE__ */ new WeakMap();
function passesOf(doc) {
  let p = passIndex.get(doc);
  if (p) return p;
  p = { chunks: /* @__PURE__ */ new Map(), counted: 0 };
  for (const [k, s] of Object.entries(doc.paths ?? {})) {
    const a = decodePasses(s);
    p.chunks.set(Number(k), a);
    for (const v of a) if (v) p.counted++;
  }
  passIndex.set(doc, p);
  return p;
}
var Community = class {
  constructor(host) {
    this.host = host;
  }
  lists = { epoch: 0, loved: [], busy: [], fresh: [] };
  listsDirty = false;
  /** Chunks of each cell whose paths reached a new stage, waiting to be sent; and when each cell last was. */
  pending = /* @__PURE__ */ new Map();
  sentAt = /* @__PURE__ */ new Map();
  /** Tiles and works each player has already been counted for today: one pass a tile, one crossing a work, a day. */
  trod = /* @__PURE__ */ new Map();
  /** A heart left or taken back, and a player road kept: for the week's picks (server/marks.ts). */
  onHeart = () => {
  };
  onRoad = () => {
  };
  /** News for a player about their works that marks keeps and tells (a name the others voted in). */
  tell = () => {
  };
  async load() {
    const had = await this.host.store.get("works");
    this.lists = had && had.epoch === this.host.epoch() ? had : { epoch: this.host.epoch(), loved: [], busy: [], fresh: [] };
    if (!had || had.epoch !== this.host.epoch()) this.listsDirty = true;
  }
  async flush() {
    if (!this.listsDirty) return;
    this.listsDirty = false;
    await this.host.store.set("works", this.lists);
  }
  docKey(cell) {
    return `${this.host.epoch()}:${cell}`;
  }
  // ---- who built it ----------------------------------------------------------------------------------
  /** Who a work is credited to (the trailblazer, for a path), and what it is; null for no such work. */
  workOf(doc, cell, id) {
    if (id.startsWith("r")) {
      const w = roadWorks(doc).works.get(id);
      return w && w.tiles.size ? { by: w.by, kind: w.kind, len: w.len, at: w.at, t: w.tiles.values().next().value } : null;
    }
    if (id.startsWith("F")) {
      const f = doc.features?.[id];
      return f ? { by: f.by, kind: f.kind, len: f.lifts, t: f.top } : null;
    }
    if (id.startsWith("W")) {
      const by = doc.pathsBy?.[id.slice(1)];
      return by?.length && doc.beaten?.includes(Number(id.slice(1))) ? { by: by[0], kind: "path", len: by.length, t: tileOfChunk(Number(id.slice(1)), 136) } : null;
    }
    const d = doc.deeds?.[`${cell}#${id}`];
    return d && d.hq !== void 0 ? { by: d.by, kind: "hq" } : null;
  }
  line(doc, cell, id) {
    const w = this.workOf(doc, cell, id);
    if (!w) return null;
    const row = doc.social?.[id];
    const name = row?.name ?? (w.kind === "hq" ? doc.deeds?.[`${cell}#${id}`]?.name : void 0);
    return { k: `${cell}#${id}`, kind: w.kind, ...name ? { name } : {}, by: w.by, h: row?.h ?? 0, u: row?.u ?? 0, f: row?.f ?? 0, ...w.len ? { len: w.len } : {}, ...w.at ? { at: w.at } : {}, ...w.t !== void 0 ? { t: w.t } : {} };
  }
  /** A work's line on the World tab's lists, put in or brought up to date. `fresh`: it has just been made. */
  list(l, fresh = false) {
    if (!l) return;
    const into = (arr, keep, by, add) => {
      const k = arr.findIndex((x) => x.k === l.k);
      if (k >= 0) arr.splice(k, 1);
      if ((k >= 0 || add) && keep(l)) arr.push(l);
      arr.sort(by);
      arr.length = Math.min(arr.length, COMMUNITY.worldTop);
    };
    into(this.lists.loved, (x) => x.h > 0, (a, b) => b.h - a.h || b.u - a.u, true);
    into(this.lists.busy, (x) => x.u > 0, (a, b) => b.u - a.u || b.h - a.h, true);
    into(this.lists.fresh, () => true, (a, b) => (b.at ?? 0) - (a.at ?? 0), fresh);
    this.listsDirty = true;
  }
  // ---- the player's own numbers ------------------------------------------------------------------------
  stats(p) {
    p.cs = { ...blankStats(), ...p.cs ?? {} };
    if (p.cs.likes === void 0) p.cs.likes = p.cs.loved;
    return p.cs;
  }
  /** A change to a player's totals: told at once if they are signed in, kept for their next sign-in if not. */
  async credit(pid, f, nice) {
    const on = this.host.conn(pid);
    const p = on?.doc ?? await this.host.players.open(pid).catch(() => null);
    if (!p || !p.id) return;
    const s = this.stats(p);
    if (on) {
      f(s, null);
      if (nice) this.host.send(on, nice);
      this.host.send(on, { t: "cstats", s });
    } else {
      p.away = { ...blankAway(), ...p.away ?? {} };
      f(s, p.away);
    }
    this.host.players.touch(pid);
  }
  /** What a game is told about community on signing in; the away news is told once. */
  welcome(p) {
    const away = p.away && (p.away.hearts || p.away.uses || p.away.listed || p.away.tracks || p.away.wearing) ? p.away : null;
    delete p.away;
    return { hearts: p.hearts ?? [], s: this.stats(p), away };
  }
  /** A line of news to everyone within chat range of a cell, at most one every two minutes each. */
  news(cell, text, but) {
    const now = Date.now();
    for (const x of this.host.conns()) {
      if (x.pid === but || this.host.cheb(x.cell, cell) > CHAT_CELLS || now - (x.newsAt ?? 0) < 12e4) continue;
      x.newsAt = now;
      this.host.send(x, { t: "chat", kind: "sys", text });
    }
  }
  fleetName(pid) {
    const w = this.host.whoOf(pid);
    return w?.brand?.name ?? w?.name ?? "Somebody";
  }
  place(line) {
    return line.name ? `${line.name}` : `${this.fleetName(line.by)}\u2019s ${WORK_LABEL[line.kind].toLowerCase()}`;
  }
  // ---- what the game says ------------------------------------------------------------------------------
  async handle(c, m) {
    switch (m.t) {
      case "heart":
        await this.heart(c, String(m.k ?? ""), !!m.on);
        return true;
      case "tread":
        await this.tread(c, m);
        return true;
      case "name":
        await this.name(c, String(m.k ?? ""), String(m.text ?? ""));
        return true;
      case "report":
        await this.report(c, String(m.k ?? ""));
        return true;
      case "works":
        this.host.send(c, { t: "works", loved: this.lists.loved, busy: this.lists.busy, fresh: this.lists.fresh.slice(0, COMMUNITY.worldTop) });
        return true;
      case "suggest":
        await this.suggest(c, String(m.k ?? ""), String(m.text ?? ""));
        return true;
      case "vote":
        await this.vote(c, String(m.k ?? ""), String(m.by ?? ""), !!m.on);
        return true;
      case "accept":
        await this.accept(c, String(m.k ?? ""), String(m.by ?? ""));
        return true;
      default:
        return false;
    }
  }
  async heart(c, k, on) {
    const m = WORK_KEY.exec(k);
    if (!m) return;
    const [, cell, id] = m;
    const sys = (text) => this.host.send(c, { t: "chat", kind: "sys", text });
    if ((c.doc.mins ?? 0) < HEART_AFTER) {
      sys(`Hearts count once you have played ${HEART_AFTER} minutes online (${Math.floor(c.doc.mins ?? 0)} so far).`);
      return;
    }
    const doc = await this.host.cells.open(this.docKey(cell));
    const w = this.workOf(doc, cell, id);
    if (!w) return;
    if (w.by === c.pid) {
      sys("That\u2019s your own work: hearts are for other players\u2019.");
      return;
    }
    const list = c.doc.hearts ??= [];
    const had = list.includes(k);
    if (had === on) {
      this.host.send(c, { t: "cstats", s: this.stats(c.doc), hearts: list });
      return;
    }
    if (on) {
      list.push(k);
      if (list.length > COMMUNITY.heartsMax) list.shift();
    } else list.splice(list.indexOf(k), 1);
    const row = (doc.social ??= {})[id] ??= { h: 0, u: 0, f: 0 };
    row.h = Math.max(0, row.h + (on ? 1 : -1));
    const listedNow = on && row.h >= COMMUNITY.listedAt && !row.l;
    if (listedNow) row.l = 1;
    this.host.cells.touch(this.docKey(cell));
    this.host.players.touch(c.pid);
    if (on) {
      const gave = c.doc.gaveTo ??= [];
      if (!gave.includes(w.by)) gave.push(w.by);
      this.stats(c.doc).gave = gave.length;
    }
    this.host.send(c, { t: "cstats", s: this.stats(c.doc), hearts: list });
    const msg = { t: "social", k, row, from: c.who, on };
    this.host.toRoom(cell, msg);
    if (!c.subs.has(cell)) this.host.send(c, msg);
    const line = this.line(doc, cell, id);
    this.list(line);
    this.onHeart(line, on);
    await this.credit(w.by, (s, away) => {
      s.loved = Math.max(0, s.loved + (on ? 1 : -1));
      s.likes = Math.max(0, (s.likes ?? 0) + (on ? 1 : -1));
      if (!away) return;
      const items = away.items ??= [];
      let it = items.find((x) => x.k === k);
      if (on) {
        away.hearts++;
        if (!it) {
          it = { k, kind: w.kind, h: 0, from: [] };
          items.push(it);
        }
        if (row.name) it.name = row.name;
        it.h++;
        const who = c.who.brand?.name ?? c.who.name;
        if (!it.from.includes(who) && it.from.length < 3) it.from.push(who);
        if (items.length > AWAY_ITEMS) items.splice(0, items.length - AWAY_ITEMS);
      } else if (it && it.h > 0) {
        it.h--;
        away.hearts = Math.max(0, away.hearts - 1);
        if (!it.h) items.splice(items.indexOf(it), 1);
      }
    }, on ? { t: "nice", what: "heart", k, from: c.who, name: row.name } : void 0);
    if (listedNow && line) {
      await this.credit(w.by, (s, away) => {
        s.listed++;
        if (away) away.listed++;
      }, { t: "nice", what: "listed", k, n: row.h, name: row.name });
      this.news(cell, `${this.place(line)} is now listed: ${COMMUNITY.listedAt} players have left it a heart.`);
    }
  }
  async name(c, k, text) {
    const m = WORK_KEY.exec(k);
    if (!m || m[2].startsWith("b")) return;
    const [, cell, id] = m;
    const doc = await this.host.cells.open(this.docKey(cell));
    const w = this.workOf(doc, cell, id);
    if (!w || w.by !== c.pid) return;
    if (w.kind === "road" && (w.len ?? 0) < COMMUNITY.roadMin) return;
    const row = (doc.social ??= {})[id] ??= { h: 0, u: 0, f: 0 };
    const name = clean(text, COMMUNITY.nameMax);
    if (name) row.name = name;
    else delete row.name;
    this.host.cells.touch(this.docKey(cell));
    const msg = { t: "social", k, row };
    this.host.toRoom(cell, msg);
    if (!c.subs.has(cell)) this.host.send(c, msg);
    this.list(this.line(doc, cell, id));
  }
  // ---- names suggested by other players (docs/marks.html) ------------------------------------------------
  /** A work the suggestion is for, and its row, or null. Paths and landforms take names; HQs don't. */
  async suggestable(k) {
    const m = WORK_KEY.exec(k);
    if (!m || m[2].startsWith("b")) return null;
    const [, cell, id] = m;
    const doc = await this.host.cells.open(this.docKey(cell));
    const w = this.workOf(doc, cell, id);
    if (!w || w.kind === "road" && (w.len ?? 0) < COMMUNITY.roadMin) return null;
    return { cell, id, doc, w, row: (doc.social ??= {})[id] ??= { h: 0, u: 0, f: 0 } };
  }
  changed(c, cell, doc, k, id, row) {
    this.host.cells.touch(this.docKey(cell));
    const msg = { t: "social", k, row };
    this.host.toRoom(cell, msg);
    if (!c.subs.has(cell)) this.host.send(c, msg);
    this.list(this.line(doc, cell, id));
  }
  /** A name for someone else's work, from another player: theirs replaces any they suggested before. */
  async suggest(c, k, text) {
    const s = await this.suggestable(k);
    if (!s || s.w.by === c.pid) return;
    const name = clean(text, COMMUNITY.nameMax);
    const list = s.row.sg ??= [];
    const mine = list.findIndex((x) => x.by === c.pid);
    if (mine >= 0) list.splice(mine, 1);
    if (name) {
      if (list.length >= MARKS.suggestMax) {
        list.sort((a, b) => b.v.length - a.v.length);
        list.pop();
      }
      list.push({ text: name, by: c.pid, v: [] });
    }
    if (!list.length) delete s.row.sg;
    this.changed(c, s.cell, s.doc, k, s.id, s.row);
  }
  /** A vote for a suggested name (not one's own, nor on one's own work). Enough of them name a work that has no name yet. */
  async vote(c, k, by, on) {
    const s = await this.suggestable(k);
    const sg = s?.row.sg?.find((x) => x.by === by);
    if (!s || !sg || sg.by === c.pid || s.w.by === c.pid) return;
    sg.v = sg.v.filter((p) => p !== c.pid);
    if (on && sg.v.length < 64) sg.v.push(c.pid);
    if (!s.row.name && sg.v.length >= MARKS.suggestVotes) {
      s.row.name = sg.text;
      delete s.row.sg;
      this.tell(s.w.by, { what: "named", k, name: sg.text, by: sg.by }, sg.by);
      this.tell(sg.by, { what: "named", k, name: sg.text, by: sg.by });
    }
    this.changed(c, s.cell, s.doc, k, s.id, s.row);
  }
  /** The builder takes a name somebody suggested. */
  async accept(c, k, by) {
    const s = await this.suggestable(k);
    const sg = s?.row.sg?.find((x) => x.by === by);
    if (!s || !sg || s.w.by !== c.pid) return;
    s.row.name = sg.text;
    delete s.row.sg;
    this.changed(c, s.cell, s.doc, k, s.id, s.row);
    this.tell(sg.by, { what: "named", k, name: sg.text, by: sg.by }, c.pid);
  }
  /** A name reported: a line on the developer's page, which can clear it. */
  async report(c, k) {
    const m = WORK_KEY.exec(k);
    if (!m) return;
    const doc = await this.host.cells.open(this.docKey(m[1]));
    const row = doc.social?.[m[2]];
    const w = this.workOf(doc, m[1], m[2]);
    if (!row?.name || !w) return;
    const now = Date.now();
    await this.host.store.set(`feedback:${now.toString(36)}-r${Math.floor(Math.random() * 1e6).toString(36)}`, {
      at: now,
      subject: `Work name reported: \u201C${row.name}\u201D`,
      text: `${c.doc.name} reported the name of ${this.fleetName(w.by)}\u2019s ${w.kind} at ${k}.`,
      gid: null,
      player: c.doc.name,
      fleet: null,
      online: c.doc.name,
      seed: null,
      cell: cellOfKey(m[1]),
      day: null,
      gen: null,
      where: "online",
      work: k
    });
    this.host.send(c, { t: "chat", kind: "sys", text: "Reported: thank you. Someone will look at it." });
  }
  /** The developer's page: take a work's name off. */
  async clearName(k) {
    const m = WORK_KEY.exec(k);
    if (!m) return false;
    const doc = await this.host.cells.open(this.docKey(m[1]));
    const row = doc.social?.[m[2]];
    if (!row?.name) return false;
    delete row.name;
    this.host.cells.touch(this.docKey(m[1]));
    this.host.toRoom(m[1], { t: "social", k, row });
    this.list(this.line(doc, m[1], m[2]));
    return true;
  }
  // ---- tyres on the ground, and crossings -----------------------------------------------------------
  async tread(c, m) {
    const now = Date.now();
    if (now - (c.treadAt ?? 0) < (COMMUNITY.treadEvery - 5) * 1e3) return;
    c.treadAt = now;
    const day = Math.floor(this.host.hours() / 24);
    let trod = this.trod.get(c.pid);
    if (!trod || trod.day !== day) this.trod.set(c.pid, trod = { day, seen: /* @__PURE__ */ new Set() });
    const seen = trod.seen;
    if (typeof m.c === "string" && Array.isArray(m.tiles) && c.subs.has(m.c) && this.host.cheb(m.c, c.cell) <= 1) {
      const doc = this.host.cells.peek(this.docKey(m.c));
      if (doc) this.wear(c, m.c, doc, m.tiles.slice(0, COMMUNITY.treadMax), seen);
    }
    for (const k of (Array.isArray(m.use) ? m.use : []).slice(0, 20)) await this.use(c, String(k), seen);
  }
  wear(c, cell, doc, tiles, seen) {
    const p = passesOf(doc);
    const touched = /* @__PURE__ */ new Set();
    const grew = /* @__PURE__ */ new Set();
    for (const raw of tiles) {
      const i = Number(raw);
      if (!Number.isInteger(i) || i < 0 || i >= N2 * N2) continue;
      const key = `${cell}:${i}`;
      if (seen.has(key) || seen.size > 4e4) continue;
      seen.add(key);
      const k = chunkOfTile(i), slot = slotOfTile(i);
      let a = p.chunks.get(k);
      if (!a) p.chunks.set(k, a = new Uint8Array(256));
      if (!a[slot]) {
        if (p.counted >= PATH.cap) this.prune(doc, p);
        if (p.counted >= PATH.cap) continue;
        p.counted++;
      }
      const by = (doc.pathsBy ??= {})[String(k)] ??= [];
      const fleetsWas = by.length;
      if (!by.includes(c.pid) && by.length < PATH.fleetsMax) by.push(c.pid);
      if (by.length !== fleetsWas) grew.add(k);
      const was = stageOf(a[slot], by.length);
      if (a[slot] < 255) a[slot]++;
      touched.add(k);
      if (stageOf(a[slot], by.length) !== was) grew.add(k);
    }
    if (!touched.size) return;
    for (const k of touched) (doc.paths ??= {})[String(k)] = encodePasses(p.chunks.get(k));
    this.host.cells.touch(this.docKey(cell));
    for (const k of grew) {
      if (!this.stagesIn(p.chunks.get(k), doc.pathsBy?.[String(k)]?.length ?? 0).some((s2) => s2 > 0)) continue;
      let s = this.pending.get(cell);
      if (!s) this.pending.set(cell, s = /* @__PURE__ */ new Set());
      s.add(k);
      this.wearing(cell, doc, k, p.chunks.get(k));
      this.beaten(cell, doc, k, p.chunks.get(k));
    }
  }
  stagesIn(a, fleets) {
    return Array.from(a, (v) => stageOf(v, fleets));
  }
  /** A chunk's first beaten track: its trailblazer is told, it is on the lists, and the news goes round. */
  beaten(cell, doc, k, a) {
    if (doc.beaten?.includes(k)) return;
    const by = doc.pathsBy?.[String(k)] ?? [];
    if (!this.stagesIn(a, by.length).includes(3)) return;
    (doc.beaten ??= []).push(k);
    const blazer = by[0];
    void this.credit(blazer, (s, away) => {
      s.blazed++;
      if (away) away.tracks++;
    }, { t: "nice", what: "track", k: `${cell}#W${k}`, n: by.length });
    const line = this.line(doc, cell, `W${k}`);
    this.list(line, true);
    if (line) this.news(cell, `A beaten track has worn in: the way ${this.fleetName(blazer)} first drove, by ${by.length} fleets.`);
  }
  /**
   * A chunk's first worn path (stage 2): its trailblazer hears that other fleets are following their line,
   * well before it is beaten in (docs/marks.html, the trailblazer's loop).
   */
  wearing(cell, doc, k, a) {
    if (doc.worn?.includes(k) || doc.beaten?.includes(k)) return;
    const by = doc.pathsBy?.[String(k)] ?? [];
    if (!this.stagesIn(a, by.length).includes(2)) return;
    (doc.worn ??= []).push(k);
    void this.credit(by[0], (_s, away) => {
      if (away) away.wearing = (away.wearing ?? 0) + 1;
    }, { t: "nice", what: "wearing", k: `${cell}#W${k}`, n: by.length });
  }
  /** At the cap: let go of tiles that never reached faint, so the paths people really use have room. */
  prune(doc, p) {
    let n = 0;
    for (const [k, a] of p.chunks) {
      let changed = false;
      for (let t = 0; t < a.length; t++) {
        if (a[t] && a[t] < PATH.faint) {
          a[t] = 0;
          changed = true;
        }
        if (a[t]) n++;
      }
      if (changed) (doc.paths ??= {})[String(k)] = encodePasses(a);
    }
    p.counted = n;
  }
  async use(c, k, seen) {
    const m = WORK_KEY.exec(k);
    if (!m || m[2].startsWith("W") || m[2].startsWith("b") || m[2].startsWith("F")) return;
    const [, cell, id] = m;
    const once = `u|${k}`;
    if (seen.has(once)) return;
    const doc = this.host.cells.peek(this.docKey(cell));
    if (!doc) return;
    const w = this.workOf(doc, cell, id);
    if (!w || w.by === c.pid) return;
    seen.add(once);
    const row = (doc.social ??= {})[id] ??= { h: 0, u: 0, f: 0 };
    row.u++;
    const users = (doc.usedBy ??= {})[id] ??= [];
    if (!users.includes(c.pid) && users.length < COMMUNITY.usersMax) users.push(c.pid);
    row.f = users.length;
    this.host.cells.touch(this.docKey(cell));
    this.host.toRoom(cell, { t: "social", k, row });
    const line = this.line(doc, cell, id);
    this.list(line);
    const milestone = COMMUNITY.usesNews.includes(row.u);
    await this.credit(w.by, (s, away) => {
      s.uses++;
      if (away) away.uses++;
    }, milestone ? { t: "nice", what: "uses", k, n: row.u, name: row.name } : void 0);
    if (milestone && row.u >= 1e3 && line) this.news(cell, `${this.place(line)} has been crossed ${row.u.toLocaleString("en-US")} times by other fleets.`);
  }
  /** Every few seconds: the paths that moved go to everyone holding their cell, no more than once in twenty seconds a cell. */
  beat() {
    const now = Date.now();
    for (const [cell, ks] of this.pending) {
      if (now - (this.sentAt.get(cell) ?? 0) < COMMUNITY.treadEvery * 1e3) continue;
      const doc = this.host.cells.peek(this.docKey(cell));
      this.pending.delete(cell);
      if (!doc) continue;
      this.sentAt.set(cell, now);
      const chunks = {}, by = {};
      for (const k of ks) {
        chunks[k] = doc.paths?.[k] ?? "";
        by[k] = doc.pathsBy?.[k] ?? [];
      }
      this.host.toRoom(cell, { t: "paths", c: cell, chunks, by });
    }
    const day = Math.floor(this.host.hours() / 24);
    for (const [pid, t] of this.trod) if (t.day !== day) this.trod.delete(pid);
    for (const c of this.host.conns()) {
      const was = Math.floor(c.doc.mins ?? 0);
      c.doc.mins = (c.doc.mins ?? 0) + 5 / 60;
      if (Math.floor(c.doc.mins) !== was) this.host.players.touch(c.pid);
    }
  }
  // ---- edits and earthworks --------------------------------------------------------------------------
  /** A road edit the server has just kept: a big one is news and goes on the newest list. */
  edited(c, cell, doc, e) {
    if (e.k !== "road" || e.g || !Array.isArray(e.n)) return;
    const id = roadWorkId(e.n);
    const spans = e.b ?? [];
    if (e.n.length < COMMUNITY.roadMin && !spans.length) return;
    void this.credit(c.pid, (s) => {
      s.works++;
    });
    if (e.n.length >= COMMUNITY.roadMin) {
      const line = this.line(doc, cell, id);
      this.list(line, true);
      this.onRoad(line);
    }
    spans.forEach((b, j) => {
      const line = this.line(doc, cell, `${id}.${j}`);
      this.list(line, true);
      if (line && b[1] - b[0] >= COMMUNITY.newsBridge) this.news(cell, `${this.fleetName(c.pid)} has built a ${b[1] - b[0]}-tile ${line.kind === "suspension" ? "suspension bridge" : line.kind} near here.`, c.pid);
    });
  }
  /**
   * Earthworks have come in: each chunk's lifts are credited to its digger, and the cell's landforms are
   * read again. A landform keeps its id (and so its name and hearts) as it grows, by the tiles it was
   * known by; two that join keep the better loved; one that is big enough for the first time gets an id,
   * its cairn and a place on the newest list, and its digger a work to their name.
   */
  landed(c, cell, doc, was, now) {
    const rows = {};
    for (const [k, v] of Object.entries(now)) {
      const a = decodeLandChunk(was[k]), b = decodeLandChunk(v);
      let lifts = 0;
      for (let t = 0; t < a.length; t++) lifts += Math.abs(b[t] - a[t]);
      if (!lifts) continue;
      const list = (doc.landBy ??= {})[k] ??= [];
      const mine = list.find((r) => r[0] === c.pid);
      if (mine) mine[1] += lifts;
      else list.push([c.pid, lifts]);
      list.sort((x, y) => y[1] - x[1]);
      list.length = Math.min(list.length, 6);
      rows[k] = list;
    }
    if (!Object.keys(rows).length) return;
    const fresh = this.readFeatures(doc);
    for (const id of Object.keys(doc.features ?? {})) this.list(this.line(doc, cell, id), fresh.includes(id));
    for (const id of fresh) void this.credit(doc.features[id].by, (s) => {
      s.works++;
    });
    this.host.toRoom(cell, { t: "landBy", c: cell, rows, features: doc.features ?? {} });
  }
  /** The cell's landforms read again and matched to the ids they had. Returns the ids given for the first time. */
  readFeatures(doc) {
    const comps = landFeatures(doc.land);
    const compOf = /* @__PURE__ */ new Map();
    comps.forEach((f, n) => {
      for (const t of f.tiles) compOf.set(t, n);
    });
    const claims = /* @__PURE__ */ new Map();
    for (const [id, f] of Object.entries(doc.features ?? {})) {
      const votes = /* @__PURE__ */ new Map();
      for (const t of f.s) {
        const n = compOf.get(t);
        if (n !== void 0) votes.set(n, (votes.get(n) ?? 0) + 1);
      }
      let best = -1, most = 0;
      for (const [n, v] of votes) if (v > most) {
        best = n;
        most = v;
      }
      if (best >= 0) (claims.get(best) ?? claims.set(best, []).get(best)).push(id);
    }
    const next = {};
    const fresh = [];
    comps.forEach((f, n) => {
      if (f.lifts < COMMUNITY.stakeLifts) return;
      const ids = (claims.get(n) ?? []).sort((a, b) => (doc.social?.[b]?.h ?? 0) - (doc.social?.[a]?.h ?? 0) || Number(a.slice(1)) - Number(b.slice(1)));
      let id = ids[0];
      if (!id) {
        id = `F${doc.fseq = (doc.fseq ?? 0) + 1}`;
        fresh.push(id);
      }
      const dug = /* @__PURE__ */ new Map();
      for (const ch of f.chunks) for (const [p, l] of doc.landBy?.[String(ch)] ?? []) dug.set(p, (dug.get(p) ?? 0) + l);
      const by = [...dug].sort((a, b) => b[1] - a[1])[0]?.[0] ?? doc.features?.[id]?.by;
      if (!by) return;
      next[id] = { by, kind: f.kind, lifts: f.lifts, top: f.top, s: sampleTiles(f.tiles) };
    });
    if (Object.keys(next).length) doc.features = next;
    else delete doc.features;
    return fresh.filter((id) => next[id]);
  }
  /** Is a tile part of a listed work that isn't this player's? Then it isn't theirs to bulldoze. */
  razeBarred(doc, x, z, pid) {
    for (const w of roadWorks(doc).at.get(z * N2 + x) ?? []) {
      const parent = w.id.split(".")[0];
      const listed = doc.social?.[w.id]?.l ? w.id : doc.social?.[parent]?.l ? parent : null;
      if (w.by === pid || !listed) continue;
      const name = doc.social[listed].name;
      return `${name ? `${name} is` : `That ${listed === w.id ? w.kind : "road"} is`} listed: only ${this.fleetName(w.by)} can clear it.`;
    }
    return null;
  }
  /** Likes from something that isn't a work (a thumbs-up on a sign, server/marks.ts): into the purse, or out of it. */
  async likes(pid, n) {
    await this.credit(pid, (s) => {
      s.likes = Math.max(0, (s.likes ?? 0) + n);
    });
  }
  /** A player's line on the board: hearts received and works built. */
  boardBits(p) {
    const s = this.stats(p);
    return { hearts: s.loved, works: s.works };
  }
};

// server/marks.ts
var N3 = 256;
var LOT = /^(-?\d{1,5},-?\d{1,5})#(b\d{1,7})$/;
var TOWN = /^(-?\d{1,5},-?\d{1,5})#\d{1,4}$/;
var CELL = /^-?\d{1,5},-?\d{1,5}$/;
var WEEK0 = Date.UTC(2026, 0, 5);
var weekOf = (t) => Math.floor((t - WEEK0) / MARKS.week);
var blankPicks = (epoch, week) => ({ epoch, week, hearts: {}, road: null, lap: null, drift: null, sign: null, guests: {}, worth0: {}, last: null });
var spot = (cell, t) => {
  const [cx, cz] = cellOfKey(cell);
  return { x: cx * N3 + t % N3 + 0.5, z: cz * N3 + Math.floor(t / N3) + 0.5 };
};
var fmtLap = (t) => `${Math.floor(t / 60)}:${(t % 60).toFixed(1).padStart(4, "0")}`;
var Marks = class {
  constructor(host) {
    this.host = host;
    this.picks = blankPicks(0, weekOf(Date.now()));
  }
  picks;
  picksDirty = false;
  /** Hand-overs a GG may answer: "loser|town" → the winner. Forgotten on a restart, which is fine. */
  lost = /* @__PURE__ */ new Map();
  /** The last hand-over told for each town and pair of fleets (real ms), so a town flapping between two isn't told every minute. */
  toldAt = /* @__PURE__ */ new Map();
  /** A heart left on (or taken back from) a player's sign: their likes go up or down (server/community.ts likes). */
  onLike = () => {
  };
  async load() {
    const had = await this.host.store.get("picks");
    const week = weekOf(Date.now());
    if (had && had.epoch === this.host.epoch()) this.picks = had;
    else {
      this.picks = blankPicks(this.host.epoch(), week);
      this.picks.last = had?.last ?? null;
      this.picksDirty = true;
    }
    this.roll();
  }
  async flush() {
    if (!this.picksDirty) return;
    this.picksDirty = false;
    await this.host.store.set("picks", this.picks);
  }
  docKey(cell) {
    return `${this.host.epoch()}:${cell}`;
  }
  name(pid) {
    const w = this.host.whoOf(pid);
    return w?.brand?.name ?? w?.name ?? "Somebody";
  }
  sys(c, text) {
    this.host.send(c, { t: "chat", kind: "sys", text });
  }
  /** Sent to the room, and to the sender too when they aren't holding the cell. */
  tell(c, cell, m) {
    this.host.toRoom(cell, m);
    if (!c.subs.has(cell)) this.host.send(c, m);
  }
  // ---- news --------------------------------------------------------------------------------------------
  /** News about a player's mark: told at once if they're signed in, kept for their next sign-in if not. */
  async news(pid, n, from) {
    if (!isPid(pid)) return;
    const on = this.host.conn(pid);
    if (on) {
      this.host.send(on, { t: "mark", n, ...from ? { who: this.host.whoOf(from) ?? void 0 } : {} });
      return;
    }
    const p = await this.host.players.open(pid).catch(() => null);
    if (!p?.id) return;
    const list = p.mnews ??= [];
    if (n.what === "guest") {
      const k = list.findIndex((x) => x.what === "guest" && x.k === n.k && x.by === n.by);
      if (k >= 0) list.splice(k, 1);
    }
    list.push(n);
    if (list.length > NEWS_KEEP) list.splice(0, list.length - NEWS_KEEP);
    this.host.players.touch(pid);
  }
  /** What a game is told on signing in: the news kept, who paid rent while away, and its best patrons. Told once. */
  welcome(p) {
    const news = p.mnews ?? [];
    const payers = Object.entries(p.payers ?? {}).sort((a, b) => b[1] - a[1]).slice(0, 8).map(([k, v]) => [k, Math.round(v)]);
    delete p.mnews;
    delete p.payers;
    return { news, payers, patrons: this.patronsOf(p) };
  }
  patronsOf(p) {
    if (p.patronsEpoch !== this.host.epoch()) return [];
    return Object.entries(p.patrons ?? {}).sort((a, b) => b[1] - a[1]).slice(0, 8).map(([k, v]) => [k, Math.round(v)]);
  }
  // ---- what the game says --------------------------------------------------------------------------------
  async handle(c, m) {
    switch (m.t) {
      case "sign":
        await this.sign(c, String(m.c), Number(m.tile), m.m);
        return true;
      case "unsign":
        await this.unsign(c, String(m.c), String(m.id));
        return true;
      case "rate":
        await this.rate(c, String(m.c), String(m.id), m.v);
        return true;
      case "drift":
        await this.drift(c, String(m.c), Number(m.s), m.p, m.m);
        return true;
      case "lap":
        await this.lap(c, m);
        return true;
      case "ghost":
        await this.ghost(c, String(m.c), String(m.k));
        return true;
      case "guest":
        await this.guest(c, String(m.k), Number(m.stamp));
        return true;
      case "gg":
        await this.gg(c, String(m.to), String(m.town), !!m.back);
        return true;
      case "picks":
        this.host.send(c, { t: "picks", picks: this.picksNow() });
        return true;
      default:
        return false;
    }
  }
  // ---- signs -------------------------------------------------------------------------------------------
  async sign(c, cell, tile, raw) {
    const msg = cleanSign(raw);
    if (!CELL.test(cell) || !msg || !Number.isInteger(tile) || tile < 0 || tile >= N3 * N3 || !c.subs.has(cell) || this.host.cheb(cell, c.cell) > 1) return;
    const now = Date.now();
    if (now - (c.signAt ?? 0) < 4e3) {
      this.sys(c, "One sign at a time: give it a moment.");
      return;
    }
    const doc = this.host.cells.peek(this.docKey(cell));
    if (!doc) return;
    const signs = doc.signs ??= {};
    if (Object.values(signs).some((s) => s.t === tile)) {
      this.sys(c, "There is a sign there already.");
      return;
    }
    if (Object.keys(signs).length >= MARKS.signsPerCell) {
      this.sys(c, "This county has all the signs it can hold: rate the ones that are here.");
      return;
    }
    c.signAt = now;
    const mine = c.doc.signs ??= [];
    while (mine.length >= MARKS.signsMax) await this.takeDown(mine[0], c.pid);
    const id = `s${doc.sseq = (doc.sseq ?? 0) + 1}`;
    const row = { by: c.pid, t: tile, m: msg, at: Math.round(this.host.hours() * 10) / 10, up: [] };
    signs[id] = row;
    mine.push(`${cell}#${id}`);
    this.host.cells.touch(this.docKey(cell));
    this.host.players.touch(c.pid);
    this.tell(c, cell, { t: "signRow", c: cell, id, row });
  }
  /** A sign taken down: its owner's own doing, the oldest of theirs making room, or knocked over by votes. */
  async takeDown(key, owner2) {
    const [cell, id] = key.split("#");
    const p = this.host.conn(owner2)?.doc ?? await this.host.players.open(owner2).catch(() => null);
    if (p?.signs) {
      const k = p.signs.indexOf(key);
      if (k >= 0) p.signs.splice(k, 1);
      this.host.players.touch(owner2);
    }
    const doc = await this.host.cells.open(this.docKey(cell)).catch(() => null);
    if (!doc?.signs?.[id] || doc.signs[id].by !== owner2) return;
    delete doc.signs[id];
    this.host.cells.touch(this.docKey(cell));
    this.host.toRoom(cell, { t: "signRow", c: cell, id, row: null });
  }
  async unsign(c, cell, id) {
    if (!CELL.test(cell) || !/^s\d{1,6}$/.test(id)) return;
    const doc = await this.host.cells.open(this.docKey(cell));
    if (doc.signs?.[id]?.by !== c.pid) return;
    await this.takeDown(`${cell}#${id}`, c.pid);
    if (!c.subs.has(cell)) this.host.send(c, { t: "signRow", c: cell, id, row: null });
  }
  /** A heart on a sign (v 1), or taken back (0). A thumbs-down (-1, from a game before hearts) counts as taking it back. */
  async rate(c, cell, id, v) {
    if (!CELL.test(cell) || !/^s\d{1,6}$/.test(id) || v !== 1 && v !== 0 && v !== -1) return;
    const doc = this.host.cells.peek(this.docKey(cell)) ?? await this.host.cells.open(this.docKey(cell));
    const row = doc.signs?.[id];
    if (!row || row.by === c.pid) return;
    const had = row.up.includes(c.pid);
    const on = v === 1;
    if (had === on) return;
    if (on) {
      if (row.up.length < MARKS.votesMax) row.up.push(c.pid);
    } else row.up = row.up.filter((p) => p !== c.pid);
    this.host.cells.touch(this.docKey(cell));
    this.onLike(row.by, on ? 1 : -1);
    const key = `${cell}#${id}`;
    const carvedNow = signFate(row.up.length) === "carved" && !row.carved;
    if (carvedNow) row.carved = 1;
    this.tell(c, cell, { t: "signRow", c: cell, id, row });
    if (on) await this.news(row.by, { what: "signUp", c: cell, k: key, by: c.pid, ...carvedNow ? { carved: true } : {} }, c.pid);
    const best = this.picks.sign;
    if (row.up.length > (best?.n ?? 0) || best?.k === key) {
      this.picks.sign = { k: key, by: row.by, title: `\u201C${signText(row.m)}\u201D`, n: row.up.length, ...spot(cell, row.t), at: Date.now() };
      this.picksDirty = true;
    }
  }
  // ---- records -----------------------------------------------------------------------------------------
  async drift(c, cell, s, p, model) {
    if (!CELL.test(cell) || !c.subs.has(cell) || this.host.cheb(cell, c.cell) > 1) return;
    if (!(s >= MARKS.driftMin && s <= MARKS.driftMaxTiles) || !Array.isArray(p) || p.length < 4 || p.length > MARKS.driftPts * 2 || p.length % 2) return;
    if (!p.every((v) => Number.isInteger(v) && v >= 0 && v <= N3 * 4)) return;
    const now = Date.now();
    if (now - (c.driftAt ?? 0) < 1500) return;
    c.driftAt = now;
    const doc = this.host.cells.peek(this.docKey(cell));
    if (!doc) return;
    const mid = Math.floor(p.length / 4) * 2;
    const tile = Math.min(N3 - 1, Math.floor(p[mid + 1] / 4)) * N3 + Math.min(N3 - 1, Math.floor(p[mid] / 4));
    const k = String(chunkOfTile(tile));
    const had = doc.drifts?.[k];
    const score = Math.round(s * 10) / 10;
    if (had && had.s >= score) return;
    const row = { by: c.pid, s: score, at: Math.round(this.host.hours() * 10) / 10, p };
    (doc.drifts ??= {})[k] = row;
    this.host.cells.touch(this.docKey(cell));
    this.tell(c, cell, { t: "driftRow", c: cell, k, row });
    if (had && had.by !== c.pid) await this.news(had.by, { what: "driftLost", c: cell, k, by: c.pid, s: score, was: had.s }, c.pid);
    if (score > (this.picks.drift?.n ?? 0)) {
      this.picks.drift = { by: c.pid, title: `${driftPoints(score)}-point drift`, n: score, ...spot(cell, tile), at: now, ...typeof model === "string" && model.length <= 40 ? { m: model } : {} };
      this.picksDirty = true;
    }
  }
  async lap(c, m) {
    const cell = String(m.c), k = String(m.k), s = Number(m.s);
    if (!CELL.test(cell) || !/^\d{1,7}$/.test(k) || !c.subs.has(cell) || this.host.cheb(cell, c.cell) > 1) return;
    if (!(s >= MARKS.lapMin && s <= 600) || typeof m.m !== "string" || typeof m.g !== "string" || m.g.length > 13e3) return;
    const n = decodeGhost(m.g).length;
    if (n < s * MARKS.ghostHz * 0.6 || n > s * MARKS.ghostHz * 1.4 + 5) return;
    const doc = this.host.cells.peek(this.docKey(cell));
    if (!doc) return;
    const had = doc.laps?.[k];
    const secs = Math.round(s * 100) / 100;
    if (had && had.s <= secs) return;
    const row = { by: c.pid, s: secs, m: m.m.slice(0, 40), at: Math.round(this.host.hours() * 10) / 10, g: m.g };
    (doc.laps ??= {})[k] = row;
    this.host.cells.touch(this.docKey(cell));
    const { g: _g, ...shown } = row;
    this.tell(c, cell, { t: "lapRow", c: cell, k, row: shown });
    if (had && had.by !== c.pid) await this.news(had.by, { what: "lapLost", c: cell, k, by: c.pid, s: secs, was: had.s }, c.pid);
    if (had && had.by !== c.pid) this.near(cell, `${this.name(c.pid)} took the lap record at a circuit near here from ${this.name(had.by)}: ${fmtLap(secs)}.`, c.pid);
    const at = Number.isInteger(m.at) && m.at >= 0 && m.at < N3 * N3 ? m.at : N3 * 128 + 128;
    this.picks.lap = { by: c.pid, title: `${fmtLap(secs)} lap`, n: secs, ...spot(cell, at), at: Date.now(), m: row.m };
    this.picksDirty = true;
  }
  async ghost(c, cell, k) {
    if (!CELL.test(cell) || !/^\d{1,7}$/.test(k)) return;
    const doc = await this.host.cells.open(this.docKey(cell));
    const row = doc.laps?.[k];
    if (row?.g) this.host.send(c, { t: "ghost", c: cell, k, g: row.g, by: row.by, s: row.s });
  }
  /** A line to everyone signed in within chat range of a cell. */
  near(cell, text, but) {
    for (const x of this.host.conns()) if (x.pid !== but && this.host.cheb(x.cell, cell) <= CHAT_CELLS) this.host.send(x, { t: "chat", kind: "sys", text });
  }
  // ---- guestbooks ----------------------------------------------------------------------------------------
  async guest(c, key, stamp) {
    const lot = LOT.exec(key);
    if (!lot || !Number.isInteger(stamp) || stamp < 0 || stamp >= STAMPS.length || this.host.cheb(lot[1], c.cell) > 1) return;
    const [, cell, id] = lot;
    const doc = await this.host.cells.open(this.docKey(cell));
    const deed = doc.deeds?.[key];
    if (!deed || deed.hq === void 0 || deed.by === c.pid) return;
    const row = (doc.guests ??= {})[id] ??= { n: 0, by: [], list: [] };
    const day = Math.floor(this.host.hours() / 24);
    const last = row.list.findIndex((x) => x[0] === c.pid && Math.floor(x[2] / 24) === day);
    const fresh = last < 0;
    if (!fresh) row.list.splice(last, 1);
    else {
      row.n++;
      if (!row.by.includes(c.pid)) {
        row.by.push(c.pid);
        if (row.by.length > MARKS.guestFleets) row.by.shift();
      }
    }
    row.list.push([c.pid, stamp, Math.round(this.host.hours() * 10) / 10]);
    if (row.list.length > MARKS.guestKeep) row.list.splice(0, row.list.length - MARKS.guestKeep);
    this.host.cells.touch(this.docKey(cell));
    this.tell(c, cell, { t: "guestRow", c: cell, k: id, row, from: c.pid });
    await this.news(deed.by, { what: "guest", k: key, by: c.pid, stamp }, c.pid);
    if (fresh) {
      const r = deed.rects?.[0];
      const g = this.picks.guests[key] ??= { k: key, by: deed.by, title: deed.name || "HQ", n: 0, ...r ? spot(cell, (r[1] + (r[3] >> 1)) * N3 + r[0] + (r[2] >> 1)) : {}, at: Date.now() };
      g.n++;
      this.picksDirty = true;
    }
  }
  // ---- territory hand-overs ------------------------------------------------------------------------------
  /** A host's carriage row for a town is about to be kept: if the town's holder changed, it is written down and told. */
  async carriage(cell, doc, town, next) {
    if (!TOWN.test(town)) return;
    const was = doc.carriage?.[town]?.[1] ?? "";
    const now = String(next[1] ?? "");
    if (was === now) return;
    const list = (doc.holders ??= {})[town] ??= [];
    if (!list.length && !now) return;
    if (!list.length && was) list.push([was, Math.max(0, Number(doc.carriage?.[town]?.[2] ?? 0) * 24)]);
    list.push([now, Math.round(this.host.hours() * 10) / 10]);
    if (list.length > MARKS.holdersKeep) list.splice(0, list.length - MARKS.holdersKeep);
    this.host.cells.touch(this.docKey(cell));
    this.host.toRoom(cell, { t: "holderRow", c: cell, town, row: list });
    const t = Date.now(), pair = `${town}|${[was, now].sort().join("|")}`;
    if (t - (this.toldAt.get(pair) ?? 0) < 10 * 6e4) return;
    this.toldAt.set(pair, t);
    if (isPid(was)) {
      await this.news(was, { what: "townLost", town, by: now }, isPid(now) ? now : void 0);
      if (isPid(now)) this.lost.set(`${was}|${town}`, now);
    }
    if (isPid(now)) await this.news(now, { what: "townWon", town, from: was }, isPid(was) ? was : void 0);
  }
  async gg(c, to, town, back) {
    const key = `${c.pid}|${town}`;
    if (this.lost.get(key) !== to) return;
    this.lost.delete(key);
    await this.news(to, { what: "gg", town, by: c.pid, ...back ? { back: true } : {} }, c.pid);
  }
  // ---- rent ----------------------------------------------------------------------------------------------
  /** Rent taken at an owner's door from a payer's trade: who paid is kept, and told while they are away. */
  async rent(payer, owner2, amt, online) {
    const p = this.host.conn(owner2)?.doc ?? await this.host.players.open(owner2).catch(() => null);
    if (!p?.id) return;
    if (p.patronsEpoch !== this.host.epoch()) {
      p.patrons = {};
      p.patronsEpoch = this.host.epoch();
    }
    const pat = p.patrons ??= {};
    pat[payer] = (pat[payer] ?? 0) + amt;
    if (Object.keys(pat).length > 120) p.patrons = Object.fromEntries(Object.entries(pat).sort((a, b) => b[1] - a[1]).slice(0, 100));
    if (!online) {
      const pay = p.payers ??= {};
      pay[payer] = (pay[payer] ?? 0) + amt;
    }
    this.host.players.touch(owner2);
  }
  /** An owner's best patrons in this world, for their HQ card. */
  patrons(c) {
    return this.patronsOf(c.doc);
  }
  // ---- picks ---------------------------------------------------------------------------------------------
  /** A new week: last week's picks are kept as they ended, and the counting starts again. */
  roll() {
    const week = weekOf(Date.now());
    if (week === this.picks.week) return;
    const last = this.picksOf(false);
    const worths = this.host.worths();
    this.picks = { ...blankPicks(this.host.epoch(), week), last: last.items.length ? last : this.picks.last, worth0: { ...worths } };
    this.picksDirty = true;
  }
  /** A heart left (or taken back) on a work this week. */
  heart(line, on) {
    if (!line) return;
    const h = this.picks.hearts[line.k];
    const [cell] = line.k.split("#");
    if (!h) {
      if (!on) return;
      this.picks.hearts[line.k] = { k: line.k, by: line.by, title: line.name ?? "", n: 1, ...line.t !== void 0 ? spot(cell, line.t) : {}, at: Date.now() };
    } else {
      h.n = Math.max(0, h.n + (on ? 1 : -1));
      if (line.name) h.title = line.name;
    }
    if (!this.picks.hearts[line.k].title) this.picks.hearts[line.k].title = `#${line.kind}`;
    this.picksDirty = true;
  }
  /** A postcard opened, or a fleet made from it, this week: towards the postcard of the week. */
  card(id, by, title, x, z, open) {
    const cards2 = this.picks.cards ??= {};
    const l = cards2[id] ??= { by, title, n: 0, x, z, at: Date.now(), o: 0, r: 0 };
    l.title = title;
    if (open) {
      l.o = (l.o ?? 0) + 1;
      l.n += 1;
    } else {
      l.r = (l.r ?? 0) + 1;
      l.n += 10;
    }
    this.picksDirty = true;
  }
  /** A player road kept: the week's longest. */
  road(line) {
    if (!line || line.kind !== "road" || !line.len) return;
    if (line.len <= (this.picks.road?.n ?? 0)) return;
    const [cell] = line.k.split("#");
    this.picks.road = { k: line.k, by: line.by, title: line.name ?? "#road", n: line.len, ...line.t !== void 0 ? spot(cell, line.t) : {}, at: Date.now() };
    this.picksDirty = true;
  }
  /** A leaderboard line has come in: a player first seen this week starts their growth from here. */
  worth(pid, worth) {
    if (this.picks.worth0[pid] === void 0) {
      this.picks.worth0[pid] = worth;
      this.picksDirty = true;
    }
  }
  pick(kind, l, detail) {
    if (!l) return null;
    const w = this.host.whoOf(l.by);
    const who = w?.brand?.name ?? w?.name ?? "A fleet";
    let title = l.title;
    const wk = l.k ? /^(-?\d+,-?\d+)#(r[0-9a-z.]+|F\d+|W\d+)$/.exec(l.k) : null;
    const named = wk ? this.host.cells.peek(this.docKey(wk[1]))?.social?.[wk[2]]?.name : void 0;
    if (named) title = named;
    else if (title.startsWith("#")) {
      const k = title.slice(1);
      title = `${who}\u2019s ${(WORK_LABEL[k] ?? "work").toLowerCase()}`;
    }
    const b = w?.brand;
    return {
      kind,
      title,
      detail,
      by: l.by,
      who,
      colour: b?.primary ?? 8227474,
      ...l.k ? { k: l.k } : {},
      ...l.x !== void 0 ? { x: l.x, z: l.z } : {},
      ...b ? { brand: { primary: b.primary, accent: b.accent, logo: b.logo } } : {},
      ...l.m ? { model: l.m } : {}
    };
  }
  picksOf(live) {
    const p = this.picks;
    const items = [];
    const loved = Object.values(p.hearts).filter((h) => h.n > 0).sort((a, b) => b.n - a.n || a.at - b.at)[0] ?? null;
    items.push(this.pick("loved", loved, loved ? `\u2665 ${loved.n} this week` : ""));
    items.push(this.pick("road", p.road, p.road ? `${p.road.n} tiles` : ""));
    const worths = this.host.worths();
    let best = null;
    for (const [pid, now] of Object.entries(worths)) {
      const was = p.worth0[pid];
      if (was === void 0) continue;
      const d = now - was;
      if (d > 0 && (!best || d > best.d)) best = { pid, d };
    }
    if (best) items.push(this.pick("growth", { by: best.pid, title: this.name(best.pid), n: best.d, at: 0 }, `+$${Math.round(best.d).toLocaleString("en-US")} this week`));
    items.push(this.pick("lap", p.lap, p.lap ? "the freshest record" : ""));
    items.push(this.pick("drift", p.drift, p.drift ? `${p.drift.n} tiles sideways` : ""));
    items.push(this.pick("sign", p.sign, p.sign ? `\u2665 ${p.sign.n}` : ""));
    const visited = Object.values(p.guests).sort((a, b) => b.n - a.n)[0] ?? null;
    items.push(this.pick("guests", visited, visited ? `${visited.n} visit${visited.n === 1 ? "" : "s"} this week` : ""));
    const card = Object.values(p.cards ?? {}).sort((a, b) => b.n - a.n || a.at - b.at)[0] ?? null;
    items.push(this.pick("postcard", card, card ? `opened ${card.o ?? 0} time${card.o === 1 ? "" : "s"}${card.r ? ` \xB7 ${card.r} took a car` : ""} this week` : ""));
    void PICK_LABEL;
    return { from: WEEK0 + p.week * MARKS.week, live, items: items.filter((x) => !!x) };
  }
  /** The picks to show: this week's once there are a couple, else last week's. */
  picksNow() {
    this.roll();
    const cur = this.picksOf(true);
    return cur.items.length >= 2 || !this.picks.last ? cur : this.picks.last;
  }
  /** Every few seconds. */
  beat() {
    this.roll();
  }
};

// server/postcards.ts
import { randomInt } from "node:crypto";

// shared/postcards.ts
var POSTCARD = {
  /** New postcards a player may make in a day (real time). Updating one is free. */
  perDay: 20,
  /** Postcards a player keeps: making one past this lets the oldest go. */
  keep: 200,
  titleMax: 80,
  idLen: 5,
  /** A take this far (tiles) from the camera session's postcard makes a new one rather than moving it. */
  apart: 48,
  /** The postcard's picture (its link preview): the longest side in pixels, the most base64 accepted, and the reports that hide it. */
  picSide: 960,
  picMax: 22e4,
  picReports: 3
};
var ID_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789";
var ID = new RegExp(`^[${ID_ALPHABET}]{${POSTCARD.idLen}}$`);
var isCardId = (s) => typeof s === "string" && ID.test(s);
var isJpeg = (b64) => typeof b64 === "string" && b64.length > 100 && b64.length <= POSTCARD.picMax && b64.startsWith("/9j/") && /^[A-Za-z0-9+/=]+$/.test(b64);
var num = (v, lo, hi) => typeof v === "number" && Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : null;
function cleanPose(raw) {
  if (!raw || typeof raw !== "object") return null;
  const r = raw;
  const W = 1 << 22;
  const x = num(r.x, -W, W), z = num(r.z, -W, W), y = num(r.y, -500, 5e3);
  const yaw = num(r.yaw, -1e4, 1e4), pitch = num(r.pitch, 0, Math.PI / 2), dist = num(r.dist, 2, 2e3);
  const look = num(r.look, 0, 2), hour = num(r.hour, 0, 24);
  if (x === null || z === null || yaw === null || pitch === null || dist === null) return null;
  const tau = Math.PI * 2;
  return {
    x: Math.round(x * 100) / 100,
    z: Math.round(z * 100) / 100,
    y: Math.round((y ?? 0) * 100) / 100,
    yaw: Math.round((yaw % tau + tau) % tau * 1e3) / 1e3,
    pitch: Math.min(Math.round(pitch * 1e3), Math.floor(Math.PI / 2 * 1e3)) / 1e3,
    dist: Math.round(dist * 10) / 10,
    look: Math.round(look ?? 1),
    hour: Math.round((hour ?? 12) * 100) / 100,
    ...r.follow === true ? { follow: true } : {}
  };
}
function cleanTitle(raw) {
  return String(raw ?? "").replace(/[\u0000-\u001f\u007f<>]/g, " ").replace(/\s+/g, " ").trim().slice(0, POSTCARD.titleMax);
}
function newCardId(rnd) {
  let s = "";
  for (let i = 0; i < POSTCARD.idLen; i++) s += ID_ALPHABET[Math.floor(rnd() * ID_ALPHABET.length) % ID_ALPHABET.length];
  return s;
}

// server/postcards.ts
var MILESTONES = [10, 100, 1e3, 1e4];
var RIDE_LIKES = 5;
var DAY = 864e5;
var esc = (s) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
var Postcards = class {
  constructor(host) {
    this.host = host;
  }
  /** Opens already counted today: "address|id" → the day. */
  opened = /* @__PURE__ */ new Map();
  /** Pictures reported: "address|id", so an address reports one once. */
  reported = /* @__PURE__ */ new Set();
  /** A message that is a postcard's: handled, and true. */
  async handle(c, m) {
    switch (m.t) {
      case "postcard": {
        const ask = Number.isFinite(m.ask) ? Number(m.ask) : 0;
        const reply = (id2, why) => this.host.send(c, { t: "postcard", ask, id: id2, ...why ? { why } : {} });
        const pose = m.pose === void 0 ? null : cleanPose(m.pose);
        if (m.pose !== void 0 && !pose) {
          reply(null, "That isn\u2019t a place");
          return true;
        }
        if (m.id !== void 0) {
          if (!isCardId(m.id) || !c.doc.cards?.includes(m.id)) {
            reply(null, "Not one of yours");
            return true;
          }
          const card2 = await this.host.cards.open(m.id);
          if (!card2 || card2.gone || card2.by !== c.pid) {
            reply(null, "Not one of yours");
            return true;
          }
          if (pose) Object.assign(card2, pose, pose.follow ? {} : { follow: void 0 });
          if (m.title !== void 0) card2.title = cleanTitle(m.title) || card2.title;
          this.host.cards.touch(m.id);
          reply(m.id);
          return true;
        }
        if (!pose) {
          reply(null, "Nowhere to send it");
          return true;
        }
        const now = Date.now();
        const made = (c.doc.cardsMade ?? []).filter((t) => now - t < DAY);
        if (made.length >= POSTCARD.perDay) {
          reply(null, `${POSTCARD.perDay} new postcards a day: this take carries the game\u2019s address instead`);
          return true;
        }
        let id = "";
        for (let i = 0; i < 12 && !id; i++) {
          const k = newCardId(() => randomInt(1 << 30) / (1 << 30));
          if (!await this.host.cards.open(k)) id = k;
        }
        if (!id) {
          reply(null, "Try again in a moment");
          return true;
        }
        const card = { ...pose, id, by: c.pid, title: cleanTitle(m.title) || "A postcard", at: now, epoch: this.host.epoch(), opens: 0, rides: 0 };
        this.host.cards.set(id, card);
        c.doc.cardsMade = [...made, now].slice(-POSTCARD.perDay);
        const mine = [...c.doc.cards ?? [], id];
        while (mine.length > POSTCARD.keep) await this.drop(mine.shift());
        c.doc.cards = mine;
        this.host.touch(c.pid);
        reply(id);
        return true;
      }
      case "unpost": {
        if (!isCardId(m.id) || !c.doc.cards?.includes(m.id)) return true;
        await this.drop(m.id);
        c.doc.cards = c.doc.cards.filter((k) => k !== m.id);
        this.host.touch(c.pid);
        this.host.send(c, { t: "postcards", rows: await this.mine(c) });
        return true;
      }
      case "postcards":
        this.host.send(c, { t: "postcards", rows: await this.mine(c) });
        return true;
      case "postcardPic": {
        if (!isCardId(m.id) || !c.doc.cards?.includes(m.id) || !isJpeg(m.jpg)) return true;
        const card = await this.host.cards.open(m.id);
        if (!card || card.gone || card.by !== c.pid) return true;
        this.host.pics.set(m.id, { jpg: m.jpg });
        card.pic = Date.now();
        card.reports = 0;
        card.picHidden = false;
        this.host.cards.touch(m.id);
        return true;
      }
      default:
        return false;
    }
  }
  async drop(id) {
    const card = await this.host.cards.open(id);
    if (card) {
      card.gone = true;
      this.host.cards.touch(id);
    }
  }
  async mine(c) {
    const out = [];
    for (const k of (c.doc.cards ?? []).slice().reverse()) {
      const card = await this.host.cards.open(k);
      if (card && !card.gone) out.push(card);
    }
    return out;
  }
  /**
   * A fleet has been made from this postcard (`fleet`, its name): counted, its maker told and given likes, and the
   * cell it was taken in handed back, for a home near it.
   */
  async ride(id, fleet) {
    const card = isCardId(id) ? await this.host.cards.open(id) : null;
    if (!card || card.gone || card.epoch !== this.host.epoch()) return null;
    card.rides++;
    this.host.cards.touch(id);
    this.host.news(card.by, { what: "postcard", id, title: card.title, ride: fleet });
    this.host.likes(card.by, RIDE_LIKES);
    this.host.week(id, card.by, card.title, card.x, card.z, false);
    return [Math.floor(card.x / 256), Math.floor(card.z / 256)];
  }
  async view(id) {
    if (!isCardId(id)) return null;
    const card = await this.host.cards.open(id);
    if (!card || card.gone) return null;
    const who = this.host.whoOf(card.by);
    const { by, x, z, y, yaw, pitch, dist, look, hour, follow, title, epoch, at } = card;
    return {
      id,
      by,
      x,
      z,
      y,
      yaw,
      pitch,
      dist,
      look,
      hour,
      ...follow ? { follow } : {},
      title,
      epoch,
      at,
      fleet: who?.brand?.name ?? who?.name ?? "A player",
      colour: who?.brand?.primary ?? 14827823,
      now: this.host.epoch(),
      ...card.pic && !card.picHidden ? { pic: true } : {}
    };
  }
  // ---- pictures: served as the link's preview, reported, and looked at by the developer ---------------
  /** A postcard's picture as served: the JPEG, or null with none (or hidden). */
  async picOf(id) {
    const card = isCardId(id) ? await this.host.cards.open(id) : null;
    if (!card || card.gone || !card.pic || card.picHidden) return null;
    const p = await this.host.pics.open(id);
    return p ? Buffer.from(p.jpg, "base64") : null;
  }
  /** Every postcard with a picture, most reported first, for the developer's page. */
  async withPictures(all) {
    return all.filter((c) => c && c.pic && !c.gone).map((c) => {
      const w = this.host.whoOf(c.by);
      return { id: c.id, title: c.title, fleet: w?.brand?.name ?? w?.name ?? c.by, at: c.pic, opens: c.opens, rides: c.rides, reports: c.reports ?? 0, hidden: !!c.picHidden };
    }).sort((a, b) => b.reports - a.reports || b.at - a.at).slice(0, 200);
  }
  /** The developer takes a picture down (or puts a hidden one back). */
  async setPicture(id, show) {
    const card = isCardId(id) ? await this.host.cards.open(id) : null;
    if (!card || !card.pic) return false;
    if (show) {
      card.picHidden = false;
      card.reports = 0;
    } else {
      card.pic = void 0;
      card.picHidden = false;
      card.reports = 0;
      this.host.pics.set(id, null);
    }
    this.host.cards.touch(id);
    return true;
  }
  /** The http side: true when it answered. `play` is where the game is served ("/play/" or "/"). */
  async http(req, res, path, ip, play) {
    let m = /^\/p\/([a-z0-9]{1,12})\.jpg$/.exec(path);
    if (m) {
      const jpg = await this.picOf(m[1]);
      if (!jpg) {
        res.writeHead(404, { "content-type": "text/plain" }).end("No picture");
        return true;
      }
      res.writeHead(200, { "content-type": "image/jpeg", "content-length": jpg.length, "cache-control": "public, max-age=600" }).end(req.method === "HEAD" ? void 0 : jpg);
      return true;
    }
    m = /^\/api\/postcard\/([a-z0-9]{1,12})\/report$/.exec(path);
    if (m) {
      const head = { "content-type": "application/json; charset=utf-8", "access-control-allow-origin": "*" };
      if (req.method === "OPTIONS") {
        res.writeHead(204, { ...head, "access-control-allow-methods": "POST, OPTIONS" }).end();
        return true;
      }
      const card2 = isCardId(m[1]) ? await this.host.cards.open(m[1]) : null;
      if (req.method !== "POST" || !card2 || !card2.pic) {
        res.writeHead(404, head).end('{"ok":false}');
        return true;
      }
      const k = `${ip}|${m[1]}`;
      if (!this.reported.has(k)) {
        if (this.reported.size > 2e4) this.reported.clear();
        this.reported.add(k);
        card2.reports = (card2.reports ?? 0) + 1;
        if (card2.reports >= POSTCARD.picReports) card2.picHidden = true;
        this.host.cards.touch(m[1]);
      }
      res.writeHead(200, head).end('{"ok":true}');
      return true;
    }
    m = /^\/api\/postcard\/([a-z0-9]{1,12})$/.exec(path);
    if (m) {
      const v2 = await this.view(m[1]);
      const head = { "content-type": "application/json; charset=utf-8", "access-control-allow-origin": "*", "cache-control": "no-store" };
      if (!v2) {
        res.writeHead(404, head).end(JSON.stringify({ error: "gone" }));
        return true;
      }
      this.countOpen(v2.id, ip);
      this.host.count("card-read");
      res.writeHead(200, head).end(JSON.stringify(v2));
      return true;
    }
    m = /^\/p\/([a-z0-9]{1,12})\/?$/.exec(path);
    if (!m) return false;
    const v = await this.view(m[1]);
    this.host.count("card-link");
    const proto = String(req.headers["x-forwarded-proto"] ?? "").split(",")[0].trim() || "http";
    const origin = `${proto}://${String(req.headers.host ?? "localhost")}`;
    const go = `${play}?postcard=${encodeURIComponent(m[1])}`;
    const title = v ? `${v.title} \xB7 TinyFleet` : "TinyFleet";
    const card = v ? this.host.cards.peek(v.id) : null;
    const image = v?.pic && card?.pic ? `${origin}/p/${v.id}.jpg?v=${card.pic}` : `${origin}/media/hero.jpg`;
    const desc = v ? `A postcard from ${v.fleet}. TinyFleet is a tiny world of towns, roads and fleets: open the link and you\u2019re there, in your browser.` : "A tiny world of towns, roads and fleets, in your browser.";
    const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>${esc(title)}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta property="og:type" content="website"><meta property="og:site_name" content="TinyFleet">
<meta property="og:title" content="${esc(title)}"><meta property="og:description" content="${esc(desc)}">
<meta property="og:url" content="${esc(`${origin}/p/${m[1]}`)}"><meta property="og:image" content="${esc(image)}"><meta name="twitter:image" content="${esc(image)}">
<meta name="twitter:card" content="summary_large_image">
<meta http-equiv="refresh" content="0;url=${esc(go)}">
<script>location.replace(${JSON.stringify(go)})</script>
</head><body style="font-family:system-ui,sans-serif;background:#f4f7f1;color:#1f2b33;display:grid;place-items:center;min-height:100vh;margin:0">
<a href="${esc(go)}">Open TinyFleet</a></body></html>`;
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" }).end(req.method === "HEAD" ? void 0 : html);
    return true;
  }
  /** An open, once an address a day. */
  countOpen(id, ip) {
    const day = Math.floor(Date.now() / DAY);
    const k = `${ip}|${id}`;
    if (this.opened.get(k) === day) return;
    if (this.opened.size > 5e4) this.opened.clear();
    this.opened.set(k, day);
    const card = this.host.cards.peek(id);
    if (!card) return;
    card.opens++;
    this.host.cards.touch(id);
    if (MILESTONES.includes(card.opens)) this.host.news(card.by, { what: "postcard", id, title: card.title, opens: card.opens });
    this.host.week(id, card.by, card.title, card.x, card.z, true);
  }
};

// server/fleets.ts
import { randomBytes as randomBytes2 } from "node:crypto";

// shared/fleets.ts
var CREW_MAX = 8;
var INVITE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ";
var INVITE_LEN = 8;
var CODE = new RegExp(`^[${INVITE_ALPHABET}]{${INVITE_LEN}}$`);
var isInviteCode = (s) => typeof s === "string" && CODE.test(s);
var STARTER_MODEL = "tomato";
var CREW_SAVE_FIELDS = ["cash", "fleet", "estate", "brand", "owned", "ride"];

// server/fleets.ts
var LOT2 = /^(-?\d{1,5},-?\d{1,5})#[bp]\d{1,7}$/;
var num2 = (v) => typeof v === "number" && Number.isFinite(v) ? v : 0;
var clone = (v) => JSON.parse(JSON.stringify(v));
function newCode() {
  const b = randomBytes2(INVITE_LEN);
  let s = "";
  for (let i = 0; i < INVITE_LEN; i++) s += INVITE_ALPHABET[b[i] % INVITE_ALPHABET.length];
  return s;
}
function cleanUnit(raw) {
  const u = raw;
  if (!u || typeof u !== "object" || !Number.isInteger(u.uid) || u.uid <= 0 || typeof u.model !== "string" || u.model.length > 40 || typeof u.where !== "string" || !Array.isArray(u.cell) || JSON.stringify(u).length > 6e3) return null;
  return u;
}
function cleanDriver(raw) {
  const d = raw;
  if (!d || typeof d !== "object" || !Number.isInteger(d.id) || d.id <= 0 || !Number.isInteger(d.unit) || JSON.stringify(d).length > 2e3) return null;
  return d;
}
var Crews = class {
  constructor(host) {
    this.host = host;
  }
  // ---- who is in which ------------------------------------------------------------------------------
  /** The shared fleet a player is in, by id; null for a fleet of their own. */
  crewOf(pid) {
    return this.host.index()[pid] ?? null;
  }
  /** Do these two players run one fleet (themselves included)? Doors, rent and bulldozing go by this. */
  same(a, b) {
    if (a === b) return true;
    const x = this.crewOf(a);
    return !!x && x === this.crewOf(b);
  }
  /** Keep a session's shared fleet in memory while it is signed in (one hold a session, moved when it changes fleet). */
  holdFor(c, id) {
    if (c.crewHeld === id) return;
    if (c.crewHeld) this.host.docs.hold(c.crewHeld, -1);
    if (id) this.host.docs.hold(id, 1);
    c.crewHeld = id ?? void 0;
  }
  online(pid) {
    return !!pid && !!this.host.conn(pid);
  }
  async open(id) {
    const d = await this.host.docs.open(id);
    return d && d.id ? d : null;
  }
  members(crew) {
    const people = this.host.people();
    return crew.members.map((id) => ({ id, name: people[id]?.name ?? id, online: this.online(id), seen: crew.seen[id] ?? 0 }));
  }
  /** Said to every member signed in, but one. */
  toMembers(crew, m, but) {
    for (const id of crew.members) {
      const c = this.host.conn(id);
      if (c && c !== but) this.host.send(c, m);
    }
  }
  tellMembers(crew) {
    this.toMembers(crew, { t: "crew", id: crew.id, creator: crew.creator, brand: crew.brand, members: this.members(crew) });
  }
  // ---- signing in -----------------------------------------------------------------------------------
  /** The livery a member goes by: the fleet's. */
  brandOf(pid) {
    const id = this.crewOf(pid);
    return id ? this.host.docs.peek(id)?.brand : void 0;
  }
  /**
   * A member signing in: their fleet as it stands, with the vehicles nobody is driving (left on the map by
   * members who signed out, or by a world begun again) handed to their game. Null for a fleet of their own.
   */
  async signedIn(c) {
    const id = c.doc.crew;
    if (!id) return null;
    const crew = await this.open(id);
    if (!crew || !crew.members.includes(c.pid)) {
      delete c.doc.crew;
      delete this.host.index()[c.pid];
      this.host.worldChanged();
      return null;
    }
    this.holdFor(c, id);
    if (crew.epoch !== this.host.epoch()) {
      for (const row of Object.values(crew.units)) {
        row.u.where = "garaged";
        row.u.driver = 0;
        row.by = null;
      }
      for (const d of Object.values(crew.drivers)) d.unit = 0;
      crew.estate = {};
      crew.meta = { ...crew.meta, hq: null };
      crew.epoch = this.host.epoch();
    }
    crew.seen[c.pid] = Date.now();
    const handed = [];
    for (const row of Object.values(crew.units)) {
      if (row.u.where === "garaged" || row.by === c.pid) continue;
      if (!row.by || !this.online(row.by) && row.u.where !== "ridden") {
        row.by = c.pid;
        handed.push(row);
      }
    }
    this.host.docs.touch(id);
    if (handed.length) this.toMembers(crew, { t: "roster", units: handed }, c);
    c.who = { ...c.who, brand: crew.brand };
    this.tellMembers(crew);
    return this.view(crew);
  }
  view(crew) {
    return {
      id: crew.id,
      creator: crew.creator,
      brand: crew.brand,
      members: this.members(crew),
      cash: Math.round(crew.cash * 100) / 100,
      units: Object.values(crew.units),
      drivers: Object.values(crew.drivers),
      estate: crew.estate,
      meta: crew.meta,
      code: crew.code
    };
  }
  /** A member signing out: the vehicles their game was driving go to another member's game, or wait for the next one in. */
  signedOut(c) {
    const id = this.crewOf(c.pid);
    const crew = id ? this.host.docs.peek(id) : null;
    if (!crew) return;
    crew.seen[c.pid] = Date.now();
    this.handOver(crew, c.pid, true);
    this.holdFor(c, null);
    this.host.docs.touch(crew.id);
    this.tellMembers(crew);
  }
  /**
   * Every vehicle `pid` was driving goes to another member signed in (the one who signed in most recently), or
   * to nobody until one does. The one they ride stays theirs while `keepRide` (signing out), and is parked otherwise.
   */
  handOver(crew, pid, keepRide) {
    const next = crew.members.filter((m) => m !== pid && this.online(m)).sort((a, b) => (crew.seen[b] ?? 0) - (crew.seen[a] ?? 0))[0] ?? null;
    const moved2 = [];
    for (const row of Object.values(crew.units)) {
      if (row.by !== pid) continue;
      if (row.u.where === "ridden") {
        if (keepRide) continue;
        row.u.where = "parked";
      }
      row.by = row.u.where === "garaged" ? null : next;
      moved2.push(row);
    }
    if (moved2.length) this.toMembers(crew, { t: "roster", units: moved2 });
  }
  /** The invite the game signed in with: which fleet, to ask the player about (or why it can't be joined). */
  async offer(c, code) {
    if (!isInviteCode(code)) return void 0;
    const id = this.host.invites()[code];
    const crew = id ? await this.open(id) : null;
    if (!crew) return { code, name: "a fleet", brand: null, members: 0, why: "That invite has expired: ask for a new one." };
    const name = crew.brand?.name ?? "a fleet";
    const base = { code, name, brand: crew.brand, members: crew.members.length };
    if (crew.members.includes(c.pid)) return { ...base, why: `You are already in ${name}.` };
    if (crew.members.length >= CREW_MAX) return { ...base, why: `${name} is full (${CREW_MAX} players).` };
    const mine = c.doc.crew ? await this.open(c.doc.crew) : null;
    if (mine && mine.creator === c.pid && mine.members.length > 1) return { ...base, why: `You run ${mine.brand?.name ?? "a shared fleet"}: boot its other players first.` };
    return base;
  }
  /** What an invite's link page and the title ask: the fleet's name and livery. */
  async peekInvite(code) {
    if (!isInviteCode(code)) return null;
    const id = this.host.invites()[code];
    const crew = id ? await this.open(id) : null;
    return crew ? { name: crew.brand?.name ?? "A fleet", brand: crew.brand, members: crew.members.length } : null;
  }
  // ---- what a member may say ------------------------------------------------------------------------
  /** A message that is a shared fleet's: handled, and true. `join` and `brand` are the caller's (a new welcome; who may). */
  async handle(c, m) {
    switch (m.t) {
      case "invite": {
        const crew = c.doc.crew ? await this.open(c.doc.crew) : await this.create(c);
        if (!crew) {
          this.host.send(c, { t: "invite", code: null, why: "Drive for a moment first: your fleet is kept as you play." });
          return true;
        }
        this.host.send(c, { t: "invite", code: crew.code });
        this.tellMembers(crew);
        return true;
      }
      case "purse": {
        const crew = this.mine(c);
        if (!crew) return true;
        const d = Math.max(-1e9, Math.min(1e9, num2(m.d)));
        crew.cash = Math.round((crew.cash + d) * 100) / 100;
        this.host.docs.touch(crew.id);
        this.host.send(c, { t: "purse", cash: crew.cash, s: num2(m.s) });
        this.toMembers(crew, { t: "purse", cash: crew.cash }, c);
        return true;
      }
      case "roster": {
        const crew = this.mine(c);
        if (crew) await this.roster(c, crew, m);
        return true;
      }
      case "boot": {
        const crew = this.mine(c);
        if (!crew) return true;
        const id = String(m.id ?? "");
        if (crew.creator !== c.pid || id === c.pid || !crew.members.includes(id)) {
          this.host.send(c, { t: "deny", what: "boot", why: crew.creator !== c.pid ? "Only the fleet\u2019s founder can remove players." : "They aren\u2019t in this fleet." });
          return true;
        }
        await this.part(crew, id, "booted");
        return true;
      }
      case "leaveCrew": {
        const crew = this.mine(c);
        if (!crew) return true;
        if (crew.creator === c.pid) {
          this.host.send(c, { t: "deny", what: "leave", why: "You founded this fleet: it stays yours. Remove the others to run it alone." });
          return true;
        }
        await this.part(crew, c.pid, "left");
        return true;
      }
      default:
        return false;
    }
  }
  mine(c) {
    const id = c.doc.crew;
    const crew = id ? this.host.docs.peek(id) : null;
    return crew && crew.members.includes(c.pid) ? crew : null;
  }
  /** Is this player allowed to change the livery? The founder of a shared fleet, or anyone with a fleet of their own. */
  mayBrand(pid) {
    const id = this.crewOf(pid);
    const crew = id ? this.host.docs.peek(id) : null;
    return !crew || crew.creator === pid;
  }
  /** The founder's new livery: the fleet's, and every member's tag. */
  setBrand(pid, b) {
    const id = this.crewOf(pid);
    const crew = id ? this.host.docs.peek(id) : null;
    if (!crew) return;
    crew.brand = b;
    this.host.docs.touch(crew.id);
    const people = this.host.people();
    for (const m of crew.members) {
      if (people[m]) people[m] = { ...people[m], brand: b };
      const c = this.host.conn(m);
      if (c) c.who = { ...c.who, brand: b };
      if (m !== pid) this.host.retag(m);
    }
    this.host.worldChanged();
    this.tellMembers(crew);
  }
  /** A fleet of one's own becomes a shared fleet of one, from the career as last kept. */
  async create(c) {
    const rec = c.doc.record;
    if (!rec || typeof rec !== "object") return null;
    const fleet = rec.fleet ?? {};
    const id = `c${this.host.nextId()}`;
    let code = newCode();
    while (this.host.invites()[code]) code = newCode();
    const units = {};
    for (const raw of fleet.units ?? []) {
      const u = cleanUnit(raw);
      if (u) units[String(u.uid)] = { u, by: u.where === "garaged" ? null : c.pid };
    }
    const drivers = {};
    for (const raw of fleet.drivers ?? []) {
      const d = cleanDriver(raw);
      if (d) drivers[String(d.id)] = d;
    }
    const crew = {
      id,
      creator: c.pid,
      members: [c.pid],
      brand: c.doc.brand,
      cash: num2(rec.cash),
      units,
      drivers,
      estate: rec.estate && typeof rec.estate === "object" ? clone(rec.estate) : {},
      meta: { hq: fleet.hq ?? null, charter: num2(fleet.charter), hires: fleet.hires, bought: num2(fleet.bought) },
      code,
      created: Date.now(),
      seen: { [c.pid]: Date.now() },
      epoch: this.host.epoch()
    };
    this.host.docs.set(id, crew);
    this.holdFor(c, id);
    this.host.invites()[code] = id;
    this.host.index()[c.pid] = id;
    this.host.worldChanged();
    c.doc.crew = id;
    this.host.players.touch(c.pid);
    console.log(`shared fleet ${id} made by ${c.doc.name}`);
    return crew;
  }
  /**
   * Yes to an invite: their own fleet is set aside on their record (money, vehicles, buildings, livery) save the
   * vehicle they are driving, which joins the shared fleet with them (their game sends it); the buildings they
   * held alone go back on the market while they are away from it. Where they start: the fleet's HQ, or beside
   * the member signed in (else the one signed in last). Resolves with that spot, or why not.
   */
  async join(c, code) {
    const o = await this.offer(c, code);
    if (!o) return { why: "That isn\u2019t an invite code." };
    if (o.why) return { why: o.why };
    const crew = await this.open(this.host.invites()[code]);
    if (c.doc.crew) {
      const old = await this.open(c.doc.crew);
      if (old) await this.part(old, c.pid, "moved");
    }
    const rec = c.doc.record && typeof c.doc.record === "object" ? c.doc.record : null;
    const solo = {};
    for (const f of CREW_SAVE_FIELDS) if (rec && rec[f] !== void 0) solo[f] = clone(rec[f]);
    solo.brand = c.doc.brand;
    const fl = solo.fleet;
    if (fl && Array.isArray(fl.units)) {
      fl.units = fl.units.filter((u) => u.uid !== fl.ride);
      const next = fl.units.find((u) => u.where !== "garaged") ?? fl.units[0];
      if (next) {
        next.where = "ridden";
        next.driver = 0;
        fl.ride = next.uid;
        solo.ride = next.model;
      } else {
        fl.ride = 0;
        solo.ride = STARTER_MODEL;
      }
      solo.owned = [...new Set(fl.units.map((u) => u.model))];
    } else {
      solo.ride = STARTER_MODEL;
      solo.owned = [];
    }
    await this.release(c.pid, Object.keys(rec?.estate ?? {}));
    delete c.doc.hq;
    c.doc.solo = solo;
    c.doc.crew = crew.id;
    crew.members.push(c.pid);
    crew.seen[c.pid] = Date.now();
    this.host.index()[c.pid] = crew.id;
    const people = this.host.people();
    if (people[c.pid]) people[c.pid] = { ...people[c.pid], brand: crew.brand };
    c.who = { ...c.who, brand: crew.brand };
    this.host.worldChanged();
    this.host.players.touch(c.pid);
    this.host.docs.touch(crew.id);
    this.host.retag(c.pid);
    console.log(`${c.doc.name} joined shared fleet ${crew.id}`);
    return { spawn: await this.spawnFor(crew, c.pid) };
  }
  /** Where a newcomer starts: the HQ's lot, else beside the member signed in, else where the one signed in last stood. */
  async spawnFor(crew, pid) {
    const hq = crew.meta.hq;
    if (hq && [hq.cx, hq.cz, hq.x, hq.z].every((v) => typeof v === "number" && Number.isFinite(v))) return [hq.cx, hq.cz, hq.x, hq.z];
    const others = crew.members.filter((m) => m !== pid);
    for (const m of others) {
      const p = this.host.conn(m)?.pose;
      if (p) return [p[0], p[1], Math.min(254, p[2] + 2), p[3]];
    }
    for (const m of others.sort((a, b) => (crew.seen[b] ?? 0) - (crew.seen[a] ?? 0))) {
      const doc = this.host.players.peek(m) ?? await this.host.players.open(m);
      const r = doc?.record;
      if (r && Array.isArray(r.cell) && typeof r.x === "number" && typeof r.z === "number") return [r.cell[0], r.cell[1], Math.min(254, r.x + 2), r.z];
    }
    return null;
  }
  /**
   * A member out of the fleet (booted, left, or moving to another): the vehicles their game drove go to the others,
   * the one they ride is parked and stays with the fleet, the fleet's doors registered in their name pass to the
   * founder, and their own fleet comes back onto their record (its buildings re-registered by their game, or
   * refunded where someone else has bought them since). Signed in, their session ends: the next sign-in is theirs alone.
   */
  async part(crew, pid, why) {
    this.handOver(crew, pid, false);
    crew.members = crew.members.filter((m) => m !== pid);
    delete crew.seen[pid];
    delete this.host.index()[pid];
    for (const key of Object.keys(crew.estate)) {
      const lot = LOT2.exec(key);
      if (!lot) continue;
      const dk = `${this.host.epoch()}:${lot[1]}`;
      const doc2 = await this.host.cells.open(dk);
      const row = doc2.deeds?.[key];
      if (row && row.by === pid) {
        row.by = crew.creator;
        this.host.cells.touch(dk);
        const people = this.host.people();
        const w = people[crew.creator];
        this.host.toRoom(lot[1], { t: "deed", key, row, who: w ? { id: crew.creator, name: w.name, brand: w.brand } : void 0 });
      }
    }
    const c = this.host.conn(pid);
    const doc = c?.doc ?? await this.host.players.open(pid);
    if (doc?.id) {
      const solo = doc.solo ?? {};
      const rec = doc.record && typeof doc.record === "object" ? doc.record : null;
      if (rec) {
        for (const f of CREW_SAVE_FIELDS) {
          if (solo[f] === void 0) delete rec[f];
          else rec[f] = solo[f];
        }
        rec.cash = num2(solo.cash);
        rec.ride ??= STARTER_MODEL;
        rec.owned ??= [];
      }
      if (solo.brand !== void 0) doc.brand = solo.brand;
      if (doc.hq && crew.estate[doc.hq.key] !== void 0) delete doc.hq;
      delete doc.solo;
      delete doc.crew;
      const people = this.host.people();
      if (people[pid]) people[pid] = { ...people[pid], brand: doc.brand };
      this.host.players.touch(pid);
    }
    this.host.worldChanged();
    this.host.docs.touch(crew.id);
    const name = crew.brand?.name ?? "the fleet";
    if (c) {
      c.who = { ...c.who, brand: doc?.brand ?? null };
      this.holdFor(c, null);
    }
    if (c && why !== "moved") {
      c.parted = true;
      this.host.send(c, { t: "refuse", code: "booted", why: why === "booted" ? `You were removed from ${name}. Sign in again to play your own fleet.` : why === "left" ? `You left ${name}. Sign in again to play your own fleet.` : `You left ${name}.` });
    }
    if (why !== "moved") this.host.retag(pid);
    this.tellMembers(crew);
    console.log(`${pid} ${why} shared fleet ${crew.id}`);
  }
  /** Doors held in `pid`'s own name taken off the server's books (they are back on the market). */
  async release(pid, keys) {
    for (const key of keys) {
      const lot = LOT2.exec(key);
      if (!lot) continue;
      const dk = `${this.host.epoch()}:${lot[1]}`;
      const doc = await this.host.cells.open(dk);
      if (doc.deeds?.[key]?.by !== pid) continue;
      delete doc.deeds[key];
      this.host.cells.touch(dk);
      this.host.toRoom(lot[1], { t: "deed", key, row: null });
    }
  }
  /** A member's game's changes to the roster, applied where they are its to make, and passed on. */
  async roster(c, crew, m) {
    const out = {}, back = {};
    const theirs = (row) => !!row && !!row.by && row.by !== c.pid && (this.online(row.by) || row.u.where === "ridden");
    for (const raw of (Array.isArray(m.units) ? m.units : []).slice(0, 64)) {
      const u = cleanUnit(raw);
      if (!u) continue;
      const have = crew.units[String(u.uid)];
      if (theirs(have)) {
        (back.units ??= []).push(have);
        continue;
      }
      const row = { u, by: u.where === "garaged" ? null : c.pid };
      crew.units[String(u.uid)] = row;
      (out.units ??= []).push(row);
    }
    for (const uid of (Array.isArray(m.gone) ? m.gone : []).slice(0, 64)) {
      const have = crew.units[String(uid)];
      if (!have) continue;
      if (theirs(have)) {
        (back.units ??= []).push(have);
        continue;
      }
      delete crew.units[String(uid)];
      (out.gone ??= []).push(Number(uid));
    }
    for (const raw of (Array.isArray(m.drivers) ? m.drivers : []).slice(0, 64)) {
      const d = cleanDriver(raw);
      if (!d) continue;
      const have = crew.drivers[String(d.id)];
      if (theirs(crew.units[String(d.unit)]) || have && theirs(crew.units[String(have.unit)])) {
        if (have) (back.drivers ??= []).push(have);
        continue;
      }
      crew.drivers[String(d.id)] = d;
      (out.drivers ??= []).push(d);
    }
    for (const id of (Array.isArray(m.fired) ? m.fired : []).slice(0, 64)) {
      const have = crew.drivers[String(id)];
      if (!have) continue;
      if (theirs(crew.units[String(have.unit)])) {
        (back.drivers ??= []).push(have);
        continue;
      }
      delete crew.drivers[String(id)];
      (out.fired ??= []).push(Number(id));
    }
    if (m.estate && typeof m.estate === "object") {
      for (const [key, v] of Object.entries(m.estate).slice(0, 64)) {
        if (!LOT2.test(key) || !Array.isArray(v) || JSON.stringify(v).length > 4e3) continue;
        crew.estate[key] = v;
        (out.estate ??= {})[key] = v;
      }
    }
    if (Array.isArray(m.sold)) {
      const gone = [];
      for (const key of m.sold.slice(0, 64)) {
        if (typeof key !== "string" || crew.estate[key] === void 0) continue;
        delete crew.estate[key];
        gone.push(key);
      }
      if (gone.length) {
        out.sold = gone;
        for (const pid of crew.members) await this.release(pid, gone);
      }
    }
    if (m.meta && typeof m.meta === "object") {
      const meta = {};
      if ("hq" in m.meta && JSON.stringify(m.meta.hq ?? null).length < 2e3) meta.hq = m.meta.hq ?? null;
      if (m.meta.charter !== void 0) meta.charter = num2(m.meta.charter);
      if (m.meta.bought !== void 0) meta.bought = num2(m.meta.bought);
      if (m.meta.hires && typeof m.meta.hires === "object" && Array.isArray(m.meta.hires.taken)) meta.hires = { day: num2(m.meta.hires.day), taken: m.meta.hires.taken.slice(0, 32).map(num2) };
      if (Object.keys(meta).length) {
        crew.meta = { ...crew.meta, ...meta };
        out.meta = meta;
      }
    }
    this.host.docs.touch(crew.id);
    if (Object.keys(out).length) this.toMembers(crew, { t: "roster", ...out }, c);
    if (Object.keys(back).length) this.host.send(c, { t: "roster", back: true, ...back });
  }
  // ---- links ----------------------------------------------------------------------------------------
  /**
   * GET /join/:code: on to the game with the invite in its address, where the title asks the player to sign in
   * or make a player, and then whether to join. GET /api/invite/:code: the fleet's name, for the title to show.
   */
  async http(_req, res, path, play) {
    const j = /^\/join\/([A-Za-z0-9-]{4,20})\/?$/.exec(path);
    if (j) {
      const code = j[1].toUpperCase().replace(/-/g, "");
      res.writeHead(302, { location: `${play}?join=${encodeURIComponent(code)}`, "cache-control": "no-store" }).end();
      return true;
    }
    const a = /^\/api\/invite\/([A-Za-z0-9]{4,20})$/.exec(path);
    if (a) {
      const v = await this.peekInvite(a[1].toUpperCase());
      res.writeHead(v ? 200 : 404, { "content-type": "application/json", "access-control-allow-origin": "*", "cache-control": "no-store" }).end(JSON.stringify(v ?? { gone: true }));
      return true;
    }
    return false;
  }
};

// src/world/tiles.ts
var N4 = 256;
var STEP = 0.5;
var SEA = 2;
var CHUNK2 = 16;
var CHUNKS2 = N4 / CHUNK2;
var REGION_KINDS = [
  "sands",
  "icefield",
  "shallows",
  "green",
  "heights",
  "moors",
  "veldt",
  "fens",
  "terraces",
  "whitewash",
  "goldhills",
  "firecoast",
  "fjords",
  "bigtimber",
  "redrock",
  "dragonbay",
  "steppe",
  "saltpan",
  "mangrove",
  "lavender"
];
var World = class {
  corners = new Int8Array(N4 * N4 * 4);
  water = new Int8Array(N4 * N4).fill(-1);
  // water surface height in steps, -1 = none
  biome = new Uint8Array(N4 * N4);
  flags = new Uint8Array(N4 * N4);
  fieldId = new Int16Array(N4 * N4).fill(-1);
  coastDist = new Uint16Array(N4 * N4);
  // tiles from the ocean (0 = ocean)
  elev = new Float32Array(N4 * N4);
  // continuous elevation used during generation
  temp = new Float32Array(N4 * N4);
  edges = new Uint8Array(N4 * N4);
  // car-agnostic drivable edge bits (bit d)
  road = new Uint8Array(N4 * N4);
  // widest RoadType covering each tile
  roadEdges = new Uint8Array(N4 * N4);
  // bit d: a road surface runs across that edge
  roadCover = new Uint8Array(N4 * N4);
  // how many roads cover the tile
  roadAxis = new Uint8Array(N4 * N4);
  // axes those roads run along (1 = x, 2 = z); both + 2 roads = junction
  /**
   * Tiles strictly inside a 45° diagonal road (world/diagonals.ts): which line runs through the tile and on
   * which side of its middle (1..4, `lineOf`), 0 for none. Rebuilt with the other road tables.
   */
  roadDiag = new Uint8Array(N4 * N4);
  laneSide = new Uint8Array(N4 * N4);
  // four-lane tiles: bit 0/1 low/high-z half of an x-running road, bit 2/3 low/high-x half of a z-running one
  roads = [];
  /** Bumped whenever the road tables are rebuilt, so what is worked out from them (the curves) is redone. */
  roadStamp = 0;
  deckAxis = new Uint8Array(N4 * N4);
  // a bridge deck crosses this tile along x (1) or z (2)
  deckToll = new Uint8Array(N4 * N4);
  // that deck is a toll bridge of the player's
  deckLo = new Float32Array(N4 * N4);
  // deck world height at the tile's low-x / low-z edge
  deckHi = new Float32Array(N4 * N4);
  // ... and at its high edge
  deckRoad = new Uint8Array(N4 * N4);
  // the road that deck carries: its surface is painted ROAD_INFO.lift above the deck
  townId = new Int8Array(N4 * N4).fill(-1);
  lot = new Uint8Array(N4 * N4);
  // Lot finish on town tiles
  blocked = new Uint8Array(N4 * N4);
  // a building stands here: nothing drives through
  cleared = new Uint8Array(N4 * N4);
  // bulldozed: no trees, rocks or cacti grow here any more
  /** Fences a car has knocked flat, by tile * 4 + edge: which way each fell (+1 or -1 along its local z). Not saved. */
  knocked = /* @__PURE__ */ new Map();
  feature = new Uint8Array(N4 * N4);
  // Feature: a broad beach, a quarry, a piste, an airfield
  roof = new Float32Array(N4 * N4);
  // world y of the building top on the tile (0 = none)
  surface = new Uint8Array(N4 * N4);
  // Surface: dune sand, salt, ice, lava, marsh, rock, cave
  /** Which biome reaches this tile: 1 + its index in REGION_KINDS, or 0 for none. */
  regionK = new Uint8Array(N4 * N4);
  /** How far into that biome the tile is, 0..255: 255 inside it, fading over a cell at a continent's blob edge. */
  regionW = new Uint8Array(N4 * N4);
  /**
   * The height, in steps, above which snow lies all year: 0 for the ordinary line. High country and a
   * continent's lifted heart carry their own, so a tableland at forty steps is not white from edge to edge.
   */
  snowLine = new Uint8Array(N4 * N4);
  /** The highest corner the generator made on this county, in steps. */
  peak = 24;
  /** Named landforms on this cell, for the map, the labels, the props and the tourist board. */
  landmarks = [];
  /** Brooks: thin streams drawn over the ground, each a polyline of local tiles x, z, ... (world/brooks.ts). */
  brooks = [];
  /** Jump ramps: a kicker at the end of a run-up, the gap it clears and the tile it lands on (world/jumps.ts). */
  jumps = [];
  /** Boost pads in the road: 1 + the DIRS direction a pad shoves a car along, 0 for none (world/jumps.ts). */
  boost = new Uint8Array(N4 * N4);
  towns = [];
  ports = [];
  /** Every town and harbour building, in the order they were built; a building's `uid` indexes this. */
  allBuildings = [];
  /** Buildings out in the country that belong to no town: wind turbines, orchards, windmills. */
  countryside = [];
  /** The roads generated between towns, and whether each could be built. */
  links = [];
  portId = new Int8Array(N4 * N4).fill(-1);
  /** RailBit per tile: track, crossing, deck overhead, tunnel beneath, station pad, loop, pier, reserved margin. */
  rail = new Uint8Array(N4 * N4);
  /** At-grade straight track a road may cross at right angles: 1 when the rails run along x, 2 along z. */
  railAxis = new Uint8Array(N4 * N4);
  /** The railway through this cell: every main line, rung end and passing loop, sampled along its centreline. */
  rails = [];
  /** Stations on this cell's railway. */
  stations = [];
  pois = [];
  /** Places that are not towns or harbours: broad beaches, quarries, pistes, airfields. */
  spots = [];
  /** Quarries, ski fields and airfields, with their plant and their cableways. */
  works = [];
  /** The trade of the cell's Vehicle Dealer (world/storefronts.ts), null with none. */
  dealerTrade = null;
  /**
   * The boat and aircraft dealers the cell has besides its car dealer (Oct 2026: every cell with a town has
   * one car dealer and one Auto Shop; a harbour may bring a boat dealer and an airfield an aircraft dealer).
   */
  dealerExtra = [];
  spawn = { x: N4 / 2 + 0.5, z: N4 / 2 + 0.5, heading: 0 };
  seed = "";
  // ---- the cell this island occupies on the endless lattice ----
  cx = 0;
  cz = 0;
  /** Cell origin in world tiles: world = local + (ox, oz). */
  ox = 0;
  oz = 0;
  kind = "full";
  /** This cell's own name: an island, or the county a continental cell is one of. */
  name = "";
  /** The continent this cell belongs to, named once for the whole blob; empty on an island. */
  continent = "";
  /** The biome this cell is part of, or null. Its ground may still fade toward a neighbour's. */
  region = null;
  /** The causeways leaving this island, one per side it shares with a neighbour. */
  straits = [];
  /** True when this island's towns are a megalopolis and its satellites. */
  mega = false;
  stats = { land: 0, drivable: 0, reachable: 0, genMs: 0, canyons: 0, fields: 0, lakes: 0, maxSlope: 0, passes: 0, riverCrossings: 0, railMs: 0, mended: 0 };
  /** Neighbour islands by DIRS index while they are resident; never serialised. */
  nbr = [null, null, null, null];
  /** Bumped whenever tiles change (a road laid, something bulldozed) so caches can notice. */
  revision = 0;
};
var OPEN_SEA = (() => {
  const w = new World();
  w.kind = "sea";
  w.name = "Open sea";
  w.water.fill(SEA);
  w.corners.fill(SEA - 5);
  w.biome.fill(0 /* Ocean */);
  w.coastDist.fill(0);
  w.elev.fill(SEA - 5);
  w.nbr = [w, w, w, w];
  return w;
})();

// src/world/rng.ts
function hashString(s) {
  let h = 1779033703 ^ s.length;
  for (let i = 0; i < s.length; i++) {
    h = Math.imul(h ^ s.charCodeAt(i), 3432918353);
    h = h << 13 | h >>> 19;
  }
  h = Math.imul(h ^ h >>> 16, 2246822507);
  h = Math.imul(h ^ h >>> 13, 3266489909);
  return (h ^ h >>> 16) >>> 0;
}
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = a + 1831565813 >>> 0;
    let t = a;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
var WORDS = [
  "plum",
  "tomato",
  "moss",
  "pebble",
  "dune",
  "lantern",
  "maple",
  "ferry",
  "clover",
  "heron",
  "cinder",
  "willow",
  "saffron",
  "meadow",
  "harbor",
  "quince",
  "thistle",
  "otter",
  "bramble",
  "sorrel"
];
var RESERVED = /* @__PURE__ */ new Set(["isles", "main"]);
var SEED_WORDS = WORDS.filter((w) => !RESERVED.has(w));
function hashCell(seed, cx, cz, salt = 0) {
  let h = seed ^ Math.imul(cx | 0, 2654435761) ^ Math.imul(cz | 0, 2246822507) ^ Math.imul(salt | 0, 3266489909) | 0;
  h = Math.imul(h ^ h >>> 16, 569420461);
  h = Math.imul(h ^ h >>> 15, 1935289751);
  return (h ^ h >>> 15) >>> 0;
}
function cell01(seed, cx, cz, salt = 0) {
  return hashCell(seed, cx, cz, salt) / 4294967296;
}
function cellNoise(seed, cx, cz, scale, salt = 0) {
  const x = cx / scale, z = cz / scale;
  const ix = Math.floor(x), iz = Math.floor(z);
  const fx = x - ix, fz = z - iz;
  const sx = fx * fx * (3 - 2 * fx), sz = fz * fz * (3 - 2 * fz);
  const v = (ax, az) => cell01(seed, ax, az, 9001 + salt);
  const a = v(ix, iz) + (v(ix + 1, iz) - v(ix, iz)) * sx;
  const b = v(ix, iz + 1) + (v(ix + 1, iz + 1) - v(ix, iz + 1)) * sx;
  return a + (b - a) * sz;
}

// src/world/noise.ts
var F2 = 0.5 * (Math.sqrt(3) - 1);
var G2 = (3 - Math.sqrt(3)) / 6;

// src/world/regions.ts
var HOT = [0.7, 2];
var WARM = [0.55, 0.85];
var COOL = [0.3, 0.6];
var COLD = [-1, 0.4];
var WET = [0.55, 2];
var DRY = [-1, 0.45];
var REGIONS = {
  // ================================================================================== 1 · the Sands
  sands: {
    kind: "sands",
    title: "the Sands",
    label: "desert",
    temp: HOT,
    wet: DRY,
    cells: ["full", "small", "continent"],
    mega: true,
    islandSuffix: [" Sands", " Sands", " Dunes", " Erg"],
    moisture: 0.15,
    arid: 1,
    temperature: 0.9,
    hills: 0.25,
    erg: true,
    cliff: 0,
    massif: 0.35,
    peakCap: 12,
    snowLine: null,
    water: 0.4,
    falloff: 1.25,
    beaches: 4,
    farms: 0,
    pistes: 0,
    quarries: 1,
    lakes: 3,
    rivers: 0.6,
    dryRivers: true,
    forbid: ["karstTowers", "mangroves", "fjordCoast", "glacier"],
    palette: {
      plains: 15124879,
      hills: 14464890,
      forest: 13217904,
      desert: 15323290,
      canyon: 13208154,
      beach: 15917744,
      mountainLow: 11565653,
      mountainHigh: 12883060,
      cliffTop: 14267002,
      track: 14071689,
      lawn: 12108906,
      park: 11057246,
      cliffEarthTop: 13208154,
      cliffEarthBot: 9062962,
      cliffPaleTop: 14729616,
      cliffPaleBot: 11569754,
      bedMud: 15258790,
      bedSand: 15654062,
      dune: 15781772,
      duneShade: 14200428,
      waterShallow: 6276288,
      waterMid: 3119784,
      waterDeep: 2785190
    },
    crops: [7313982, 8890442, 6261306],
    cropKinds: ["maize", "wheat", "maize"],
    sky: 15918796,
    atlas: 14725216,
    treeDensity: { forest: 0.05, hills: 0.01, plains: 6e-3, beach: 0.03 },
    trees: { datePalm: 1 },
    clutter: { shrub: 0.03 },
    cacti: false,
    herds: ["camels"],
    towns: ["city", "suburb", "rural", "rural"],
    tier: { metro: "city" },
    site: "oasis",
    taste: { adobe: 10, mediterranean: 0.6 },
    surface: { rural: "dirt", suburb: "dirt", city: "brick", metro: "brick", mega: "paved" },
    names: [["Qasr ", "Wadi ", "Ain ", "Bir ", "Ras ", "Dar ", "Tel "], ["Zahra", "Amal", "Nour", "Sahel", "Rimal", "Kheir", "Salam", "Anbar", "Jadid", "Hamra", "Safi", "Tamr"]],
    swaps: { church: "mosque", chapel: null, waterTower: "windTower", field: "dateGrove", cityHall: "kasbah", windTurbine: null, lakePark: null, iceRink: null, cemetery: null },
    grid: 4,
    country: [{ kind: "tentCamp", count: 3, w: 3, d: 3, where: "any" }],
    wonder: { kind: "pyramid", name: ["the Great Pyramid", "the Pyramids of ", "the Sun Pyramid"], w: 20, d: 30, where: "flat" },
    lamp: "torch"
  },
  // ================================================================================== 2 · the Icefield
  icefield: {
    kind: "icefield",
    title: "the Icefield",
    label: "glacial",
    temp: COLD,
    cells: ["full", "small", "continent"],
    islandSuffix: [" Icefield", " Glacier", " Land", " Fjord"],
    temperature: 0.05,
    moisture: 0.55,
    massif: 0.95,
    cliff: 0.8,
    snowLine: 2.4,
    water: 0.45,
    falloff: 1.3,
    beaches: 0,
    farms: 0,
    pistes: 3,
    quarries: 1,
    airfields: 1,
    forbid: ["volcano", "mangroves", "dunes", "badlands", "mesaCountry", "karstTowers", "saltFlat", "hotSprings"],
    palette: {
      plains: 10135698,
      hills: 10858656,
      forest: 8361094,
      beach: 13225156,
      cliffTop: 11844784,
      mountainLow: 6120299,
      mountainHigh: 8160140,
      snow: 16054524,
      track: 12170150,
      lawn: 10925724,
      park: 10005904,
      cliffGreyTop: 6120299,
      cliffGreyBot: 3817286,
      cliffPaleTop: 13161692,
      cliffPaleBot: 8360865,
      waterShallow: 7051440,
      waterMid: 3103352,
      waterDeep: 1916252
    },
    sky: 13952244,
    atlas: 13624306,
    treeDensity: { forest: 0.45, hills: 0.05, plains: 0.02, beach: 0 },
    trees: { spruce: 1 },
    clutter: { snowFence: 4e-3, iceberg: 0 },
    herds: ["sledTeam"],
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city" },
    site: "valley",
    taste: { timber: 10, victorian: 0.5 },
    surface: { rural: "paved", suburb: "paved", city: "paved", metro: "paved" },
    names: [["Skar", "Hvit", "Kald", "Bj\xF8rn", "Ulv", "Stor", "Is", "Rav", "Frost", "Sild"], ["vik", "fjord", "nes", "dal", "havn", "\xF8y", "berg"]],
    swaps: { field: null, gas: "fuelDepot", vineyard: null, orchard: null, lido: "sauna", waterPark: null, windTurbine: null, greenhouse: null },
    country: [{ kind: "researchStation", count: 1, w: 3, d: 3, where: "any" }, { kind: "fishRacks", count: 3, w: 2, d: 1, where: "shore" }],
    wonder: { kind: "iceHotel", name: ["the Ice Hotel", "the Aurora Hotel", "the Frost Palace"], w: 12, d: 12, where: "lake" }
  },
  // ================================================================================== 3 · the Shallows
  shallows: {
    kind: "shallows",
    title: "the Shallows",
    label: "resort",
    temp: [0.55, 2],
    cells: ["full", "small"],
    mega: true,
    islandSuffix: [" Cay", " Key", " Keys", " Cay"],
    moisture: 0.5,
    hills: 0,
    massif: 0,
    relief: 0.45,
    cap: 3,
    cliff: 0,
    shelf: 12,
    snowLine: null,
    water: 0.5,
    falloff: 1.2,
    beaches: 8,
    farms: 0,
    quarries: 0,
    pistes: 0,
    airfields: 1,
    forbid: ["volcano", "caldera", "fjordCoast", "glacier", "mesaCountry", "badlands", "dunes", "karstTowers", "skerries"],
    palette: {
      plains: 10477690,
      hills: 11067518,
      forest: 7127658,
      beach: 16511700,
      cliffTop: 13034394,
      lawn: 10282618,
      park: 8836712,
      track: 15260596,
      bedSand: 16182472,
      cliffPaleTop: 16182480,
      cliffPaleBot: 14207140,
      waterShallow: 7333596,
      waterMid: 2933972,
      waterDeep: 1740752
    },
    sky: 12905720,
    atlas: 6217942,
    treeDensity: { forest: 0.5, hills: 0.12, plains: 0.05, beach: 0.12 },
    trees: { palm: 1 },
    clutter: { beachHut: 0.012 },
    herds: ["surfers"],
    beachAllYear: true,
    towns: ["city", "suburb", "suburb", "rural"],
    site: "coast",
    taste: { pastel: 10, mediterranean: 1, modernist: 0.5 },
    surface: { rural: "brick", suburb: "brick", city: "brick", metro: "brick" },
    names: [["Coral", "Palm", "Conch", "Pelican", "Sunset", "Marlin", "Pearl", "Driftwood", "Flamingo", "Mango"], [" Cay", " Key", "side", " Beach", " Point", " Harbour"]],
    swaps: { church: "chapel", diner: "tikiBar", field: "golfCourse", promenade: "boardwalk", windTurbine: null, iceRink: null, cemetery: null, factory: null, powerPlant: null, prison: null },
    country: [{ kind: "beachClub", count: 2, w: 3, d: 2, where: "shore" }],
    wonder: { kind: "pleasurePier", name: ["the Pleasure Pier", "the Grand Pier", "the Sunset Pier"], w: 5, d: 26, where: "shore" }
  },
  // ================================================================================== 4 · the Green
  green: {
    kind: "green",
    title: "the Green",
    label: "rainforest",
    temp: HOT,
    wet: WET,
    cells: ["full", "continent"],
    islandSuffix: [" Jungle", " Isle", " Selva", " Wilds"],
    moisture: 0.95,
    arid: 0,
    temperature: 0.85,
    hills: 0.7,
    cliff: 0.2,
    snowLine: null,
    water: 0.42,
    falloff: 1.3,
    beaches: 1,
    farms: 3,
    quarries: 0,
    pistes: 0,
    rivers: 1.3,
    forbid: ["dunes", "saltFlat", "glacier", "badlands", "mesaCountry", "fjordCoast"],
    palette: {
      plains: 5217860,
      hills: 4689726,
      forest: 3111478,
      beach: 16052448,
      cliffTop: 5016128,
      track: 9071176,
      lawn: 5941322,
      park: 4889152,
      bedMud: 6966836,
      cliffEarthTop: 5916210,
      cliffEarthBot: 3812384,
      mountainLow: 4876868,
      mountainHigh: 6978150,
      waterShallow: 9079386,
      waterMid: 6974018,
      waterDeep: 3108746
    },
    crops: [9091130, 5937716, 10142282],
    cropKinds: ["maize", "pumpkin", "maize"],
    sky: 14215384,
    atlas: 2058796,
    treeDensity: { forest: 0.98, hills: 0.7, plains: 0.35, beach: 0.08 },
    trees: { canopy: 3, round: 2, palm: 0.6 },
    clutter: { fern: 0.08 },
    herds: [],
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city" },
    site: "clearing",
    taste: { colonial: 10, victorian: 0.5 },
    surface: { rural: "dirt", suburb: "dirt", city: "dirt", metro: "paved" },
    names: [["Rio ", "Selva ", "Monte ", "Puerto ", "Villa ", "San "], ["Verde", "Claro", "Negro", "Alto", "Dorado", "Lindo", "Palma", "Bonito", "Oscuro"]],
    swaps: { store: "tradingPost", barn: "plantation", field: "field", windTurbine: null, iceRink: null, windmill: null },
    margin: 2,
    country: [{ kind: "canopyWalkway", count: 1, w: 4, d: 4, where: "any" }, { kind: "stiltHouse", count: 4, w: 1, d: 1, where: "shore" }],
    wonder: { kind: "lostTemple", name: ["the Lost Temple", "the Temple of the Jaguar", "the Sunken Temple"], w: 12, d: 12, where: "flat" }
  },
  // ================================================================================== 5 · the Heights
  heights: {
    kind: "heights",
    title: "the Heights",
    label: "alpine",
    temp: COOL,
    cells: ["full", "continent"],
    islandSuffix: [" Alps", " Highlands", " Peaks", " Massif"],
    temperature: 0.35,
    massif: 0.9,
    relief: 0.6,
    cliff: 0.4,
    snowLine: 6.8,
    water: 0.44,
    falloff: 1.3,
    pistes: 3,
    farms: 3,
    quarries: 2,
    forbid: ["dunes", "mangroves", "saltFlat", "badlands", "mesaCountry", "karstTowers"],
    palette: {
      plains: 8832092,
      hills: 9750630,
      forest: 4160090,
      beach: 14209728,
      cliffTop: 9091170,
      mountainLow: 9407106,
      mountainHigh: 11051930,
      track: 12166015,
      waterShallow: 10475222,
      waterMid: 7322824,
      waterDeep: 4889272
    },
    sky: 14085370,
    atlas: 9416312,
    treeDensity: { forest: 0.8, hills: 0.16, plains: 0.03, beach: 0 },
    trees: { spruce: 3, pine: 1.5, birch: 0.5 },
    clutter: { haystack: 6e-3 },
    herds: ["cattle", "hikers"],
    towns: ["metro", "city", "suburb", "rural"],
    site: "valley",
    taste: { chalet: 10, tudor: 1.5, victorian: 0.4, classic: 0.4 },
    surface: { rural: "paved", suburb: "paved" },
    names: [["Adler", "Hoch", "Schnee", "Wald", "Gr\xFCn", "Stein", "Kirch", "Linden", "Roth", "Edel"], ["berg", "tal", "matt", "bruck", "horn", "alp", "bach"]],
    swaps: { waterTower: null, barn: "dairy", windTurbine: null, vineyard: null, waterPark: null },
    country: [{ kind: "mountainHut", count: 3, w: 2, d: 2, where: "high" }, { kind: "monastery", count: 1, w: 3, d: 3, where: "high" }],
    wonder: { kind: "dam", name: ["the Great Dam", "the High Dam", "the Valley Dam"], w: 16, d: 6, where: "valley" }
  },
  // ================================================================================== 6 · the Moors
  moors: {
    kind: "moors",
    title: "the Moors",
    label: "moorland",
    temp: COOL,
    wet: WET,
    cells: ["full", "small", "continent"],
    islandSuffix: [" Moor", " Moors", " Heath", " Fell"],
    moisture: 0.6,
    temperature: 0.4,
    hills: 1,
    hillAmp: 1.25,
    massif: 0,
    cliff: 0.75,
    water: 0.5,
    falloff: 1.35,
    beaches: 1,
    farms: 3,
    pistes: 0,
    quarries: 2,
    lakes: 4,
    forbid: ["dunes", "mangroves", "saltFlat", "mesaCountry", "karstTowers", "volcano"],
    palette: {
      plains: 9083486,
      hills: 9071230,
      forest: 6254672,
      beach: 13156518,
      cliffTop: 9075312,
      mountainLow: 6973024,
      mountainHigh: 8419956,
      track: 10127978,
      lawn: 9085026,
      park: 8033368,
      cliffGreyTop: 6249560,
      cliffGreyBot: 3947064,
      cliffPaleTop: 8025714,
      cliffPaleBot: 4867908,
      cliffEarthTop: 5917242,
      cliffEarthBot: 3812900,
      marsh: 5921338,
      waterShallow: 5925474,
      waterMid: 3820104,
      waterDeep: 2767428
    },
    crops: [9085008, 10135648, 8032328],
    cropKinds: ["wheat", "wheat", "pumpkin"],
    sky: 14212320,
    atlas: 9330566,
    treeDensity: { forest: 0.01, hills: 6e-3, plains: 4e-3, beach: 0 },
    trees: { windPine: 1 },
    clutter: { heather: 0.14, drystoneWall: 0 },
    herds: ["sheep", "hikers"],
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city" },
    site: "lee",
    taste: { granite: 10, tudor: 1.2, victorian: 0.6, gothic: 0.3 },
    surface: { rural: "dirt" },
    names: [["Glen", "Kirk", "Heather", "Black", "Stone", "Raven", "Grey", "High", "Low", "Peat"], ["more", "beck", "stow", "burn", "hope", "gill", "dale", "law"]],
    swaps: { store: "pub", vineyard: null, orchard: null, windTurbine: null, waterPark: null, lido: null },
    country: [
      { kind: "croft", count: 7, w: 1, d: 1, where: "any" },
      { kind: "bothy", count: 2, w: 1, d: 1, where: "high" },
      { kind: "distillery", count: 1, w: 2, d: 2, where: "low" },
      { kind: "cairnField", count: 1, w: 3, d: 3, where: "high" }
    ],
    wonder: { kind: "castle", name: ["Castle ", "the Keep of ", "Dun "], w: 12, d: 12, where: "high" }
  },
  // ================================================================================== 7 · the Veldt
  veldt: {
    kind: "veldt",
    title: "the Veldt",
    label: "savanna",
    temp: HOT,
    wet: [-1, 0.55],
    cells: ["full", "continent"],
    islandSuffix: [" Veldt", " Plains", " Land", " Savanna"],
    moisture: 0.4,
    arid: 0.3,
    temperature: 0.85,
    hills: 0.35,
    hillAmp: 0.5,
    massif: 0.45,
    peakCap: 8,
    relief: 0.8,
    snowLine: null,
    water: 0.42,
    falloff: 1.3,
    lakes: 5,
    rivers: 0.5,
    farms: 2,
    pistes: 0,
    forbid: ["glacier", "fjordCoast", "karstTowers", "mangroves", "dunes"],
    palette: {
      plains: 14072938,
      hills: 13216346,
      forest: 10133584,
      beach: 15127200,
      cliffTop: 13150304,
      desert: 14203e3,
      mountainLow: 10119754,
      mountainHigh: 11172442,
      track: 12614218,
      lawn: 12105826,
      park: 11054168,
      cliffEarthTop: 11034682,
      cliffEarthBot: 6961700,
      bedMud: 11045466,
      waterShallow: 10132074,
      waterMid: 8030826,
      waterDeep: 4881034
    },
    crops: [13148224, 12098106, 11055176],
    cropKinds: ["wheat", "sunflower", "maize"],
    sky: 15659236,
    atlas: 14200906,
    treeDensity: { forest: 0.2, hills: 0.03, plains: 0.03, beach: 0.02 },
    trees: { acacia: 6, baobab: 0.25 },
    clutter: { termiteMound: 0.012 },
    cacti: false,
    herds: ["herd"],
    towns: ["city", "rural", "rural"],
    tier: { metro: "city", suburb: "rural" },
    site: "valley",
    taste: { colonial: 10, mediterranean: 0.4 },
    surface: { rural: "dirt", suburb: "dirt", city: "dirt", metro: "dirt" },
    names: [["Ol", "Bara", "Ma", "Ka", "Tsa", "Ngo", "Ze", "Lo"], ["wana", "kwe", "boro", "sasa", "lani", "tembo", "nyati", "kopje"]],
    swaps: { church: "chapel", field: "kraal", house: "rondavel", windTurbine: "pumpWindmill", iceRink: null, windmill: "pumpWindmill" },
    lamp: "none",
    country: [
      { kind: "safariLodge", count: 2, w: 3, d: 3, where: "high" },
      { kind: "rangerPost", count: 2, w: 1, d: 1, where: "any" },
      { kind: "pumpWindmill", count: 4, w: 1, d: 1, where: "low" },
      { kind: "reserveGate", count: 1, w: 3, d: 1, where: "any" }
    ],
    wonder: { kind: "greatKopje", name: ["the Great Kopje", "Lion Rock", "the Sentinel Kopje"], w: 12, d: 12, where: "flat" }
  },
  // ================================================================================== 8 · the Fens
  fens: {
    kind: "fens",
    title: "the Fens",
    label: "bayou",
    temp: WARM,
    wet: WET,
    cells: ["full", "small", "continent"],
    coastal: true,
    islandSuffix: [" Bayou", " Fens", " Marsh", " Isle"],
    moisture: 0.9,
    temperature: 0.75,
    hills: 0.15,
    massif: 0,
    relief: 0.5,
    cap: 2,
    cliff: 0,
    snowLine: null,
    water: 0.4,
    falloff: 1.2,
    beaches: 0,
    farms: 3,
    quarries: 0,
    pistes: 0,
    lakes: 3,
    rivers: 1.6,
    forbid: ["dunes", "saltFlat", "glacier", "mesaCountry", "badlands", "fjordCoast", "volcano", "caldera", "skerries"],
    palette: {
      plains: 9083474,
      hills: 9873504,
      forest: 5926976,
      beach: 12103818,
      cliffTop: 9083482,
      marsh: 6978116,
      track: 10127970,
      lawn: 9743450,
      park: 8822864,
      bedMud: 4870710,
      waterShallow: 5925444,
      waterMid: 4149816,
      waterDeep: 3099204
    },
    crops: [10142298, 9091146, 11061354],
    sky: 15001300,
    atlas: 7305786,
    treeDensity: { forest: 0.6, hills: 0.1, plains: 0.05, beach: 0.02 },
    trees: { cypress: 3, round: 1 },
    clutter: { reeds: 0.06 },
    herds: [],
    towns: ["city", "rural", "rural"],
    tier: { metro: "city", suburb: "rural" },
    site: "levee",
    taste: { colonial: 10, pastel: 1 },
    surface: { rural: "dirt", suburb: "dirt", city: "brick", metro: "brick" },
    names: [["Bayou ", "Belle ", "Pointe ", "Isle ", "Grand ", "Petit "], ["Rouge", "Noire", "Ch\xEAne", "Marais", "Lafitte", "Blanc", "Teche", "Terrebonne"]],
    swaps: { store: "generalStore", field: "riceTerraces", windTurbine: null, iceRink: null, windmill: null, factory: null },
    country: [
      { kind: "shrimpShack", count: 3, w: 1, d: 1, where: "shore" },
      { kind: "stiltHouse", count: 6, w: 1, d: 1, where: "water" },
      { kind: "airboatDock", count: 2, w: 2, d: 2, where: "water" }
    ],
    wonder: { kind: "plantationManor", name: ["Belle Rive", "Oak Alley", "Magnolia Hall"], w: 12, d: 16, where: "flat" }
  },
  // ================================================================================== 9 · the Terraces
  terraces: {
    kind: "terraces",
    title: "the Terraces",
    label: "blossom",
    temp: WARM,
    wet: WET,
    cells: ["full", "continent"],
    islandSuffix: [" Shima", " Isle", " Terraces", " Hills"],
    moisture: 0.75,
    hills: 0.9,
    hillAmp: 1.15,
    massif: 0.55,
    cliff: 0.4,
    snowLine: 8.5,
    water: 0.46,
    falloff: 1.3,
    farms: 5,
    rivers: 1.4,
    pistes: 1,
    forbid: ["dunes", "saltFlat", "badlands", "mesaCountry", "fjordCoast"],
    palette: {
      plains: 8176730,
      hills: 8831068,
      forest: 4889162,
      beach: 15261892,
      cliffTop: 8041562,
      track: 11573876,
      cliffEarthTop: 5916214,
      cliffEarthBot: 3811872,
      lawn: 8702048,
      park: 6993998,
      waterShallow: 10146016,
      waterMid: 6466256,
      waterDeep: 3835072
    },
    crops: [9097322, 8044634, 10147962],
    sky: 14740724,
    atlas: 15243960,
    autumn: 13119530,
    blossom: true,
    treeDensity: { forest: 0.85, hills: 0.18, plains: 0.05, beach: 0.02 },
    trees: { cherry: 3, pine: 1.2, bamboo: 1.2 },
    clutter: { stoneLantern: 3e-3 },
    herds: ["pilgrims"],
    towns: ["metro", "city", "suburb", "rural"],
    site: "valley",
    taste: { eastern: 10, japanese: 5 },
    surface: { rural: "brick", suburb: "brick", city: "brick", metro: "brick" },
    names: [["Kawa", "Yama", "Taka", "Matsu", "Sakura", "Mori", "Hana", "Kiri", "Tsuki", "Ishi"], ["yama", "saki", "mura", "hara", "shima", "kawa", "no", "dera"]],
    swaps: { church: "temple", chapel: "shrine", waterTower: "pagoda", field: "riceTerraces", plaza: "shrine", windTurbine: null, lido: "bathhouse", diner: "teaHouse", fastFood: null },
    lamp: "lantern",
    country: [{ kind: "shrine", count: 5, w: 1, d: 1, where: "any" }, { kind: "teaHouse", count: 1, w: 2, d: 2, where: "low" }, { kind: "bathhouse", count: 1, w: 2, d: 2, where: "high" }],
    wonder: { kind: "hilltopCastle", name: ["White Heron Castle", "Crane Castle", "Moon Castle"], w: 12, d: 12, where: "high" }
  },
  // ================================================================================== 10 · the Whitewash
  whitewash: {
    kind: "whitewash",
    title: "the Whitewash",
    label: "aegean",
    temp: WARM,
    wet: DRY,
    cells: ["full", "small"],
    islandSuffix: [" Island", " Isle", "os", " Nisos"],
    moisture: 0.28,
    arid: 0.2,
    hills: 1,
    hillAmp: 1.1,
    massif: 0.3,
    peakCap: 14,
    cliff: 0.9,
    snowLine: null,
    water: 0.55,
    falloff: 1.4,
    beaches: 2,
    farms: 1,
    quarries: 1,
    pistes: 0,
    forbid: ["glacier", "fjordCoast", "mangroves", "dunes", "karstTowers", "saltFlat"],
    palette: {
      plains: 11842170,
      hills: 10134654,
      forest: 8030810,
      beach: 15787720,
      cliffTop: 13155476,
      track: 14207136,
      mountainLow: 13153418,
      mountainHigh: 14207140,
      cliffPaleTop: 15262422,
      cliffPaleBot: 11840672,
      cliffEarthTop: 14208176,
      cliffEarthBot: 11049600,
      lawn: 11055216,
      park: 10003558,
      waterShallow: 4114624,
      waterMid: 1734848,
      waterDeep: 1003432
    },
    crops: [9083482, 8010330, 10133600],
    cropKinds: ["maize", "pumpkin", "wheat"],
    sky: 13823228,
    atlas: 16054002,
    treeDensity: { forest: 0.25, hills: 0.05, plains: 0.03, beach: 0.02 },
    trees: { olive: 3, cypress: 0, poplar: 0.4, stonePine: 0.6 },
    clutter: { shrub: 0.05, mooredBoat: 0 },
    herds: ["goats"],
    towns: ["suburb", "rural", "rural"],
    site: "cliffTop",
    taste: { cycladic: 10 },
    surface: { rural: "brick", suburb: "brick", city: "brick" },
    names: [["Ano ", "Kato ", "Agios ", "Nea ", "Paleo ", "Kala "], ["Chora", "Lefkos", "Petra", "Oia", "Kamari", "Livadi", "Ormos", "Pyrgos"]],
    swaps: { church: "blueChapel", chapel: "blueChapel", windTurbine: "windmill", field: "vineyard", amphitheatre: "stoneAmphitheatre", diner: "taverna", pub: "taverna", iceRink: null },
    grid: 4,
    margin: 0.5,
    country: [{ kind: "blueChapel", count: 4, w: 1, d: 1, where: "high" }, { kind: "windmillRow", count: 1, w: 3, d: 1, where: "high" }],
    wonder: { kind: "capeTemple", name: ["the Temple of Poseidon", "the Cape Temple", "the Temple of the Winds"], w: 12, d: 12, where: "cape" }
  },
  // ================================================================================== 11 · the Golden Hills
  goldhills: {
    kind: "goldhills",
    title: "the Golden Hills",
    label: "golden hills",
    temp: WARM,
    wet: [-1, 0.5],
    cells: ["continent"],
    islandSuffix: [" Hills", " Downs", " Ranch", " Valley"],
    // smooth rounded hills, gold in the dry season: the Coast Ranges in summer
    moisture: 0.36,
    arid: 0.12,
    temperature: 0.7,
    hills: 1,
    hillAmp: 0.4,
    massif: 0.15,
    peakCap: 10,
    relief: 1,
    cliff: 0.35,
    snowLine: null,
    water: 0.45,
    falloff: 1.3,
    beaches: 1,
    farms: 3,
    quarries: 1,
    pistes: 0,
    rivers: 0.8,
    lakes: 1,
    forbid: ["dunes", "glacier", "mangroves", "karstTowers", "fjordCoast", "badlands", "saltFlat"],
    palette: {
      plains: 14466146,
      hills: 13807703,
      forest: 5927992,
      beach: 15260856,
      cliffTop: 13610588,
      desert: 14202992,
      mountainLow: 11047014,
      mountainHigh: 12099706,
      track: 13149810,
      lawn: 12106850,
      park: 11055192,
      cliffEarthTop: 12094034,
      cliffEarthBot: 8017460,
      bedMud: 10125400,
      waterShallow: 6992064,
      waterMid: 4164264,
      waterDeep: 2781080
    },
    crops: [6261306, 7313986, 9075258],
    cropKinds: ["maize", "wheat", "pumpkin"],
    sky: 15397108,
    atlas: 14201418,
    treeDensity: { forest: 0.35, hills: 0.035, plains: 0.02, beach: 0.01 },
    trees: { oak: 6, olive: 0.6, cypress: 0.25 },
    clutter: { shrub: 0.02 },
    cacti: false,
    herds: ["cattle"],
    towns: ["city", "suburb", "rural", "rural"],
    tier: { metro: "city" },
    site: "valley",
    taste: { mediterranean: 6, colonial: 2, adobe: 1, western: 2.5 },
    surface: { rural: "dirt" },
    names: [["San ", "Santa ", "Los ", "El ", "Rio ", "Paso "], ["Rosa", "Clara", "Luis", "Cruz", "Robles", "Olivos", "Oro", "Lomas", "Altos", "Ynez", "Benito", "Mateo"]],
    swaps: { field: "vineyard", barn: "wineryBarn", farmhouse: "ranchHouse", iceRink: null, windTurbine: null },
    country: [{ kind: "ranchHouse", count: 4, w: 2, d: 2, where: "any" }, { kind: "wineryBarn", count: 2, w: 3, d: 2, where: "low" }, { kind: "vineyard", count: 3, w: 3, d: 3, where: "low" }],
    wonder: { kind: "mission", name: ["Mission San ", "the Old Mission", "Mission Santa "], w: 12, d: 12, where: "flat" }
  },
  // ================================================================================== 12 · the Fire Coast
  firecoast: {
    kind: "firecoast",
    title: "the Fire Coast",
    label: "volcanic",
    temp: [0.15, 0.8],
    cells: ["full", "continent"],
    coastal: true,
    islandSuffix: [" Fire Isle", " Cinders", " Ashlands", " Isle"],
    // black sand, moss over old flows and a cone that still smokes: Iceland, Hawaii, the Canaries
    moisture: 0.5,
    arid: 0,
    temperature: 0.42,
    hills: 0.65,
    hillAmp: 0.9,
    massif: 0.6,
    peakCap: 16,
    cliff: 0.7,
    water: 0.45,
    falloff: 1.3,
    beaches: 3,
    farms: 1,
    quarries: 1,
    pistes: 0,
    lakes: 2,
    rivers: 0.7,
    forbid: ["karstTowers", "mangroves", "dunes", "saltFlat", "mesaCountry", "badlands", "caldera"],
    ask: ["volcano"],
    lava: 0.5,
    palette: {
      plains: 7309912,
      hills: 6452058,
      forest: 5204554,
      beach: 3025968,
      cliffTop: 5000266,
      desert: 5919824,
      canyon: 6961712,
      mountainLow: 3814968,
      mountainHigh: 4998728,
      track: 4867652,
      lawn: 8034910,
      park: 6982226,
      cliffGreyTop: 3815482,
      cliffGreyBot: 2236452,
      cliffEarthTop: 4864566,
      cliffEarthBot: 2761252,
      cliffPaleTop: 5920344,
      cliffPaleBot: 3420726,
      bedSand: 3815484,
      bedMud: 3025966,
      waterShallow: 5218472,
      waterMid: 2781064,
      waterDeep: 1920880
    },
    sky: 14672614,
    atlas: 5914438,
    treeDensity: { forest: 0.14, hills: 0.02, plains: 0.01, beach: 0 },
    trees: { birch: 2, spruce: 1 },
    clutter: { shrub: 0.03, heather: 0.06 },
    herds: ["sheep", "hikers"],
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city" },
    site: "coast",
    taste: { timber: 6, norse: 4 },
    surface: {},
    names: [["Reykja", "Akur", "H\xFAsa", "Borgar", "Eld", "Hraun", "Sel", "Grinda", "Hvera", "Skaga"], ["v\xEDk", "nes", "fj\xF6r\xF0ur", "dalur", "eyri", "holt", "ger\xF0i"]],
    swaps: { church: "blackChurch", chapel: "blackChurch", lido: "hotSpringBaths", waterPark: "hotSpringBaths", powerPlant: "geothermalPlant", windTurbine: null, vineyard: null, orchard: null },
    country: [{ kind: "geothermalPlant", count: 1, w: 4, d: 3, where: "any" }, { kind: "hotSpringBaths", count: 1, w: 3, d: 3, where: "low" }, { kind: "turfHouse", count: 5, w: 1, d: 1, where: "any" }],
    wonder: { kind: "greatGeyser", name: ["the Great Geysir", "Old Steamer", "the Geyser of "], w: 12, d: 12, where: "flat" }
  },
  // ================================================================================== 13 · the Fjordlands
  fjords: {
    kind: "fjords",
    title: "the Fjordlands",
    label: "fjord",
    temp: [-1, 0.52],
    cells: ["full", "small", "continent"],
    coastal: true,
    islandSuffix: [" Fjord", " Sound", " Fjords", " Land"],
    // the sea let deep into high green country: every road a ferry, a tunnel or a climb
    temperature: 0.28,
    moisture: 0.65,
    hills: 0.7,
    massif: 0.9,
    cliff: 1,
    fjords: true,
    snowLine: 7.5,
    water: 0.5,
    falloff: 1.3,
    beaches: 0,
    farms: 2,
    pistes: 1,
    quarries: 1,
    rivers: 1.3,
    lakes: 2,
    forbid: ["volcano", "caldera", "mangroves", "dunes", "badlands", "mesaCountry", "karstTowers", "saltFlat"],
    palette: {
      plains: 8038496,
      hills: 8825960,
      forest: 4157006,
      beach: 12104872,
      cliffTop: 8033384,
      mountainLow: 6975090,
      mountainHigh: 8817296,
      track: 11050634,
      lawn: 8696934,
      park: 7775836,
      cliffGreyTop: 6448748,
      cliffGreyBot: 3817028,
      cliffPaleTop: 9080466,
      cliffPaleBot: 5527646,
      waterShallow: 5216928,
      waterMid: 2452092,
      waterDeep: 1326938
    },
    sky: 14214896,
    atlas: 4160138,
    treeDensity: { forest: 0.75, hills: 0.14, plains: 0.03, beach: 0 },
    trees: { spruce: 3, birch: 1.5, pine: 1 },
    clutter: { haystack: 4e-3, mooredBoat: 0.05 },
    herds: ["sheep", "hikers"],
    towns: ["city", "suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city" },
    site: "coast",
    taste: { norse: 8, timber: 4 },
    surface: {},
    names: [["Sogn", "Hardang", "Geirang", "Lyse", "N\xE6r", "Aur", "Stav", "Ber", "Trond", "\xC5le", "Fl\xE5", "Bal"], ["fjord", "vik", "dal", "sund", "nes", "heim", "vang", "strand"]],
    swaps: { chapel: "staveChurch", vineyard: null, windTurbine: null, waterPark: null, lido: "sauna" },
    country: [{ kind: "boatShed", count: 5, w: 1, d: 1, where: "shore" }, { kind: "rorbu", count: 5, w: 1, d: 1, where: "shore" }, { kind: "fishFarm", count: 2, w: 3, d: 3, where: "water" }],
    wonder: { kind: "arcticCathedral", name: ["the Arctic Cathedral", "the Cathedral of the Northern Lights", "the Ice Sea Cathedral"], w: 12, d: 12, where: "lake" }
  },
  // ================================================================================== 14 · the Big Timber
  bigtimber: {
    kind: "bigtimber",
    title: "the Big Timber",
    label: "giant forest",
    temp: [0.2, 0.66],
    wet: [0.48, 2],
    cells: ["full", "small", "continent"],
    islandSuffix: [" Timber", " Woods", " Forest", " Sound"],
    // trees three hundred feet tall, logging roads and float-plane lakes: the Pacific Northwest, Siberia
    moisture: 0.95,
    arid: 0,
    temperature: 0.4,
    hills: 0.75,
    hillAmp: 1.1,
    massif: 0.6,
    cliff: 0.6,
    snowLine: 8.5,
    water: 0.44,
    falloff: 1.3,
    beaches: 1,
    farms: 0,
    pistes: 1,
    quarries: 1,
    lakes: 5,
    rivers: 1.4,
    forbid: ["dunes", "saltFlat", "badlands", "mesaCountry", "karstTowers", "mangroves"],
    palette: {
      plains: 6263888,
      hills: 5540426,
      forest: 2907194,
      beach: 11052180,
      cliffTop: 5930060,
      track: 8020552,
      mountainLow: 5923422,
      mountainHigh: 8028798,
      lawn: 6725716,
      park: 5804618,
      cliffEarthTop: 5916210,
      cliffEarthBot: 3812384,
      bedMud: 5917238,
      waterShallow: 5936784,
      waterMid: 3107442,
      waterDeep: 1919580
    },
    sky: 13951708,
    atlas: 2050612,
    treeDensity: { forest: 0.95, hills: 0.6, plains: 0.3, beach: 0.02 },
    trees: { redwood: 3, spruce: 2, pine: 1 },
    clutter: { fern: 0.1 },
    herds: ["hikers"],
    towns: ["city", "suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city" },
    site: "clearing",
    taste: { timber: 6, western: 2, chalet: 2, victorian: 0.6 },
    surface: { rural: "dirt" },
    names: [["Cedar", "Spruce", "Fir", "Eagle", "Bear", "Salmon", "Moss", "Raven", "Timber", "Otter", "Elk", "Hemlock"], [" Creek", " Falls", " Landing", "ton", " Bay", " Hollow", " Mill", " Ridge"]],
    swaps: { field: null, vineyard: null, orchard: null, windTurbine: null, store: "generalStore", factory: "paperMill", clockTower: "lookoutTower" },
    margin: 1.5,
    country: [
      { kind: "loggingCamp", count: 3, w: 3, d: 3, where: "any" },
      { kind: "lookoutTower", count: 3, w: 1, d: 1, where: "high" },
      { kind: "seaplaneBase", count: 2, w: 3, d: 2, where: "water" },
      { kind: "lumberYard", count: 1, w: 5, d: 4, where: "low" }
    ],
    wonder: { kind: "giantTree", name: ["the Grandfather Tree", "the Cathedral Grove", "the Tunnel Tree"], w: 12, d: 12, where: "flat" }
  },
  // ================================================================================== 15 · the Red Rock
  redrock: {
    kind: "redrock",
    title: "the Red Rock",
    label: "badlands",
    temp: [0.5, 2],
    wet: DRY,
    cells: ["continent"],
    islandSuffix: [" Mesa", " Buttes", " Badlands", " Rock"],
    // mesas, buttes and banded canyon walls, a roadhouse every hundred miles: Monument Valley, the Outback
    moisture: 0.12,
    arid: 0.95,
    temperature: 0.85,
    hills: 0.8,
    hillAmp: 0.7,
    massif: 0.2,
    peakCap: 10,
    cliff: 0.5,
    badlands: true,
    snowLine: null,
    water: 0.4,
    falloff: 1.3,
    beaches: 0,
    farms: 0,
    quarries: 2,
    pistes: 0,
    lakes: 1,
    rivers: 0.5,
    dryRivers: true,
    forbid: ["glacier", "mangroves", "karstTowers", "fjordCoast", "volcano", "caldera", "saltFlat", "dunes"],
    ask: ["mesaCountry"],
    palette: {
      plains: 13142618,
      hills: 12548684,
      forest: 10127954,
      desert: 13668444,
      canyon: 12081210,
      beach: 14727312,
      mountainLow: 11031610,
      mountainHigh: 12347980,
      cliffTop: 12877902,
      track: 12087882,
      lawn: 11053152,
      park: 10002520,
      cliffEarthTop: 12081210,
      cliffEarthBot: 8008740,
      cliffPaleTop: 14195306,
      cliffPaleBot: 10509372,
      cliffGreyTop: 10115652,
      cliffGreyBot: 6961708,
      bedMud: 13146732,
      bedSand: 14198904,
      dune: 14195298,
      duneShade: 12087358,
      waterShallow: 6989984,
      waterMid: 4164240,
      waterDeep: 2779782
    },
    sky: 16049360,
    atlas: 12079151,
    treeDensity: { forest: 0.04, hills: 8e-3, plains: 6e-3, beach: 0 },
    trees: { acacia: 1, olive: 0.4 },
    clutter: { shrub: 0.05 },
    cacti: true,
    herds: ["cattle"],
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city", city: "suburb" },
    site: "valley",
    taste: { western: 6, motherroad: 5, adobe: 4 },
    surface: { rural: "dirt", suburb: "dirt" },
    names: [["Red ", "Dry ", "Copper ", "Dust ", "Coyote ", "Iron ", "Bitter ", "Lone ", "Broken ", "Rattlesnake "], ["Mesa", "Gulch", "Springs", "Wash", "Butte", "Creek", "Flat", "Wells", "Junction"]],
    swaps: { store: "generalStore", pub: "roadhouse", motel: "roadhouse", house: "house", field: null, vineyard: null, orchard: null, windTurbine: "pumpWindmill", windmill: "pumpWindmill", iceRink: null, lido: null, waterPark: null, lakePark: null },
    country: [
      { kind: "roadhouse", count: 2, w: 3, d: 2, where: "any" },
      { kind: "pueblo", count: 3, w: 2, d: 2, where: "high" },
      { kind: "dinosaurDig", count: 1, w: 3, d: 3, where: "low" },
      { kind: "pumpWindmill", count: 4, w: 1, d: 1, where: "low" },
      { kind: "mineHeadframe", count: 1, w: 4, d: 4, where: "any" }
    ],
    wonder: { kind: "monumentButtes", name: ["the Three Sisters", "the Mittens", "the Monuments of "], w: 14, d: 14, where: "flat" },
    lamp: "none"
  },
  // ================================================================================== 16 · Dragon Bay
  dragonbay: {
    kind: "dragonbay",
    title: "Dragon Bay",
    label: "karst",
    temp: [0.55, 2],
    wet: WET,
    cells: ["full", "continent"],
    coastal: true,
    islandSuffix: [" Bay", " Towers", " Isles", " Karst"],
    // limestone towers standing out of rice flats and a jade sea: Ha Long, Guilin
    moisture: 0.85,
    arid: 0,
    temperature: 0.82,
    hills: 0.3,
    massif: 0.15,
    peakCap: 8,
    relief: 0.6,
    cliff: 0.6,
    shelf: 10,
    snowLine: null,
    water: 0.52,
    falloff: 1.25,
    beaches: 2,
    farms: 4,
    quarries: 0,
    pistes: 0,
    lakes: 2,
    rivers: 1.4,
    forbid: ["dunes", "saltFlat", "glacier", "badlands", "mesaCountry", "fjordCoast", "volcano", "caldera"],
    ask: ["karstTowers"],
    palette: {
      plains: 8175712,
      hills: 7254616,
      forest: 3967560,
      beach: 15261888,
      cliffTop: 6988888,
      track: 11048048,
      mountainLow: 8030842,
      mountainHigh: 9740948,
      lawn: 8702052,
      park: 7387220,
      cliffGreyTop: 9081992,
      cliffGreyBot: 5923932,
      cliffPaleTop: 11055268,
      cliffPaleBot: 7239792,
      bedMud: 6974024,
      waterShallow: 7329984,
      waterMid: 3715236,
      waterDeep: 2064520
    },
    crops: [9097322, 8044634, 10147962],
    sky: 14479080,
    atlas: 4173460,
    treeDensity: { forest: 0.9, hills: 0.4, plains: 0.06, beach: 0.06 },
    trees: { canopy: 2, bamboo: 2, palm: 0.8, round: 1 },
    clutter: { fern: 0.04, mooredBoat: 0.08 },
    herds: [],
    beachAllYear: true,
    towns: ["city", "suburb", "rural", "rural"],
    tier: { mega: "metro" },
    site: "coast",
    taste: { chinese: 6, seasia: 4, eastern: 2, hongkong: 3 },
    surface: { rural: "dirt", suburb: "brick", city: "brick" },
    names: [["Long ", "Bai ", "Hai ", "Cat ", "Yang", "Gui", "Lan ", "Ha ", "Ninh ", "Xing"], ["Shan", "Wan", "Ba", "Shuo", "Lin", "Hai", "Tien", "Binh", "Ping"]],
    swaps: { church: "temple", chapel: "shrine", waterTower: "karstPagoda", field: "riceTerraces", windTurbine: null, iceRink: null, diner: "teaHouse" },
    lamp: "lantern",
    country: [{ kind: "floatingVillage", count: 2, w: 3, d: 3, where: "water" }, { kind: "junkMooring", count: 4, w: 2, d: 2, where: "shore" }, { kind: "karstPagoda", count: 3, w: 1, d: 1, where: "high" }],
    wonder: { kind: "cloudTemple", name: ["the Temple in the Clouds", "Dragon Gate Temple", "the Jade Pinnacle"], w: 12, d: 12, where: "high" }
  },
  // ================================================================================== 17 · the Steppe
  steppe: {
    kind: "steppe",
    title: "the Steppe",
    label: "steppe",
    temp: [0.12, 0.72],
    wet: [-1, 0.58],
    cells: ["full", "continent"],
    islandSuffix: [" Steppe", " Plain", " Grass", " Land"],
    // grass to every horizon, a track instead of a road and felt tents instead of a town: Mongolia
    moisture: 0.3,
    arid: 0.1,
    temperature: 0.4,
    hills: 0.5,
    hillAmp: 0.45,
    massif: 0.1,
    peakCap: 8,
    relief: 0.7,
    cliff: 0.2,
    water: 0.4,
    falloff: 1.3,
    beaches: 0,
    farms: 0,
    quarries: 0,
    pistes: 0,
    lakes: 3,
    rivers: 0.6,
    forbid: ["dunes", "mangroves", "karstTowers", "fjordCoast", "volcano", "caldera", "mesaCountry", "badlands", "saltFlat"],
    palette: {
      plains: 11057256,
      hills: 10267230,
      forest: 9083990,
      beach: 13682848,
      cliffTop: 10003548,
      desert: 12628080,
      track: 10126424,
      mountainLow: 9077352,
      mountainHigh: 10525312,
      lawn: 11189354,
      park: 10268256,
      cliffEarthTop: 9073224,
      cliffEarthBot: 5917232,
      bedMud: 9075284,
      waterShallow: 6990008,
      waterMid: 4163236,
      waterDeep: 2779796
    },
    sky: 14871796,
    atlas: 11055200,
    treeDensity: { forest: 0.02, hills: 3e-3, plains: 2e-3, beach: 0 },
    trees: { birch: 1, windPine: 1 },
    clutter: { shrub: 0.015 },
    cacti: false,
    herds: ["horses", "sheep", "goats"],
    towns: ["rural", "rural", "rural"],
    tier: { metro: "suburb", mega: "suburb", city: "suburb", suburb: "rural" },
    site: "valley",
    taste: { himalayan: 4, russian: 3, timber: 2 },
    surface: { rural: "dirt", suburb: "dirt", city: "dirt", metro: "dirt" },
    names: [["Ulaan", "Khar", "Tsagaan", "Altan", "Bayan", "Erdene", "Dalan", "M\xF6r\xF6n", "T\xF6m\xF6r", "Kh\xF6kh"], ["gol", "nuur", "uul", "tal", "bulag", "khot", "sum", "dalai"]],
    swaps: { house: "yurt", farmhouse: "yurt", cottage: "yurt", bungalow: "yurt", barn: "horseCorral", field: null, vineyard: null, orchard: null, windTurbine: null, waterPark: null, lido: null, memorial: "ovoo", chapel: "stupa" },
    lamp: "none",
    grid: 5,
    country: [{ kind: "yurtCamp", count: 5, w: 3, d: 3, where: "any" }, { kind: "ovoo", count: 4, w: 1, d: 1, where: "high" }, { kind: "horseCorral", count: 3, w: 3, d: 2, where: "low" }],
    wonder: { kind: "khanStatue", name: ["the Great Khan", "the Horseman of ", "the Steel Rider"], w: 12, d: 12, where: "flat" }
  },
  // ================================================================================== 18 · the Salt Pan
  saltpan: {
    kind: "saltpan",
    title: "the Salt Pan",
    label: "salt flats",
    temp: [0.45, 2],
    wet: DRY,
    cells: ["continent"],
    islandSuffix: [" Salar", " Flats", " Pan", " Salt"],
    // a dry lake bed white to the horizon and flat enough to chase a record on: Bonneville, Uyuni
    moisture: 0.1,
    arid: 0.85,
    temperature: 0.75,
    hills: 0.15,
    hillAmp: 0.4,
    massif: 0.1,
    peakCap: 9,
    relief: 0.35,
    cliff: 0.1,
    snowLine: null,
    water: 0.4,
    falloff: 1.3,
    beaches: 0,
    farms: 0,
    quarries: 1,
    pistes: 0,
    lakes: 0,
    rivers: 0.3,
    dryRivers: true,
    forbid: ["glacier", "mangroves", "karstTowers", "fjordCoast", "volcano", "caldera", "mesaCountry", "dunes"],
    ask: ["saltFlat"],
    salt: 0.5,
    palette: {
      plains: 14208942,
      hills: 13352856,
      forest: 11053176,
      desert: 14866616,
      canyon: 12095600,
      beach: 15657176,
      mountainLow: 11045488,
      mountainHigh: 12361864,
      cliffTop: 13681824,
      track: 13616292,
      lawn: 11843704,
      park: 10791532,
      cliffEarthTop: 12098168,
      cliffEarthBot: 8020552,
      cliffPaleTop: 15262416,
      cliffPaleBot: 11577488,
      bedMud: 14735552,
      bedSand: 15525588,
      waterShallow: 9425104,
      waterMid: 5941432,
      waterDeep: 3835560
    },
    sky: 15790836,
    atlas: 15789798,
    treeDensity: { forest: 0.01, hills: 2e-3, plains: 1e-3, beach: 0 },
    trees: { acacia: 1 },
    clutter: { shrub: 0.02 },
    cacti: true,
    herds: [],
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city", city: "suburb" },
    site: "lee",
    taste: { western: 4, adobe: 3, motherroad: 3, andean: 3 },
    surface: { rural: "dirt", suburb: "dirt" },
    names: [["Salt ", "White ", "Mirror ", "Borax ", "Alkali ", "Salar ", "Blanca ", "Dry Lake ", "Speed "], ["Flats", "Wells", "Siding", "Junction", "Springs", "City", "Station", "Bend"]],
    swaps: { field: null, vineyard: null, orchard: null, windTurbine: null, hotel: "saltHotel", motel: "saltHotel", factory: "saltWorks", iceRink: null, lakePark: null, lido: null, waterPark: null, scrapyard: "trainCemetery" },
    lamp: "none",
    country: [{ kind: "saltWorks", count: 2, w: 4, d: 3, where: "low" }, { kind: "saltHotel", count: 1, w: 2, d: 2, where: "any" }, { kind: "trainCemetery", count: 1, w: 4, d: 3, where: "any" }],
    wonder: { kind: "speedStrip", name: ["the Measured Mile", "the Speedway of ", "the International Speedway"], w: 6, d: 30, where: "flat" }
  },
  // ================================================================================== 19 · the Mangroves
  mangrove: {
    kind: "mangrove",
    title: "the Mangroves",
    label: "mangrove delta",
    temp: [0.7, 2],
    wet: WET,
    cells: ["full", "small", "continent"],
    coastal: true,
    islandSuffix: [" Delta", " Mangroves", " Creeks", " Sunderbans"],
    // a delta going out to sea through a maze of creeks: airboat and stilt-house country
    moisture: 0.95,
    arid: 0,
    temperature: 0.9,
    hills: 0.05,
    massif: 0,
    relief: 0.35,
    cap: 2,
    cliff: 0,
    shelf: 14,
    mangroves: true,
    snowLine: null,
    water: 0.46,
    falloff: 1.15,
    beaches: 0,
    farms: 2,
    quarries: 0,
    pistes: 0,
    lakes: 4,
    rivers: 2,
    forbid: ["dunes", "saltFlat", "glacier", "mesaCountry", "badlands", "fjordCoast", "volcano", "caldera", "skerries", "karstTowers"],
    palette: {
      plains: 8035408,
      hills: 8824920,
      forest: 4616252,
      beach: 11051128,
      cliffTop: 8034900,
      marsh: 6257216,
      track: 9075284,
      lawn: 8957014,
      park: 8036428,
      bedMud: 5919286,
      waterShallow: 6982234,
      waterMid: 4878424,
      waterDeep: 3103324
    },
    crops: [10142298, 9091146, 11061354],
    sky: 14739672,
    atlas: 5208638,
    treeDensity: { forest: 0.75, hills: 0.2, plains: 0.12, beach: 0.1 },
    trees: { canopy: 2, palm: 1.5, cypress: 1 },
    clutter: { reeds: 0.08, mooredBoat: 0.06 },
    herds: [],
    beachAllYear: true,
    towns: ["suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city", city: "suburb" },
    site: "levee",
    taste: { seasia: 6, colonial: 3, havana: 2 },
    surface: { rural: "dirt", suburb: "dirt", city: "brick" },
    names: [["Sundar", "Khulna", "Mong", "Kali", "Bhola", "Pat", "Bari", "Gosa", "Hiron"], ["ban", "pur", "ganj", "khali", "hat", "dwip", "gram"]],
    swaps: { store: "generalStore", house: "stiltHouse", field: "riceTerraces", windTurbine: null, iceRink: null, windmill: null, factory: "cannery", cemetery: null },
    lamp: "lantern",
    country: [
      { kind: "stiltVillage", count: 3, w: 3, d: 3, where: "water" },
      { kind: "shrimpFarm", count: 2, w: 3, d: 3, where: "low" },
      { kind: "airboatDock", count: 3, w: 2, d: 2, where: "water" },
      { kind: "stiltHouse", count: 6, w: 1, d: 1, where: "water" }
    ],
    wonder: { kind: "floatingMarket", name: ["the Floating Market", "the Market of a Thousand Boats", "the Water Bazaar"], w: 12, d: 12, where: "lake" }
  },
  // ================================================================================== 20 · the Lavender Plateau
  lavender: {
    kind: "lavender",
    title: "the Lavender Plateau",
    label: "provence",
    temp: [0.45, 0.88],
    wet: [-1, 0.66],
    cells: ["full", "small", "continent"],
    islandSuffix: [" Plateau", " Garrigue", " Lavande", " Hills"],
    // purple rows to the hills, stone farmhouses, plane trees on the square: the Valensole plateau
    moisture: 0.4,
    arid: 0.1,
    temperature: 0.7,
    hills: 0.7,
    hillAmp: 0.55,
    massif: 0.25,
    peakCap: 12,
    relief: 0.8,
    cliff: 0.5,
    snowLine: null,
    water: 0.45,
    falloff: 1.3,
    beaches: 1,
    farms: 7,
    quarries: 1,
    pistes: 0,
    lakes: 1,
    rivers: 0.8,
    forbid: ["dunes", "glacier", "mangroves", "karstTowers", "fjordCoast", "badlands", "saltFlat", "volcano", "caldera", "mesaCountry"],
    palette: {
      plains: 11975796,
      hills: 10924140,
      forest: 7244370,
      beach: 15260864,
      cliffTop: 12104828,
      track: 13943968,
      mountainLow: 12102288,
      mountainHigh: 13286820,
      lawn: 11057260,
      park: 10005090,
      cliffPaleTop: 14735040,
      cliffPaleBot: 11050116,
      cliffEarthTop: 13153424,
      cliffEarthBot: 9337436,
      waterShallow: 6275264,
      waterMid: 3117232,
      waterDeep: 2058912
    },
    crops: [9069248, 8017072, 10122444, 14202954],
    cropKinds: ["lavender", "lavender", "lavender", "sunflower"],
    sky: 14674680,
    atlas: 10119880,
    treeDensity: { forest: 0.3, hills: 0.05, plains: 0.025, beach: 0.01 },
    trees: { cypress: 2, olive: 2, stonePine: 1, poplar: 0.6, oak: 0.6 },
    clutter: { shrub: 0.03 },
    herds: ["goats", "hikers"],
    towns: ["city", "suburb", "rural", "rural"],
    tier: { metro: "city", mega: "city" },
    site: "cliffTop",
    taste: { mediterranean: 8, roman: 2, medieval: 1.5, moorish: 1 },
    surface: { rural: "cobble", suburb: "cobble" },
    names: [["Saint-", "Mont", "Val", "Roque", "Beau", "Ch\xE2teauneuf-", "Aigue", "Puy", "Gran"], ["R\xE9my", "ensole", "brune", "mont", "vert", "luz", "morte", "sault", "ville", "Didier"]],
    swaps: { field: "lavenderField", farmhouse: "bastide", barn: "perfumery", silo: "dovecote", windTurbine: "windmill", iceRink: null, brewery: "perfumery", pub: "taverna" },
    country: [
      { kind: "bastide", count: 4, w: 2, d: 2, where: "any" },
      { kind: "lavenderField", count: 4, w: 6, d: 5, where: "low" },
      { kind: "dovecote", count: 3, w: 1, d: 1, where: "any" },
      { kind: "perfumery", count: 1, w: 2, d: 2, where: "low" }
    ],
    wonder: { kind: "lavenderAbbey", name: ["the Abbey of the Lavender", "Sainte-Lavande Abbey", "the Abbey of "], w: 12, d: 12, where: "flat" }
  }
};

// src/world/macro.ts
var CONT_THR = 0.601;
var MODE_SUFFIX = { both: "", islands: "-isles", mainland: "-main" };
function parseSeed(seed) {
  for (const mode of ["islands", "mainland"]) {
    const suf = MODE_SUFFIX[mode];
    if (seed.length > suf.length && seed.endsWith(suf)) return { base: seed.slice(0, -suf.length), mode };
  }
  return { base: seed, mode: "both" };
}
var MODE = "both";
function setWorldMode(mode) {
  MODE = mode;
}
var MAIN_THR = 0.3;
var BAY = [0.5, -1.3];
var BAY_R = [0.7, 2.04];
var HOME_MID = [0.5, 0.6];
var MAIN_SHARE = [["sea", 0.55], ["islet", 0.3], ["small", 0.15]];
var KIND_SHARE = [["full", 0.55], ["small", 0.2], ["islet", 0.1], ["sea", 0.15]];
var MEGA_CHANCE = 0.1;
var MEGA_CHANCE_COUNTY = 0.24;
var STRAIT_CHANCE = 0.6;
var HOME_CONT = [4, 0];
var HOME_CONT_R = 3.5;
var HOME_ISLE = [0.5, 0.5];
var HOME_ISLE_R = 1.25;
function contField(seedHash2, cx, cz) {
  const f = (cellNoise(seedHash2, cx, cz, 7, 21) + 0.5 * cellNoise(seedHash2, cx, cz, 3, 22)) / 1.5;
  if (MODE === "islands") return f - 1;
  if (MODE === "mainland") {
    let g2 = f + (CONT_THR - MAIN_THR);
    const dh = Math.hypot(cx - HOME_MID[0], cz - HOME_MID[1]);
    if (dh < 0.8) {
      const e3 = dh <= 0.4 ? 0 : (dh - 0.4) / 0.4;
      g2 = Math.max(g2, (CONT_THR + 0.1) * (1 - e3 * e3 * (3 - 2 * e3)));
    }
    const db = Math.hypot((cx - BAY[0]) / BAY_R[0], (cz - BAY[1]) / BAY_R[1]);
    if (db >= 1) return g2;
    const e2 = db <= 0.75 ? 0 : (db - 0.75) / 0.25;
    return Math.min(g2, CONT_THR - 0.12 + 0.8 * e2 * e2 * (3 - 2 * e2));
  }
  let g = f;
  const d = Math.hypot(cx - HOME_CONT[0], cz - HOME_CONT[1]) / HOME_CONT_R;
  if (d < 1) {
    const t = 1 - d;
    g = Math.max(g, (CONT_THR + 0.16) * t * t * (3 - 2 * t));
  }
  const di = Math.hypot(cx - HOME_ISLE[0], cz - HOME_ISLE[1]) / HOME_ISLE_R;
  if (di >= 1) return g;
  const e = di <= 0.6 ? 0 : (di - 0.6) / 0.4;
  return Math.min(g, CONT_THR - 0.12 + 0.8 * e * e * (3 - 2 * e));
}
function continentality(seedHash2, cx, cz) {
  const t = (contField(seedHash2, cx, cz) - CONT_THR) / 0.11;
  return t <= 0 ? 0 : t >= 1 ? 1 : t * t * (3 - 2 * t);
}
function cellKind(seedHash2, cx, cz) {
  const raw = rawKind(seedHash2, cx, cz);
  return raw === "islet" && featureAt(seedHash2, cx, cz, "atoll") ? "atoll" : raw;
}
function rawKind(seedHash2, cx, cz) {
  if (MODE === "mainland") {
    if (cx === 0 && cz === 0 || contField(seedHash2, cx, cz) > CONT_THR) return "continent";
  } else {
    if (cx === 0 && cz === 0) return "full";
    if (MODE === "both" && contField(seedHash2, cx, cz) > CONT_THR) return "continent";
  }
  let r = cell01(seedHash2, cx, cz, 1);
  for (const [kind, share] of MODE === "mainland" ? MAIN_SHARE : KIND_SHARE) {
    if (r < share) return kind;
    r -= share;
  }
  return "sea";
}
function coastalContinent(seedHash2, cx, cz) {
  if (rawKind(seedHash2, cx, cz) !== "continent") return false;
  for (let dz = -1; dz <= 1; dz++) {
    for (let dx = -1; dx <= 1; dx++) {
      if (!dx && !dz) continue;
      if (rawKind(seedHash2, cx + dx, cz + dz) !== "continent") return true;
    }
  }
  return false;
}
function megaRoll(seedHash2, cx, cz, kind) {
  if (kind === "full") return cell01(seedHash2, cx, cz, 2) < MEGA_CHANCE;
  if (kind !== "continent" || !coastalContinent(seedHash2, cx, cz)) return false;
  return cell01(seedHash2, cx, cz, 2) < MEGA_CHANCE_COUNTY && landable(seedHash2, cx, cz);
}
function sharedEdge(cx, cz, d) {
  const axis = d % 2 === 0 ? 0 : 1;
  const ax = d === 2 ? cx - 1 : cx;
  const az = d === 3 ? cz - 1 : cz;
  return [ax, az, axis === 0 ? ax + 1 : ax, axis === 0 ? az : az + 1, axis];
}
function straitAt(seedHash2, cx, cz, d) {
  const [ax, az, bx, bz, axis] = sharedEdge(cx, cz, d);
  const ka = cellKind(seedHash2, ax, az), kb = cellKind(seedHash2, bx, bz);
  if (ka === "sea" || kb === "sea" || ka === "continent" && kb === "continent") return -1;
  if (ka === "atoll" || kb === "atoll") return -1;
  if (cell01(seedHash2, ax, az, 300 + axis) >= STRAIT_CHANCE) return -1;
  if (ka === "continent" && !landable(seedHash2, ax, az) || kb === "continent" && !landable(seedHash2, bx, bz)) return -1;
  return Math.round(N4 * 0.25 + cell01(seedHash2, ax, az, 400 + axis) * N4 * 0.5);
}
function landable(seedHash2, cx, cz) {
  for (let j = 0; j < 7; j++) {
    for (let i = 0; i < 7; i++) {
      if (contField(seedHash2, cx + 0.2 + i * 0.1, cz + 0.2 + j * 0.1) > CONT_THR + 0.03) return true;
    }
  }
  return false;
}
var DEALER_PLAN = 0.45;
function plannedDealer(seedHash2, cx, cz) {
  const k = cellKind(seedHash2, cx, cz);
  if (k !== "continent" && k !== "full") return false;
  return cell01(seedHash2, cx, cz, 2711) < DEALER_PLAN;
}
function landBorder(seedHash2, cx, cz, d) {
  if (cellKind(seedHash2, cx, cz) !== "continent") return false;
  const nx = cx + (d === 0 ? 1 : d === 2 ? -1 : 0);
  const nz = cz + (d === 1 ? 1 : d === 3 ? -1 : 0);
  return cellKind(seedHash2, nx, nz) === "continent";
}
var RARITY = { common: 1 / 2, uncommon: 1 / 4, rare: 1 / 12, veryRare: 1 / 40, legendary: 1 / 150 };
var PROMISE = { rare: 6, veryRare: 12, legendary: 20 };
var FEATURE_RULE = {
  volcano: { rarity: "rare", salt: 800 },
  caldera: { rarity: "veryRare", salt: 810 },
  atoll: { rarity: "rare", salt: 820 },
  saltFlat: { rarity: "rare", salt: 830 },
  karstTowers: { rarity: "rare", salt: 840 },
  mesaCountry: { rarity: "rare", salt: 850 }
};
function climateAt(seedHash2, u, v) {
  const temp = 1.25 - v * 0.055 + (cellNoise(seedHash2, u, v, 5.5, 7) - 0.5) * 0.7;
  const wet = cellNoise(seedHash2, u, v, 3.1, 43);
  return { temp: temp < 0 ? 0 : temp > 1 ? 1 : temp, wet };
}
function eligible(seedHash2, cx, cz, f) {
  const kind = rawKind(seedHash2, cx, cz);
  if (f === "atoll") return kind === "islet";
  const region = regionAt(seedHash2, cx, cz);
  if (region && REGIONS[region].forbid?.includes(f)) return false;
  const land = kind === "full" || kind === "small" || kind === "continent";
  const mega = hasMegaRaw(seedHash2, cx, cz, kind);
  const { temp, wet } = climateAt(seedHash2, cx + 0.5, cz + 0.5);
  switch (f) {
    case "volcano":
      return !mega && (kind === "full" || kind === "continent" && coastalContinent(seedHash2, cx, cz)) && !(cx === 0 && cz === 0);
    case "caldera":
      return !mega && kind === "full" && !(cx === 0 && cz === 0);
    case "saltFlat":
      return !mega && kind === "continent" && continentality(seedHash2, cx + 0.5, cz + 0.5) > 0.35;
    // one giant a cell: the towers and the mesas give way to anything rarer
    case "karstTowers":
      return !mega && land && temp > 0.55 && wet > 0.45 && !giant(seedHash2, cx, cz);
    case "mesaCountry":
      return !mega && land && temp > 0.4 && wet < 0.55 && !giant(seedHash2, cx, cz) && !featureAt(seedHash2, cx, cz, "karstTowers");
  }
}
function giant(seedHash2, cx, cz) {
  return featureAt(seedHash2, cx, cz, "volcano") || featureAt(seedHash2, cx, cz, "caldera") || featureAt(seedHash2, cx, cz, "saltFlat");
}
function hasMegaRaw(seedHash2, cx, cz, kind) {
  if (!megaRoll(seedHash2, cx, cz, kind)) return false;
  const r = regionAt(seedHash2, cx, cz);
  return !r || !!REGIONS[r].mega;
}
var promised = /* @__PURE__ */ new Map();
var promising = /* @__PURE__ */ new Set();
function guaranteedCell(seedHash2, f) {
  const key = `${seedHash2}:${f}`;
  if (promised.has(key)) return promised.get(key);
  const rule = FEATURE_RULE[f];
  const R = PROMISE[rule.rarity] ?? 0;
  if (promising.has(key)) throw new Error(`guaranteedCell: ${f} depends on itself`);
  promising.add(key);
  let best = null, bestRoll = Infinity;
  try {
    for (let cz = -R; cz <= R; cz++) {
      for (let cx = -R; cx <= R; cx++) {
        if (cx * cx + cz * cz > R * R || !eligible(seedHash2, cx, cz, f)) continue;
        const roll = cell01(seedHash2, cx, cz, rule.salt + 50);
        if (roll < bestRoll) {
          bestRoll = roll;
          best = [cx, cz];
        }
      }
    }
  } finally {
    promising.delete(key);
  }
  promised.set(key, best);
  if (promised.size > 64) promised.delete(promised.keys().next().value);
  return best;
}
var LONER = { saltFlat: 2, karstTowers: 2, mesaCountry: 2 };
var featureCache = /* @__PURE__ */ new Map();
function asked(seedHash2, cx, cz, f) {
  if (f === "atoll" || f === "caldera" || cx === 0 && cz === 0) return false;
  const region = regionAt(seedHash2, cx, cz);
  if (!region || !REGIONS[region].ask?.includes(f)) return false;
  const kind = rawKind(seedHash2, cx, cz);
  return kind === "full" || kind === "continent";
}
function featureRolled(seedHash2, cx, cz, f) {
  if (asked(seedHash2, cx, cz, f)) return true;
  if (!eligible(seedHash2, cx, cz, f)) return false;
  if (f === "volcano" && featureAt(seedHash2, cx, cz, "caldera")) return false;
  const rule = FEATURE_RULE[f];
  if (cell01(seedHash2, cx, cz, rule.salt) < RARITY[rule.rarity]) return true;
  const g = guaranteedCell(seedHash2, f);
  return !!g && g[0] === cx && g[1] === cz;
}
function featureAt(seedHash2, cx, cz, f) {
  const key = `${seedHash2}:${f}:${cx},${cz}`;
  const hit = featureCache.get(key);
  if (hit !== void 0) return hit;
  let out = featureRolled(seedHash2, cx, cz, f);
  const R = LONER[f];
  if (out && R && !asked(seedHash2, cx, cz, f)) {
    const g = guaranteedCell(seedHash2, f);
    const promisedHere = !!g && g[0] === cx && g[1] === cz;
    const roll = cell01(seedHash2, cx, cz, FEATURE_RULE[f].salt);
    for (let dz = -R; dz <= R && out; dz++) {
      for (let dx = -R; dx <= R; dx++) {
        if (!dx && !dz || !featureRolled(seedHash2, cx + dx, cz + dz, f)) continue;
        const theirs = !!g && g[0] === cx + dx && g[1] === cz + dz;
        const r2 = cell01(seedHash2, cx + dx, cz + dz, FEATURE_RULE[f].salt);
        if (theirs || !promisedHere && (r2 < roll || r2 === roll && (dz < 0 || dz === 0 && dx < 0))) {
          out = false;
          break;
        }
      }
    }
  }
  featureCache.set(key, out);
  if (featureCache.size > 5e4) featureCache.delete(featureCache.keys().next().value);
  return out;
}
var REGION_ROLL = 1 / 6;
var REGION_SPACING = 4;
var BLOB_R = 2;
var BLOB_MAX = 5;
var FIRST_PROMISE = 4;
var KIND_PROMISE = 20;
var REGION_SALT = 1100;
function regionEligible(seedHash2, cx, cz, k) {
  if (cx === 0 && cz === 0) return false;
  const spec = REGIONS[k];
  const kind = rawKind(seedHash2, cx, cz);
  if (!spec.cells.includes(kind)) return false;
  if (kind === "continent" && spec.coastal && !coastalContinent(seedHash2, cx, cz)) return false;
  const { temp, wet } = climateAt(seedHash2, cx + 0.5, cz + 0.5);
  if (spec.temp && (temp < spec.temp[0] || temp > spec.temp[1])) return false;
  if (spec.wet && (wet < spec.wet[0] || wet > spec.wet[1])) return false;
  return true;
}
var regionRoll = (seedHash2, cx, cz, ki) => cell01(seedHash2, cx, cz, REGION_SALT + ki);
var promiseCache = /* @__PURE__ */ new Map();
function promises(seedHash2) {
  let p = promiseCache.get(seedHash2);
  if (p) return p;
  p = { cells: /* @__PURE__ */ new Map() };
  promiseCache.set(seedHash2, p);
  if (promiseCache.size > 8) promiseCache.delete(promiseCache.keys().next().value);
  const taken = [];
  const clear = (cx, cz) => taken.every(([x, z]) => Math.max(Math.abs(x - cx), Math.abs(z - cz)) > REGION_SPACING);
  const out = p;
  const choose = (R, kinds, eff) => {
    let best = null, bestRoll = Infinity;
    for (let cz = -R; cz <= R; cz++) {
      for (let cx = -R; cx <= R; cx++) {
        if (cx * cx + cz * cz > R * R || !clear(cx, cz)) continue;
        for (const ki of kinds) {
          if (!regionEligible(seedHash2, cx, cz, REGION_KINDS[ki])) continue;
          const roll = regionRoll(seedHash2, cx, cz, ki);
          if (roll < bestRoll) {
            bestRoll = roll;
            best = [cx, cz, ki];
          }
        }
      }
    }
    if (!best) return;
    taken.push([best[0], best[1]]);
    out.cells.set(`${best[0]},${best[1]}`, { kind: REGION_KINDS[best[2]], eff });
  };
  const all = REGION_KINDS.map((_, i) => i);
  choose(FIRST_PROMISE, all, -2);
  const room = all.map((ki) => {
    let n = 0;
    for (let cz = -KIND_PROMISE; cz <= KIND_PROMISE; cz++) {
      for (let cx = -KIND_PROMISE; cx <= KIND_PROMISE; cx++) {
        if (cx * cx + cz * cz <= KIND_PROMISE * KIND_PROMISE && regionEligible(seedHash2, cx, cz, REGION_KINDS[ki])) n++;
      }
    }
    return n;
  });
  for (const ki of [...all].sort((a, b) => room[a] - room[b] || a - b)) {
    if ([...out.cells.values()].some((v) => v.kind === REGION_KINDS[ki])) continue;
    choose(KIND_PROMISE, [ki], -1 + ki * 1e-3);
  }
  return p;
}
function candidate(seedHash2, cx, cz) {
  const pr = promises(seedHash2).cells.get(`${cx},${cz}`);
  if (pr) return pr;
  let best = null;
  for (let ki = 0; ki < REGION_KINDS.length; ki++) {
    const roll = regionRoll(seedHash2, cx, cz, ki);
    if (roll >= REGION_ROLL || best && roll >= best.eff) continue;
    if (!regionEligible(seedHash2, cx, cz, REGION_KINDS[ki])) continue;
    best = { kind: REGION_KINDS[ki], eff: roll };
  }
  return best;
}
var anchorCache = /* @__PURE__ */ new Map();
function cached2(cache, key, make) {
  if (cache.has(key)) return cache.get(key);
  const v = make();
  cache.set(key, v);
  if (cache.size > 2e4) cache.delete(cache.keys().next().value);
  return v;
}
function regionAnchor(seedHash2, cx, cz) {
  return cached2(anchorCache, `${seedHash2}:${cx},${cz}`, () => {
    const me = candidate(seedHash2, cx, cz);
    if (!me) return null;
    for (let dz = -REGION_SPACING; dz <= REGION_SPACING; dz++) {
      for (let dx = -REGION_SPACING; dx <= REGION_SPACING; dx++) {
        if (!dx && !dz) continue;
        const o = candidate(seedHash2, cx + dx, cz + dz);
        if (!o || !(o.eff < me.eff || o.eff === me.eff && (dz < 0 || dz === 0 && dx < 0))) continue;
        if (regionAnchor(seedHash2, cx + dx, cz + dz)) return null;
      }
    }
    return me.kind;
  });
}
var blobCache = /* @__PURE__ */ new Map();
function blobOf(seedHash2, ax, az, kind) {
  return cached2(blobCache, `${seedHash2}:${ax},${az}`, () => {
    const ki = REGION_KINDS.indexOf(kind);
    const out = /* @__PURE__ */ new Set([`${ax},${az}`]);
    const score = (cx, cz) => cellNoise(seedHash2, cx, cz, 2, 900 + ki);
    for (let n = 0; n < BLOB_MAX; n++) {
      let best = null, bestS = 0.45;
      for (const key of out) {
        const [x, z] = key.split(",").map(Number);
        for (let d = 0; d < 4; d++) {
          const nx = x + (d === 0 ? 1 : d === 2 ? -1 : 0), nz = z + (d === 1 ? 1 : d === 3 ? -1 : 0);
          if (Math.max(Math.abs(nx - ax), Math.abs(nz - az)) > BLOB_R || out.has(`${nx},${nz}`)) continue;
          if (rawKind(seedHash2, nx, nz) !== "continent" || nx === 0 && nz === 0 || nearRival(seedHash2, nx, nz, ax, az)) continue;
          const s = score(nx, nz);
          if (s > bestS) {
            bestS = s;
            best = [nx, nz];
          }
        }
      }
      if (!best) break;
      out.add(`${best[0]},${best[1]}`);
    }
    return out;
  });
}
function nearRival(seedHash2, cx, cz, ax, az) {
  const R = 2 * BLOB_R;
  for (let dz = -R; dz <= R; dz++) {
    for (let dx = -R; dx <= R; dx++) {
      const x = cx + dx, z = cz + dz;
      if (x === ax && z === az) continue;
      if (regionAnchor(seedHash2, x, z)) return true;
    }
  }
  return false;
}
var regionCache = /* @__PURE__ */ new Map();
function regionAt(seedHash2, cx, cz) {
  return cached2(regionCache, `${seedHash2}:${cx},${cz}`, () => {
    const own = regionAnchor(seedHash2, cx, cz);
    if (own) return own;
    const kind = rawKind(seedHash2, cx, cz);
    if (kind === "continent") {
      for (let dz = -BLOB_R; dz <= BLOB_R; dz++) {
        for (let dx = -BLOB_R; dx <= BLOB_R; dx++) {
          const a = regionAnchor(seedHash2, cx + dx, cz + dz);
          if (!a || rawKind(seedHash2, cx + dx, cz + dz) !== "continent") continue;
          if (blobOf(seedHash2, cx + dx, cz + dz, a).has(`${cx},${cz}`)) return a;
        }
      }
    } else if (kind === "islet" && !(cx === 0 && cz === 0)) {
      for (let d = 0; d < 4; d++) {
        const nx = cx + (d === 0 ? 1 : d === 2 ? -1 : 0), nz = cz + (d === 1 ? 1 : d === 3 ? -1 : 0);
        const a = regionAnchor(seedHash2, nx, nz);
        const nk = rawKind(seedHash2, nx, nz);
        if (a && (nk === "full" || nk === "small") && cell01(seedHash2, cx, cz, 1250 + d) < 0.5) return a;
      }
    }
    return null;
  });
}

// src/world/landforms.ts
var TAU = Math.PI * 2;

// src/world/landformsLocal.ts
var SWAMP = REGION_KINDS.indexOf("mangrove") + 1;

// src/world/brooks.ts
var SPRING = {
  [5 /* Forest */]: 0.75,
  [4 /* Hills */]: 0.7,
  [3 /* Plains */]: 0.45,
  [9 /* Mountain */]: 0.55,
  [6 /* Farm */]: 0.35,
  [15 /* Marsh */]: 0.2
};

// src/world/biomes.ts
function hex(h) {
  return [(h >> 16 & 255) / 255, (h >> 8 & 255) / 255, (h & 255) / 255];
}
var C = {
  plains: hex(9425004),
  hills: hex(10672243),
  forest: hex(6531934),
  beach: hex(15721129),
  cliffTop: hex(11784066),
  desert: hex(15324574),
  canyon: hex(14330232),
  mountainLow: hex(9345670),
  mountainHigh: hex(11118241),
  snow: hex(15922680),
  track: hex(13281399),
  bedSand: hex(14470037),
  bedMud: hex(12167032),
  bedCanyon: hex(13279344),
  crops: [hex(14198594), hex(10930524), hex(15192940), hex(12159573)],
  waterShallow: hex(9426415),
  waterMid: hex(5679577),
  waterDeep: hex(3835586),
  cliffPaleTop: hex(14340545),
  cliffPaleBot: hex(10984845),
  cliffRedTop: hex(13733217),
  cliffRedBot: hex(9062962),
  cliffGreyTop: hex(10855068),
  cliffGreyBot: hex(7038819),
  cliffEarthTop: hex(11243371),
  cliffEarthBot: hex(7297861),
  lawn: hex(9426026),
  paved: hex(13881027),
  park: hex(7979866),
  pitch: hex(7127642),
  pitchStripe: hex(6337615),
  yard: hex(13481098),
  asphalt: hex(7172470),
  plaza: hex(14933195),
  deck: hex(11569760),
  deckTop: hex(9071176),
  deckBot: hex(6178352),
  quayTop: hex(12433582),
  quayBot: hex(9341570),
  runway: hex(5133144),
  apron: hex(8619916),
  gravel: hex(10721932),
  quarry: hex(10328724),
  quarryCut: hex(11775911),
  piste: hex(16054267),
  // ---- landforms ----
  lava: hex(3814964),
  lavaWarm: hex(5913136),
  blackSand: hex(6052182),
  salt: hex(15987178),
  saltCrack: hex(14473420),
  ice: hex(14478580),
  crevasse: hex(10468555),
  marsh: hex(8231514),
  marshPool: hex(6982242),
  mangrove: hex(6261322),
  lagoonBed: hex(15266e3),
  lagoonShallow: hex(8381400),
  lagoonDeep: hex(4179912),
  dune: hex(15519636),
  duneShade: hex(14465660),
  badBands: [hex(12607551), hex(14588506), hex(15654072), hex(10129286)],
  basalt: hex(4869200),
  cave: hex(5919820),
  cliffLavaTop: hex(4867138),
  cliffLavaBot: hex(2367518),
  cliffIceTop: hex(13624558),
  cliffIceBot: hex(9416898)
};

// src/world/greatParks.ts
var TAU2 = Math.PI * 2;

// src/world/factionKinds.ts
var FACTION_KINDS = [
  // the Constabulary
  "highwayPatrol",
  "impoundYard",
  // the Family
  "socialClub",
  "chopShop",
  "bossVilla",
  // the Ministry
  "embassy",
  "motorPool",
  "mint",
  "hangar18",
  // the Visitors
  "crashSite",
  "landingRing",
  "cropCircle",
  // the Brotherhood
  "unionHall",
  "buildersYard",
  "asphaltPlant",
  // the Post
  "sortingOffice",
  "mailDock",
  "mailboxRow",
  // the Army
  "airbase",
  "armyDepot",
  "checkpoint",
  // the Studios
  "backlot",
  "studioGate",
  "premiereTheatre",
  "starMansion",
  // the Guild
  "fishMarket",
  "netLoft",
  "lifeboatStation",
  "guildHall",
  // the Press
  "printworks",
  "broadcastCentre",
  "newsstand"
];
var OPEN2 = { solid: false };
var FACTION_INFO = {
  highwayPatrol: { label: "Highway patrol", icon: "\u{1F693}", size: [2, 2], h: 1.09, price: 999, grade: 2, tag: "work" },
  impoundYard: { label: "Impound yard", icon: "\u{1F6A7}", size: [3, 3], h: 0.83, price: 799, grade: 1, tag: "depot", spec: { w: 3, d: 3, zone: "edge", lot: 6 /* Asphalt */, apart: 20 } },
  socialClub: { label: "Social club", icon: "\u2615", size: [1, 2], h: 1.3, price: 899, grade: 2, tag: "shop", spec: { w: 1, d: 2, floors: 2, zone: "mid", lot: 2 /* Paved */, apart: 16 } },
  chopShop: { label: "Chop shop", icon: "\u{1F527}", size: [2, 3], h: 0.95, price: 699, grade: 1, tag: "depot", spec: { w: 2, d: 3, zone: "edge", lot: 6 /* Asphalt */, apart: 24 } },
  bossVilla: { label: "Villa", icon: "\u{1F3F0}", size: [4, 4], h: 1.6, price: 3999, grade: 5, tag: "home", spec: { w: 4, d: 4, zone: "edge", lot: 1 /* Lawn */, highest: true, apart: 40 } },
  embassy: { label: "Embassy", icon: "\u{1F3F3}\uFE0F", size: [3, 3], h: 1.57, price: 3499, grade: 5, tag: "work", spec: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 16 } },
  motorPool: { label: "Motor pool", icon: "\u{1F698}", size: [3, 2], h: 0.98, price: 999, grade: 2, tag: "depot", spec: { w: 3, d: 2, zone: "mid", lot: 6 /* Asphalt */, apart: 20 } },
  mint: { label: "The Mint", icon: "\u{1FA99}", size: [3, 3], h: 1.51, price: 4999, grade: 6, tag: "work", spec: { w: 3, d: 3, zone: "core", lot: 2 /* Paved */, apart: 40 } },
  hangar18: { label: "Hangar 18", icon: "\u{1F6EC}", size: [4, 3], h: 1.27, price: 2999, grade: 3, tag: "depot" },
  crashSite: { label: "Crash site", icon: "\u{1F6F8}", size: [3, 3], h: 0.65, price: 499, grade: 0, tag: "field", open: true },
  landingRing: { label: "Landing ring", icon: "\u2B55", size: [3, 3], h: 0.6, price: 399, grade: 0, tag: "field", open: true },
  cropCircle: { label: "Crop circle", icon: "\u{1F33E}", size: [3, 3], h: 0.04, price: 99, grade: 0, tag: "field", open: true },
  unionHall: { label: "Union hall", icon: "\u{1F477}", size: [2, 2], h: 1.31, price: 1199, grade: 3, tag: "work", spec: { w: 2, d: 2, floors: 2, zone: "mid", lot: 2 /* Paved */, apart: 18 } },
  buildersYard: { label: "Builders\u2019 yard", icon: "\u{1F9F1}", size: [3, 3], h: 0.9, price: 699, grade: 1, tag: "depot", spec: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 18 } },
  asphaltPlant: { label: "Asphalt plant", icon: "\u{1F6E3}\uFE0F", size: [3, 3], h: 1.8, price: 1499, grade: 2, tag: "depot" },
  sortingOffice: { label: "Sorting office", icon: "\u{1F4EF}", size: [4, 3], h: 1.42, price: 1999, grade: 3, tag: "depot", spec: { w: 4, d: 3, zone: "mid", lot: 6 /* Asphalt */, apart: 30 } },
  mailDock: { label: "Mail dock", icon: "\u{1F4EC}", size: [2, 1], h: 1.01, price: 499, grade: 1, tag: "dock" },
  mailboxRow: { label: "Mailboxes", icon: "\u{1F4EB}", size: [1, 1], h: 0.35, price: 49, grade: 0, tag: "home", open: true, spec: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, solid: false, apart: 12 } },
  airbase: { label: "Airbase", icon: "\u2708\uFE0F", size: [7, 5], h: 1.36, price: 5999, grade: 5, tag: "depot" },
  armyDepot: { label: "Army depot", icon: "\u{1FA96}", size: [4, 3], h: 1.02, price: 1999, grade: 3, tag: "depot" },
  checkpoint: { label: "Checkpoint", icon: "\u{1F6A7}", size: [1, 2], h: 0.88, price: 299, grade: 0, tag: "work" },
  backlot: { label: "Backlot", icon: "\u{1F3AC}", size: [5, 4], h: 1.63, price: 1999, grade: 2, tag: "leisure", spec: { w: 5, d: 4, zone: "edge", lot: 6 /* Asphalt */, apart: 30 } },
  studioGate: { label: "Studio gate", icon: "\u{1F39E}\uFE0F", size: [2, 1], h: 1.61, price: 799, grade: 1, tag: "work", spec: { w: 2, d: 1, zone: "edge", lot: 2 /* Paved */, apart: 30 } },
  premiereTheatre: { label: "Premiere theatre", icon: "\u{1F31F}", size: [3, 3], h: 1.8, price: 3499, grade: 5, tag: "leisure", spec: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 30 } },
  starMansion: { label: "Star\u2019s mansion", icon: "\u{1F576}\uFE0F", size: [3, 3], h: 1.39, price: 2999, grade: 4, tag: "home", spec: { w: 3, d: 3, zone: "edge", lot: 1 /* Lawn */, highest: true, apart: 14 } },
  fishMarket: { label: "Fish market", icon: "\u{1F41F}", size: [3, 2], h: 1.13, price: 999, grade: 2, tag: "shop", spec: { w: 3, d: 2, zone: "any", lot: 2 /* Paved */, waterside: true, apart: 20 } },
  netLoft: { label: "Net loft", icon: "\u{1F578}\uFE0F", size: [1, 2], h: 1.17, price: 299, grade: 1, tag: "dock", spec: { w: 1, d: 2, zone: "edge", lot: 12 /* Gravel */, waterside: "prefer", apart: 10 } },
  lifeboatStation: { label: "Lifeboat station", icon: "\u{1F6DF}", size: [2, 2], h: 1.3, price: 899, grade: 2, tag: "dock", spec: { w: 2, d: 2, zone: "any", lot: 2 /* Paved */, waterside: true, apart: 30 } },
  guildHall: { label: "Guild hall", icon: "\u2693", size: [2, 2], h: 1.29, price: 1199, grade: 3, tag: "work", spec: { w: 2, d: 2, floors: 2, zone: "mid", lot: 2 /* Paved */, waterside: "prefer", apart: 24 } },
  printworks: { label: "Printworks", icon: "\u{1F5DE}\uFE0F", size: [3, 3], h: 1.28, price: 1999, grade: 3, tag: "depot", spec: { w: 3, d: 3, zone: "edge", lot: 6 /* Asphalt */, apart: 30 } },
  broadcastCentre: { label: "Broadcast centre", icon: "\u{1F4FA}", size: [3, 3], h: 2.22, price: 3999, grade: 5, tag: "work", spec: { w: 3, d: 3, floors: 5, zone: "core", lot: 7 /* Plaza */, apart: 30 } },
  newsstand: { label: "Newsstand", icon: "\u{1F4F0}", size: [1, 1], h: 0.47, price: 99, grade: 0, tag: "shop", open: true, spec: { w: 1, d: 1, zone: "core", lot: 7 /* Plaza */, solid: false, apart: 8 } }
};
var pick = (f) => Object.fromEntries(FACTION_KINDS.map((k) => [k, f(FACTION_INFO[k])]).filter(([, v]) => v !== void 0));
var FACTION_SPEC = pick((i) => i.spec ? { ...i.spec, ...i.open ? OPEN2 : {} } : i.open ? { w: i.size[0], d: i.size[1], ...OPEN2 } : void 0);
var FACTION_HEIGHT = pick((i) => i.h);
var FACTION_PRICE = pick((i) => i.price);
var FACTION_ICON = pick((i) => i.icon);
var FACTION_LABEL = pick((i) => i.label);
var FACTION_GRADE = pick((i) => i.grade);
var FACTION_TAG = pick((i) => i.tag);
var FACTION_SIZE = pick((i) => i.size);

// src/world/wildKinds.ts
var WILD_KINDS = [
  // A: the strange and the lonely
  "monolith",
  "phoneBox",
  "lonelyVending",
  "doorToNowhere",
  "fieldPiano",
  "giantChair",
  "shoeTree",
  "signForest",
  "bigfootStatue",
  "giantDuck",
  // B: what was left behind
  "abandonedBus",
  "carhenge",
  "planeBoneyard",
  "derelictFair",
  "hauntedHouse",
  "bottleHouse",
  "meteorCrater",
  "whaleBones",
  "stoneHeads",
  "giantSundial",
  // C: somebody is out here
  "treeHouse",
  "balloonField",
  "radioArray",
  "giantScarecrow",
  "campsite",
  "apiary",
  "prospectorCamp",
  "hedgeMaze",
  "giantSnowman",
  "giantChess"
];
var WILD_INFO = {
  monolith: { label: "Monolith", icon: "\u2B1B", size: [1, 1], h: 1.25, price: 99, grade: 0, where: "edge", chance: 0.1, lot: 0 /* None */ },
  phoneBox: { label: "Phone box", icon: "\u260E\uFE0F", size: [1, 1], h: 0.5, price: 49, grade: 0, where: "edge", chance: 0.22, lot: 0 /* None */ },
  lonelyVending: { label: "Vending machine", icon: "\u{1F964}", size: [1, 1], h: 0.42, price: 49, grade: 0, where: "edge", chance: 0.2, lot: 12 /* Gravel */ },
  doorToNowhere: { label: "Door to nowhere", icon: "\u{1F6AA}", size: [1, 1], h: 0.7, price: 49, grade: 0, where: "high", chance: 0.12, lot: 0 /* None */ },
  fieldPiano: { label: "Piano in a field", icon: "\u{1F3B9}", size: [1, 1], h: 0.32, price: 199, grade: 0, where: "edge", chance: 0.1, open: true, lot: 0 /* None */ },
  giantChair: { label: "Giant chair", icon: "\u{1FA91}", size: [1, 1], h: 1.05, price: 99, grade: 0, where: "edge", chance: 0.15, lot: 0 /* None */ },
  shoeTree: { label: "Shoe tree", icon: "\u{1F45F}", size: [1, 1], h: 1.2, price: 49, grade: 0, where: "edge", chance: 0.18, lot: 0 /* None */ },
  signForest: { label: "Sign forest", icon: "\u{1FAA7}", size: [2, 2], h: 0.75, price: 99, grade: 0, where: "edge", chance: 0.12, open: true, lot: 12 /* Gravel */, lane: true },
  bigfootStatue: { label: "Bigfoot statue", icon: "\u{1F9B6}", size: [1, 1], h: 1, price: 149, grade: 0, where: "forest", chance: 0.15, lot: 0 /* None */ },
  giantDuck: { label: "Giant duck", icon: "\u{1F986}", size: [2, 2], h: 0.8, price: 149, grade: 0, where: "water", chance: 0.1, lot: 13 /* Sand */ },
  abandonedBus: { label: "Abandoned bus", icon: "\u{1F68C}", size: [2, 1], h: 0.45, price: 99, grade: 0, where: "edge", chance: 0.22, lot: 0 /* None */ },
  carhenge: { label: "Carhenge", icon: "\u{1F697}", size: [3, 3], h: 0.62, price: 299, grade: 0, where: "edge", chance: 0.08, lot: 0 /* None */, lane: true },
  planeBoneyard: { label: "Plane boneyard", icon: "\u{1F6E9}\uFE0F", size: [6, 5], h: 0.7, price: 999, grade: 1, where: "edge", chance: 0.07, climate: "warm", lot: 13 /* Sand */, lane: true },
  derelictFair: { label: "Derelict fair", icon: "\u{1F3A1}", size: [4, 4], h: 1.7, price: 499, grade: 0, where: "edge", chance: 0.09, lot: 12 /* Gravel */, lane: true },
  hauntedHouse: { label: "Haunted house", icon: "\u{1F3DA}\uFE0F", size: [2, 2], h: 1.35, price: 399, grade: 1, where: "edge", chance: 0.12, lot: 5 /* Yard */, lane: true },
  bottleHouse: { label: "Bottle house", icon: "\u{1F37E}", size: [2, 2], h: 0.62, price: 299, grade: 1, where: "edge", chance: 0.1, lot: 5 /* Yard */, lane: true },
  meteorCrater: { label: "Meteor crater", icon: "\u2604\uFE0F", size: [4, 4], h: 0.35, price: 199, grade: 0, where: "edge", chance: 0.07, open: true, lot: 0 /* None */ },
  whaleBones: { label: "Whale bones", icon: "\u{1F40B}", size: [3, 2], h: 0.55, price: 99, grade: 0, where: "shore", chance: 0.12, open: true, lot: 13 /* Sand */ },
  stoneHeads: { label: "Stone heads", icon: "\u{1F5FF}", size: [3, 2], h: 1.1, price: 299, grade: 0, where: "high", chance: 0.1, lot: 1 /* Lawn */ },
  giantSundial: { label: "Giant sundial", icon: "\u{1F570}\uFE0F", size: [3, 3], h: 0.95, price: 299, grade: 0, where: "high", chance: 0.1, open: true, lot: 2 /* Paved */ },
  treeHouse: { label: "Tree house", icon: "\u{1F333}", size: [2, 2], h: 1.45, price: 299, grade: 1, where: "forest", chance: 0.25, lot: 0 /* None */ },
  balloonField: { label: "Balloon field", icon: "\u{1F388}", size: [3, 3], h: 1.9, price: 499, grade: 0, where: "field", chance: 0.15, open: true, lot: 1 /* Lawn */, lane: true },
  radioArray: { label: "Radio telescopes", icon: "\u{1F4E1}", size: [5, 4], h: 1.4, price: 1999, grade: 2, where: "edge", chance: 0.08, lot: 12 /* Gravel */, lane: true },
  giantScarecrow: { label: "Giant scarecrow", icon: "\u{1F33E}", size: [1, 1], h: 1.3, price: 49, grade: 0, where: "field", chance: 0.2, lot: 0 /* None */ },
  campsite: { label: "Campsite", icon: "\u26FA", size: [3, 2], h: 0.4, price: 199, grade: 0, where: "forest", chance: 0.3, open: true, lot: 1 /* Lawn */, lane: true },
  apiary: { label: "Apiary", icon: "\u{1F41D}", size: [2, 2], h: 0.3, price: 149, grade: 0, where: "field", chance: 0.2, open: true, lot: 1 /* Lawn */ },
  prospectorCamp: { label: "Prospector\u2019s camp", icon: "\u{1FA99}", size: [3, 2], h: 0.55, price: 199, grade: 0, where: "water", chance: 0.15, lot: 12 /* Gravel */ },
  hedgeMaze: { label: "Hedge maze", icon: "\u{1F33F}", size: [4, 4], h: 0.3, price: 399, grade: 0, where: "edge", chance: 0.1, open: true, lot: 1 /* Lawn */, lane: true },
  giantSnowman: { label: "Giant snowman", icon: "\u2603\uFE0F", size: [1, 1], h: 1, price: 49, grade: 0, where: "edge", chance: 0.8, climate: "cold", lot: 0 /* None */ },
  giantChess: { label: "Giant chess", icon: "\u265F\uFE0F", size: [2, 2], h: 0.3, price: 199, grade: 0, where: "edge", chance: 0.1, open: true, lot: 2 /* Paved */ }
};
var pick2 = (f) => Object.fromEntries(WILD_KINDS.map((k) => [k, f(WILD_INFO[k])]));
var WILD_SPEC = pick2((i) => ({
  w: i.size[0],
  d: i.size[1],
  zone: "edge",
  lot: i.lot ?? 0 /* None */,
  apart: 40,
  ...i.open ? { solid: false } : {}
}));
var WILD_HEIGHT = pick2((i) => i.h);
var WILD_PRICE = pick2((i) => i.price);
var WILD_ICON = pick2((i) => i.icon);
var WILD_LABEL = pick2((i) => i.label);
var WILD_GRADE = pick2((i) => i.grade);
var WILD_SIZE = pick2((i) => i.size);

// src/world/land.ts
var LAND = {
  /** Dollars a lift, in ordinary ground. */
  lift: 2,
  /** Multipliers by what the ground is made of. */
  soft: 1,
  ice: 2,
  rock: 3,
  /** A tile under water before or after the step: dredging and reclaiming. The larger of this and the ground's applies. */
  wet: 2.5,
  /** Dollars a tile a step of water level, for the water tool. */
  water: 3,
  /** Highest and lowest a corner may go, in steps. The floor is the sea bed every cell's rim is forced to. */
  ceiling: 26,
  floor: SEA - 5,
  /** Most tiles one smooth step may ripple through before it is refused rather than bankrupting anyone. */
  ripple: 600,
  /** Strokes Z can take back. */
  undo: 32
};

// src/world/diagonals.ts
var ROAD_HALF = {
  [1 /* Dirt */]: 0.3,
  [2 /* Brick */]: 0.32,
  [3 /* Paved */]: 0.31,
  [4 /* TwoLane */]: 0.48,
  [6 /* Gravel */]: 0.29,
  [7 /* Cobble */]: 0.33
};

// src/world/roads.ts
var ROAD_INFO = [
  { name: "No road", width: 0, speed: 1, span: 0, lift: 0 },
  { name: "Dirt lane", width: 0.6, speed: 1.1, span: 6, lift: 0.02 },
  { name: "Brick lane", width: 0.64, speed: 1.18, span: 7, lift: 0.025 },
  { name: "Paved lane", width: 0.62, speed: 1.25, span: 8, lift: 0.03 },
  { name: "Two-lane road", width: 0.96, speed: 1.25, span: 12, lift: 0.04 },
  { name: "Four-lane highway", width: 1.94, speed: 1.25, span: 14, lift: 0.05 },
  { name: "Gravel track", width: 0.58, speed: 1.13, span: 6, lift: 0.02 },
  { name: "Cobbled street", width: 0.66, speed: 1.16, span: 7, lift: 0.028 },
  // The speeds are all against bare ground (1): a paved road is +25%, and the interstate +50% — a fifth
  // quicker again than any other paved road. Its width is the whole three tiles, median and all.
  { name: "Interstate", width: 3, speed: 1.5, span: 16, lift: 0.055 },
  { name: "On/off ramp", width: 0.5, speed: 1.25, span: 8, lift: 0.03 }
];
var PAVED_SPEED = 1.25;
var ROAD_RANK = {
  [0 /* None */]: 0,
  [1 /* Dirt */]: 1,
  [6 /* Gravel */]: 2,
  [2 /* Brick */]: 3,
  [7 /* Cobble */]: 4,
  [3 /* Paved */]: 5,
  [4 /* TwoLane */]: 6,
  [5 /* FourLane */]: 7,
  // a ramp gives way to whatever it joins: the road it meets owns the junction tile and draws over it
  [9 /* Ramp */]: 5.5,
  [8 /* Interstate */]: 8
};
var INTERSTATE_STEP = PAVED_SPEED / ROAD_INFO[8 /* Interstate */].speed;

// src/world/townKinds.ts
var OPEN3 = { solid: false, apart: 8 };
var KIND_SPEC = {
  // the factions' town buildings (world/factionKinds.ts)
  ...FACTION_SPEC,
  ...WILD_SPEC,
  // ---- parks and plazas ----
  meadow: { w: [5, 8], d: [5, 7], zone: "edge", lot: 1 /* Lawn */, ...OPEN3, frontage: false },
  civicSquare: { w: [5, 6], d: [4, 5], zone: "core", lot: 7 /* Plaza */, ...OPEN3 },
  fountainSquare: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, ...OPEN3 },
  greenway: { w: [6, 10], d: 2, zone: "any", lot: 3 /* Park */, ...OPEN3, waterside: true, frontage: false },
  picnicGround: { w: 3, d: 3, zone: "edge", lot: 1 /* Lawn */, ...OPEN3 },
  dogRun: { w: 2, d: 3, zone: "mid", lot: 12 /* Gravel */, ...OPEN3 },
  plazaSteps: { w: [3, 4], d: 3, zone: "core", lot: 7 /* Plaza */, ...OPEN3, allow: 3 },
  sculpturePark: { w: 4, d: 4, zone: "mid", lot: 3 /* Park */, ...OPEN3 },
  bandstandGreen: { w: 4, d: 4, zone: "mid", lot: 3 /* Park */, ...OPEN3 },
  memorial: { w: [2, 3], d: 2, zone: "core", lot: 12 /* Gravel */, ...OPEN3 },
  skatePark: { w: 2, d: 3, zone: "mid", lot: 6 /* Asphalt */, ...OPEN3 },
  promenade: { w: [6, 10], d: 1, zone: "any", lot: 7 /* Plaza */, ...OPEN3, along: "wide" },
  cemetery: { w: [4, 6], d: [4, 5], zone: "edge", lot: 1 /* Lawn */, solid: true, apart: 10 },
  amphitheatre: { w: 4, d: 4, zone: "edge", lot: 1 /* Lawn */, ...OPEN3, allow: 3 },
  marketSquare: { w: [4, 5], d: 4, zone: "core", lot: 7 /* Plaza */, ...OPEN3 },
  lakePark: { w: [7, 9], d: [6, 8], zone: "mid", lot: 3 /* Park */, solid: true, apart: 12, waterside: "prefer", frontage: false },
  courts: { w: [3, 4], d: 3, zone: "mid", lot: 4 /* Pitch */, ...OPEN3 },
  formalGarden: { w: [4, 5], d: [4, 5], zone: "core", lot: 12 /* Gravel */, ...OPEN3 },
  allotments: { w: [3, 4], d: 3, zone: "edge", lot: 5 /* Yard */, ...OPEN3 },
  botanicalGarden: { w: [5, 6], d: [5, 6], zone: "mid", lot: 3 /* Park */, solid: true, apart: 12 },
  playground: { w: [2, 3], d: [2, 3], zone: "mid", lot: 13 /* Sand */, ...OPEN3 },
  grove: { w: [3, 4], d: [3, 4], zone: "edge", lot: 3 /* Park */, ...OPEN3 },
  // a Central Park: never put down by a roster or by growth, only by a big city's layout round its own ring of
  // streets (towns.ts greatParkSite); listed for its size, and so the Passport has a page for it
  greatPark: { w: [12, 18], d: [12, 18], zone: "mid", lot: 3 /* Park */, solid: true, apart: 200, frontage: false },
  // ---- civic ----
  courthouse: { w: 3, d: 3, floors: 3, zone: "core", lot: 7 /* Plaza */ },
  postOffice: { w: 2, d: 1, zone: "core", lot: 2 /* Paved */ },
  villageHall: { w: 1, d: 2, zone: "core", lot: 1 /* Lawn */ },
  communityCentre: { w: 2, d: 2, zone: "mid", lot: 2 /* Paved */ },
  kindergarten: { w: 2, d: 2, zone: "mid", lot: 1 /* Lawn */ },
  university: { w: 6, d: 5, floors: [3, 4], zone: "mid", lot: 1 /* Lawn */, gap: 1 },
  busStation: { w: 3, d: 2, zone: "edge", lot: 6 /* Asphalt */, roadside: true },
  prison: { w: 5, d: 4, zone: "edge", lot: 12 /* Gravel */, gap: 1 },
  cathedral: { w: 3, d: 5, zone: "core", lot: 7 /* Plaza */ },
  chapel: { w: 1, d: 1, zone: "edge", lot: 1 /* Lawn */ },
  mosque: { w: 2, d: 2, zone: "mid", lot: 7 /* Plaza */ },
  temple: { w: 2, d: 2, zone: "core", lot: 7 /* Plaza */ },
  substation: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */ },
  powerPlant: { w: 5, d: 4, zone: "edge", lot: 12 /* Gravel */, gap: 1 },
  recyclingCentre: { w: 2, d: 2, zone: "edge", lot: 6 /* Asphalt */ },
  ministry: { w: 3, d: 3, floors: [8, 12], zone: "core", lot: 7 /* Plaza */ },
  waterworks: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, waterside: "prefer" },
  observatory: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true },
  // ---- business ----
  hotel: { w: 2, d: 2, floors: [6, 14], zone: "core", lot: 7 /* Plaza */, apart: 6 },
  motel: { w: 3, d: 1, zone: "edge", lot: 6 /* Asphalt */, roadside: true },
  bank: { w: 2, d: 1, floors: 2, zone: "core", lot: 2 /* Paved */, apart: 5 },
  carDealership: { w: 2, d: 2, zone: "edge", lot: 6 /* Asphalt */, roadside: true },
  bigBox: { w: 4, d: 3, zone: "edge", lot: 6 /* Asphalt */ },
  stripMall: { w: 3, d: 1, zone: "mid", lot: 6 /* Asphalt */ },
  cafe: { w: 1, d: 1, floors: 2, zone: "core", lot: 7 /* Plaza */, apart: 3 },
  bakery: { w: 1, d: 1, floors: 2, zone: "core", lot: 2 /* Paved */, apart: 5 },
  officePark: { w: 3, d: 2, zone: "edge", lot: 6 /* Asphalt */ },
  techCampus: { w: 4, d: 4, zone: "edge", lot: 1 /* Lawn */ },
  factory: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */ },
  brewery: { w: 2, d: 2, zone: "mid", lot: 2 /* Paved */ },
  logisticsDepot: { w: 4, d: 3, zone: "edge", lot: 6 /* Asphalt */, roadside: true },
  pub: { w: 1, d: 1, floors: 2, zone: "mid", lot: 2 /* Paved */, apart: 5 },
  fastFood: { w: 1, d: 1, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 6 },
  garage: { w: 1, d: 1, zone: "edge", lot: 6 /* Asphalt */ },
  marketHall: { w: 3, d: 2, zone: "core", lot: 7 /* Plaza */ },
  departmentStore: { w: 2, d: 2, floors: [4, 6], zone: "core", lot: 7 /* Plaza */ },
  exchange: { w: 2, d: 2, floors: 5, zone: "core", lot: 7 /* Plaza */ },
  dataCentre: { w: 3, d: 2, zone: "edge", lot: 12 /* Gravel */ },
  // ---- entertainment ----
  cinema: { w: 2, d: 2, floors: 3, zone: "core", lot: 2 /* Paved */, apart: 8 },
  theatre: { w: 2, d: 2, floors: 4, zone: "core", lot: 7 /* Plaza */, apart: 8 },
  arena: { w: 4, d: 4, zone: "mid", lot: 2 /* Paved */ },
  concertHall: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, waterside: "prefer" },
  operaHouse: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 30 },
  bowlingAlley: { w: 2, d: 1, zone: "edge", lot: 6 /* Asphalt */ },
  casino: { w: 3, d: 2, floors: 3, zone: "core", lot: 7 /* Plaza */ },
  zoo: { w: 5, d: 5, zone: "edge", lot: 1 /* Lawn */, solid: false, apart: 12 },
  aquarium: { w: 3, d: 2, zone: "edge", lot: 7 /* Plaza */, waterside: true },
  lido: { w: 3, d: 2, zone: "mid", lot: 2 /* Paved */, solid: false, apart: 8 },
  waterPark: { w: 4, d: 3, zone: "edge", lot: 6 /* Asphalt */, solid: false, apart: 12 },
  miniGolf: { w: 2, d: 2, zone: "edge", lot: 1 /* Lawn */, solid: false, roadside: true },
  fairground: { w: 4, d: 4, zone: "edge", lot: 12 /* Gravel */, solid: false, apart: 12 },
  racetrack: { w: 8, d: 5, zone: "edge", lot: 12 /* Gravel */, solid: false, frontage: false, allow: 2 },
  iceRink: { w: 3, d: 2, zone: "mid", lot: 2 /* Paved */ },
  nightclub: { w: 1, d: 1, floors: 2, zone: "core", lot: 2 /* Paved */, apart: 6 },
  sportsHall: { w: 2, d: 2, zone: "mid", lot: 2 /* Paved */ },
  // ---- homes ----
  bungalow: { w: 1, d: 1, zone: "mid", lot: 1 /* Lawn */, gap: 1 },
  cottage: { w: 1, d: 1, zone: "any", lot: 5 /* Yard */, gap: 1 },
  duplex: { w: 1, d: 1, zone: "mid", lot: 1 /* Lawn */ },
  terraceRow: { w: 3, d: 1, floors: 2, zone: "mid", lot: 2 /* Paved */ },
  mansion: { w: 2, d: 2, zone: "edge", lot: 1 /* Lawn */, gap: 1, apart: 8 },
  retirementHome: { w: 3, d: 2, zone: "mid", lot: 1 /* Lawn */ },
  trailerPark: { w: 3, d: 2, zone: "edge", lot: 12 /* Gravel */ },
  courtyardBlock: { w: 3, d: 3, floors: [4, 6], zone: "mid", lot: 2 /* Paved */ },
  towerBlock: { w: 2, d: 2, floors: [18, 30], zone: "mid", lot: 1 /* Lawn */ },
  condoTower: { w: 2, d: 2, floors: [12, 24], zone: "any", lot: 7 /* Plaza */, waterside: true },
  // ---- landmarks and infrastructure ----
  commsTower: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true },
  observationTower: { w: 1, d: 1, zone: "core", lot: 7 /* Plaza */, waterside: "prefer" },
  clockTower: { w: 1, d: 1, zone: "core", lot: 7 /* Plaza */ },
  parkingGarage: { w: 2, d: 2, floors: [4, 6], zone: "core", lot: 6 /* Asphalt */ },
  windTurbine: { w: 1, d: 1, zone: "edge", lot: 1 /* Lawn */, frontage: false, highest: true, apart: 2 },
  fuelDepot: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */ },
  // ---- country and roadside ----
  windmill: { w: 1, d: 1, zone: "edge", lot: 5 /* Yard */, frontage: false, highest: true },
  sawmill: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */ },
  greenhouse: { w: 2, d: 1, zone: "edge", lot: 5 /* Yard */ },
  orchard: { w: [3, 4], d: 3, zone: "edge", lot: 1 /* Lawn */, solid: false, frontage: false },
  vineyard: { w: [3, 4], d: 3, zone: "edge", lot: 1 /* Lawn */, solid: false, frontage: false, allow: 2, highest: true },
  farmShop: { w: 1, d: 1, zone: "edge", lot: 5 /* Yard */, roadside: true },
  stables: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */ },
  // ---- the biomes' own ----
  // the Sands
  souk: { w: [4, 5], d: 2, zone: "core", lot: 7 /* Plaza */, ...OPEN3, apart: 10 },
  caravanserai: { w: 3, d: 3, zone: "edge", lot: 13 /* Sand */, roadside: true, apart: 14 },
  windTower: { w: 1, d: 1, zone: "mid", lot: 13 /* Sand */, frontage: false },
  kasbah: { w: 3, d: 3, zone: "core", lot: 13 /* Sand */ },
  tentCamp: { w: 3, d: 3, zone: "edge", lot: 13 /* Sand */, solid: false },
  dateGrove: { w: [5, 7], d: [4, 6], zone: "edge", lot: 13 /* Sand */, solid: false, frontage: false },
  // the Icefield
  researchStation: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */ },
  sauna: { w: 1, d: 1, zone: "any", lot: 12 /* Gravel */, waterside: "prefer", apart: 6 },
  sledKennels: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */ },
  fishRacks: { w: 2, d: 1, zone: "edge", lot: 12 /* Gravel */, solid: false, waterside: "prefer" },
  // the Shallows
  beachClub: { w: 3, d: 2, zone: "any", lot: 13 /* Sand */, waterside: "prefer", apart: 10 },
  marina: { w: 4, d: 3, zone: "any", lot: 8 /* Deck */, solid: false, waterside: true, apart: 16 },
  golfCourse: { w: [8, 9], d: [5, 6], zone: "edge", lot: 1 /* Lawn */, solid: false, frontage: false, apart: 20 },
  boardwalk: { w: [6, 9], d: 1, zone: "any", lot: 8 /* Deck */, solid: false, waterside: "prefer" },
  resortVillas: { w: 3, d: 3, zone: "mid", lot: 1 /* Lawn */ },
  tikiBar: { w: 1, d: 1, zone: "any", lot: 13 /* Sand */, waterside: "prefer", apart: 5 },
  // the Green
  stiltHouse: { w: 1, d: 1, zone: "edge", lot: 0 /* None */, waterside: "prefer", gap: 1 },
  canopyWalkway: { w: 4, d: 4, zone: "edge", lot: 3 /* Park */, solid: false },
  ropeBridge: { w: 1, d: 4, zone: "any", lot: 0 /* None */, solid: false, frontage: false },
  plantation: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */ },
  tradingPost: { w: 2, d: 1, zone: "core", lot: 5 /* Yard */ },
  // the Heights
  mountainHut: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, highest: true },
  funicular: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */ },
  dairy: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */ },
  monastery: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 30 },
  // the Moors
  distillery: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */, waterside: "prefer" },
  croft: { w: 1, d: 1, zone: "edge", lot: 5 /* Yard */, gap: 1 },
  bothy: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */ },
  cairnField: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, solid: false, highest: true },
  // the Veldt
  safariLodge: { w: 3, d: 3, zone: "edge", lot: 5 /* Yard */, highest: true },
  reserveGate: { w: 3, d: 1, zone: "edge", lot: 12 /* Gravel */, solid: false },
  rangerPost: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */ },
  pumpWindmill: { w: 1, d: 1, zone: "edge", lot: 5 /* Yard */, frontage: false },
  rondavel: { w: 1, d: 1, zone: "any", lot: 5 /* Yard */, gap: 1 },
  kraal: { w: [5, 6], d: [4, 5], zone: "edge", lot: 5 /* Yard */, solid: false, frontage: false },
  // the Fens
  shrimpShack: { w: 1, d: 1, zone: "edge", lot: 8 /* Deck */, waterside: "prefer" },
  airboatDock: { w: 2, d: 2, zone: "edge", lot: 8 /* Deck */, solid: false, waterside: true },
  generalStore: { w: 2, d: 1, zone: "core", lot: 5 /* Yard */ },
  // the Terraces
  pagoda: { w: 2, d: 2, floors: 5, zone: "mid", lot: 12 /* Gravel */, apart: 12 },
  shrine: { w: 1, d: 1, zone: "any", lot: 12 /* Gravel */, apart: 6 },
  teaHouse: { w: 2, d: 2, zone: "any", lot: 12 /* Gravel */, waterside: "prefer", apart: 8 },
  bathhouse: { w: 2, d: 2, zone: "mid", lot: 7 /* Plaza */ },
  riceTerraces: { w: [5, 7], d: [4, 6], zone: "edge", lot: 15 /* Paddy */, solid: false, frontage: false },
  // the Whitewash
  blueChapel: { w: 1, d: 1, zone: "any", lot: 7 /* Plaza */, apart: 5 },
  // ---- a medieval town's own (swapped in by the town's style: see STYLE_SWAPS in towns.ts)
  watchtower: { w: 1, d: 1, floors: 4, zone: "edge", lot: 12 /* Gravel */, apart: 8 },
  guildhall: { w: 2, d: 2, floors: 2, zone: "core", lot: 7 /* Plaza */, apart: 6 },
  titheBarn: { w: 3, d: 2, zone: "edge", lot: 5 /* Yard */, apart: 6 },
  // ---- the themed towns' own (swapped in by the town's style: see STYLE_SWAPS in towns.ts)
  canalWarehouse: { w: 1, d: 2, floors: [4, 5], zone: "core", lot: 2 /* Paved */, waterside: "prefer", apart: 3 },
  fireLookout: { w: 1, d: 1, zone: "core", lot: 12 /* Gravel */, apart: 8 },
  parliament: { w: 4, d: 3, floors: 3, zone: "core", lot: 7 /* Plaza */, waterside: "prefer", apart: 30 },
  thermalBath: { w: 3, d: 3, zone: "mid", lot: 7 /* Plaza */, apart: 12 },
  saloon: { w: 1, d: 1, floors: 2, zone: "core", lot: 2 /* Paved */, apart: 3 },
  meetingHouse: { w: 1, d: 2, zone: "core", lot: 1 /* Lawn */, apart: 6 },
  statehouse: { w: 3, d: 2, floors: 2, zone: "core", lot: 1 /* Lawn */, apart: 20 },
  // the kremlin takes a square of its own; the tower is a landmark; the mosque and the pyramid want the core,
  // the bazaar a street front and the ball court the edge of the plaza
  kremlin: { w: [5, 6], d: [4, 5], zone: "core", lot: 7 /* Plaza */, gap: 1, apart: 40 },
  stalinTower: { w: 2, d: 2, floors: [10, 18], zone: "mid", lot: 7 /* Plaza */, apart: 14 },
  blueMosque: { w: [4, 5], d: [4, 5], zone: "core", lot: 7 /* Plaza */, apart: 30 },
  bazaar: { w: [4, 5], d: 2, zone: "core", lot: 2 /* Paved */, frontage: true, apart: 14 },
  stepPyramid: { w: [5, 6], d: [5, 6], zone: "core", lot: 12 /* Gravel */, gap: 1, apart: 40 },
  ballCourt: { w: [4, 5], d: 3, zone: "mid", lot: 12 /* Gravel */, allow: 3, apart: 20 },
  // the classical world: the amphitheatre out by the edge, the aqueduct striding in from the country,
  // and the forum taking the middle of town
  // (one a town, however big: the apart is wider than any town)
  colosseum: { w: [6, 7], d: [5, 6], zone: "edge", lot: 12 /* Gravel */, gap: 1, apart: 400 },
  // an open lot: a street runs under the arches, and a solid one nine tiles long can wall a town's
  // road network off from its causeway landing (npm run seams caught exactly that)
  aqueduct: { w: [6, 9], d: 2, zone: "edge", lot: 1 /* Lawn */, along: "wide", apart: 30, frontage: false, solid: false },
  forum: { w: [5, 6], d: [4, 5], zone: "core", lot: 7 /* Plaza */, apart: 40 },
  // China: the earth house out at the edge, the gateway on a street and the drum tower on the square
  tulou: { w: [4, 5], d: [4, 5], zone: "edge", lot: 5 /* Yard */, apart: 14 },
  paifang: { w: [3, 4], d: 1, zone: "core", lot: 7 /* Plaza */, roadside: true, apart: 18 },
  drumTower: { w: 2, d: 2, floors: 3, zone: "core", lot: 7 /* Plaza */, apart: 30 },
  // Southeast Asia: the temple and its stupa, the temple-mountain out of town, the split gate on the street
  wat: { w: [3, 4], d: [4, 5], zone: "mid", lot: 2 /* Paved */, apart: 24 },
  khmerTemple: { w: [6, 7], d: [6, 7], zone: "edge", lot: 12 /* Gravel */, gap: 1, apart: 40 },
  splitGate: { w: [2, 3], d: 2, zone: "core", lot: 7 /* Plaza */, roadside: true, apart: 16 },
  // the bay city: the tower wants the highest ground it can find, the palace a lot of room for its
  // lagoon, and the two towers stand downtown
  coitTower: { w: 3, d: 3, zone: "mid", lot: 1 /* Lawn */, highest: true, gap: 1, apart: 40 },
  palaceFineArts: { w: [6, 7], d: [5, 6], zone: "mid", lot: 1 /* Lawn */, gap: 1, apart: 40 },
  // both stand on a downtown lot the size of any other tower's, or they never find room to stand at all
  pyramidTower: { w: 2, d: 2, floors: [26, 44], zone: "core", lot: 7 /* Plaza */, apart: 40 },
  glassSpire: { w: 2, d: 2, floors: [34, 58], zone: "core", lot: 7 /* Plaza */, apart: 30 },
  // Tokyo (the tokyo style's swaps; towers are 2x2 so they find room in a packed core)
  shibuyaTower: { w: 2, d: 2, floors: [5, 7], zone: "core", lot: 7 /* Plaza */, apart: 40 },
  scrambleCrossing: { w: 3, d: 3, zone: "core", lot: 6 /* Asphalt */, ...OPEN3, apart: 40 },
  metroGovTower: { w: 4, d: 3, zone: "core", lot: 7 /* Plaza */, gap: 1, apart: 40 },
  cocoonTower: { w: 2, d: 2, floors: [34, 50], zone: "core", lot: 7 /* Plaza */, apart: 30 },
  godzillaTower: { w: 2, d: 2, floors: 30, zone: "core", lot: 2 /* Paved */, apart: 40 },
  yokocho: { w: 3, d: 1, floors: 2, zone: "mid", lot: 2 /* Paved */ },
  kabukichoGate: { w: 3, d: 1, zone: "core", lot: 7 /* Plaza */, roadside: true, solid: false, apart: 30 },
  // Paris
  eiffelTower: { w: 3, d: 3, zone: "mid", lot: 3 /* Park */, gap: 1, apart: 80 },
  arcDeTriomphe: { w: 2, d: 2, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  notreDame: { w: 3, d: [4, 5], zone: "core", lot: 7 /* Plaza */, apart: 60 },
  louvrePyramid: { w: [4, 5], d: 4, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  sacreCoeur: { w: 3, d: 3, zone: "mid", lot: 1 /* Lawn */, highest: true, gap: 1, apart: 60 },
  // New York
  empireState: { w: 2, d: 2, floors: 90, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  chryslerBuilding: { w: 2, d: 2, floors: 70, zone: "core", lot: 7 /* Plaza */, apart: 40 },
  flatironBuilding: { w: 2, d: 2, floors: 22, zone: "core", lot: 2 /* Paved */, apart: 40 },
  statueLiberty: { w: 3, d: 3, zone: "edge", lot: 1 /* Lawn */, waterside: "prefer", apart: 80 },
  // Washington
  capitolBuilding: { w: [5, 6], d: [3, 4], zone: "core", lot: 1 /* Lawn */, gap: 1, apart: 80 },
  washingtonMonument: { w: 2, d: 2, zone: "mid", lot: 1 /* Lawn */, apart: 80 },
  lincolnMemorial: { w: 3, d: 2, zone: "mid", lot: 1 /* Lawn */, apart: 60 },
  whiteHouse: { w: 4, d: 3, zone: "mid", lot: 1 /* Lawn */, gap: 1, apart: 80 },
  jeffersonMemorial: { w: 4, d: 4, zone: "mid", lot: 3 /* Park */, waterside: "prefer", apart: 80 },
  // ---- broadcasting: a country town's radio station with its mast beside it, a city's news station and towers
  radioStation: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, apart: 12 },
  newsStation: { w: 3, d: 2, floors: [3, 4], zone: "mid", lot: 2 /* Paved */, apart: 10 },
  radioMast: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 10 },
  radioTower: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, apart: 10 },
  windmillRow: { w: 3, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true, frontage: false },
  taverna: { w: 2, d: 1, zone: "any", lot: 7 /* Plaza */, waterside: "prefer", apart: 5 },
  stoneAmphitheatre: { w: 4, d: 4, zone: "edge", lot: 12 /* Gravel */, ...OPEN3, allow: 3 },
  // ---- houses of worship (the parish church is drawn by the town mesher; these are the rest) ----
  synagogue: { w: [2, 3], d: 3, zone: "mid", lot: 2 /* Paved */, apart: 20 },
  orthodoxChurch: { w: 3, d: [3, 4], zone: "core", lot: 7 /* Plaza */, apart: 20 },
  staveChurch: { w: 2, d: 2, zone: "edge", lot: 1 /* Lawn */, apart: 20 },
  stupa: { w: 3, d: 3, zone: "mid", lot: 12 /* Gravel */, apart: 16 },
  // ---- the working country and the road hauler's stops ----
  lumberYard: { w: [4, 5], d: 4, zone: "edge", lot: 5 /* Yard */, apart: 14 },
  cannery: { w: [3, 4], d: 3, zone: "edge", lot: 12 /* Gravel */, waterside: "prefer", apart: 12 },
  grainElevator: { w: [3, 4], d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 14 },
  coolingTowers: { w: 6, d: 5, zone: "edge", lot: 12 /* Gravel */, gap: 1, waterside: "prefer", apart: 40 },
  truckStop: { w: [4, 5], d: 3, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 14 },
  weighStation: { w: 4, d: 2, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 20 },
  carWash: { w: 3, d: 3, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 8 },
  scrapyard: { w: 4, d: 4, zone: "edge", lot: 12 /* Gravel */, apart: 16 },
  // ---- landmarks and roadside fun ----
  roadsideGiant: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, roadside: true, apart: 20 },
  driveIn: { w: 6, d: 5, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 24 },
  rollerCoaster: { w: [5, 6], d: [4, 5], zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  waterMill: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */, waterside: true, apart: 12 },
  leaningTower: { w: 1, d: 1, zone: "core", lot: 7 /* Plaza */, apart: 40 },
  stoneCircle: { w: 3, d: 3, zone: "edge", lot: 1 /* Lawn */, solid: false, frontage: false, highest: true, apart: 24 },
  airshipMast: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 30 },
  // ---- the nine newer biomes' own ----
  // the Fire Coast
  geothermalPlant: { w: 4, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  hotSpringBaths: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 14 },
  turfHouse: { w: 1, d: 1, zone: "any", lot: 1 /* Lawn */, gap: 1 },
  blackChurch: { w: 1, d: 2, zone: "edge", lot: 1 /* Lawn */, apart: 14 },
  // the Fjordlands
  boatShed: { w: 1, d: 1, zone: "any", lot: 12 /* Gravel */, waterside: "prefer", apart: 3 },
  fishFarm: { w: 3, d: 3, zone: "edge", lot: 8 /* Deck */, waterside: true, apart: 16 },
  rorbu: { w: 1, d: 1, zone: "any", lot: 8 /* Deck */, waterside: "prefer" },
  // the Big Timber
  loggingCamp: { w: 3, d: 3, zone: "edge", lot: 5 /* Yard */, apart: 14 },
  lookoutTower: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 10 },
  seaplaneBase: { w: 3, d: 2, zone: "edge", lot: 8 /* Deck */, waterside: true, apart: 20 },
  // the Red Rock
  roadhouse: { w: 3, d: 2, zone: "edge", lot: 12 /* Gravel */, roadside: true, apart: 12 },
  pueblo: { w: 2, d: 2, zone: "edge", lot: 13 /* Sand */, apart: 8 },
  dinosaurDig: { w: 3, d: 3, zone: "edge", lot: 13 /* Sand */, solid: false, apart: 30 },
  // Dragon Bay
  floatingVillage: { w: 3, d: 3, zone: "any", lot: 8 /* Deck */, waterside: true, apart: 14 },
  junkMooring: { w: 2, d: 2, zone: "any", lot: 8 /* Deck */, waterside: true, apart: 8 },
  karstPagoda: { w: 1, d: 1, zone: "mid", lot: 12 /* Gravel */, apart: 10 },
  // the Steppe
  yurtCamp: { w: 3, d: 3, zone: "edge", lot: 5 /* Yard */, solid: false },
  yurt: { w: 1, d: 1, zone: "any", lot: 5 /* Yard */, gap: 1 },
  ovoo: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true, frontage: false, apart: 8 },
  horseCorral: { w: 3, d: 2, zone: "edge", lot: 5 /* Yard */, solid: false, frontage: false },
  // the Salt Pan
  saltWorks: { w: 4, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 14 },
  saltHotel: { w: 2, d: 2, zone: "mid", lot: 12 /* Gravel */, apart: 10 },
  trainCemetery: { w: 4, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 20 },
  // the Mangroves
  stiltVillage: { w: 3, d: 3, zone: "any", lot: 8 /* Deck */, waterside: true, apart: 12 },
  shrimpFarm: { w: 3, d: 3, zone: "edge", lot: 5 /* Yard */, solid: false, waterside: "prefer", apart: 12 },
  // the Lavender Plateau
  lavenderField: { w: [5, 7], d: [4, 6], zone: "edge", lot: 5 /* Yard */, solid: false, frontage: false },
  perfumery: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */, apart: 10 },
  bastide: { w: 2, d: 2, zone: "edge", lot: 5 /* Yard */, gap: 1 },
  dovecote: { w: 1, d: 1, zone: "edge", lot: 5 /* Yard */, frontage: false, apart: 4 },
  // ---- things to find (placed out in the country: world/roadside.ts) ----
  ghostTown: { w: 6, d: 5, zone: "edge", lot: 5 /* Yard */, apart: 40 },
  abandonedMine: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  shipwreck: { w: 3, d: 2, zone: "edge", lot: 13 /* Sand */, apart: 30 },
  planeWreck: { w: 3, d: 3, zone: "edge", lot: 0 /* None */, apart: 30 },
  ruinedAbbey: { w: 4, d: 5, zone: "edge", lot: 1 /* Lawn */, apart: 40 },
  bunker: { w: 2, d: 2, zone: "edge", lot: 1 /* Lawn */, apart: 30 },
  crashedSatellite: { w: 2, d: 2, zone: "edge", lot: 0 /* None */, solid: false, apart: 40 },
  capeLight: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 40 },
  hermitCabin: { w: 1, d: 1, zone: "edge", lot: 5 /* Yard */, apart: 20 },
  // ---- heavy industry and energy ----
  refinery: { w: 6, d: 5, zone: "edge", lot: 12 /* Gravel */, gap: 1, waterside: "prefer", apart: 40 },
  steelworks: { w: 6, d: 5, zone: "edge", lot: 12 /* Gravel */, gap: 1, apart: 40 },
  textileMill: { w: 4, d: 3, zone: "edge", lot: 2 /* Paved */, waterside: "prefer", apart: 30 },
  paperMill: { w: 5, d: 4, zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  cementPlant: { w: 5, d: 4, zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  containerTerminal: { w: 6, d: 5, zone: "edge", lot: 6 /* Asphalt */, waterside: "prefer", apart: 40 },
  autoPlant: { w: 7, d: 5, zone: "edge", lot: 6 /* Asphalt */, apart: 40 },
  solarFarm: { w: 6, d: 5, zone: "edge", lot: 12 /* Gravel */, frontage: false, apart: 30 },
  offshoreWind: { w: 3, d: 3, zone: "any", lot: 0 /* None */, waterside: true, frontage: false, apart: 12 },
  tidalBarrage: { w: 8, d: 2, zone: "any", lot: 2 /* Paved */, waterside: true, frontage: false, apart: 60 },
  pumpedHydro: { w: 5, d: 4, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 40 },
  // ---- the driver's destinations ----
  summitCafe: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 30 },
  scenicOverlook: { w: 2, d: 2, zone: "edge", lot: 6 /* Asphalt */, highest: true, apart: 16 },
  hillClimb: { w: 3, d: 2, zone: "edge", lot: 6 /* Asphalt */, apart: 40 },
  rallyStage: { w: 4, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 40 },
  dragStrip: { w: 3, d: 14, zone: "edge", lot: 6 /* Asphalt */, frontage: false, apart: 60 },
  provingGround: { w: 10, d: 8, zone: "edge", lot: 6 /* Asphalt */, frontage: false, apart: 80 },
  iceRoadCheckpoint: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, waterside: "prefer", apart: 30 },
  motorwayServices: { w: 6, d: 4, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 40 },
  // ---- institutions ----
  filmStudio: { w: 6, d: 5, zone: "edge", lot: 6 /* Asphalt */, apart: 40 },
  militaryBase: { w: 8, d: 6, zone: "edge", lot: 12 /* Gravel */, gap: 1, apart: 60 },
  borderFort: { w: 4, d: 4, zone: "edge", lot: 1 /* Lawn */, apart: 40 },
  cableCarStation: { w: 2, d: 2, zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  researchDome: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 30 },
  festivalGrounds: { w: 6, d: 6, zone: "edge", lot: 1 /* Lawn */, frontage: false, apart: 40 },
  // ---- the newer styles' signature buildings (swapped in by the town's style: STYLE_SWAPS in towns.ts) ----
  // Mughal India
  tajMahal: { w: [5, 6], d: [5, 6], zone: "mid", lot: 7 /* Plaza */, gap: 1, apart: 60 },
  stepwell: { w: 3, d: 3, zone: "mid", lot: 7 /* Plaza */, apart: 20 },
  hawaMahal: { w: 3, d: 1, floors: 5, zone: "core", lot: 2 /* Paved */, apart: 30 },
  // Andalusia
  alhambra: { w: [5, 6], d: [4, 5], zone: "core", lot: 7 /* Plaza */, gap: 1, apart: 60 },
  giralda: { w: 1, d: 1, zone: "core", lot: 7 /* Plaza */, apart: 30 },
  // the Andes
  machuPicchu: { w: [6, 7], d: [5, 6], zone: "edge", lot: 12 /* Gravel */, highest: true, gap: 1, apart: 60 },
  sunTemple: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 30 },
  // the Sahel
  mudMosque: { w: [4, 5], d: 4, zone: "core", lot: 13 /* Sand */, apart: 40 },
  mudGranary: { w: 1, d: 1, zone: "edge", lot: 13 /* Sand */, apart: 3 },
  // the Himalaya
  dzong: { w: [6, 7], d: [4, 5], zone: "edge", lot: 12 /* Gravel */, highest: true, gap: 1, apart: 60 },
  // the Norse
  longhouse: { w: 3, d: 2, zone: "core", lot: 5 /* Yard */, apart: 12 },
  runestone: { w: 1, d: 1, zone: "any", lot: 1 /* Lawn */, apart: 10 },
  // Venice
  campanile: { w: 1, d: 1, zone: "core", lot: 7 /* Plaza */, apart: 40 },
  dogePalace: { w: 4, d: 3, zone: "core", lot: 7 /* Plaza */, waterside: "prefer", apart: 60 },
  marcoBasilica: { w: 3, d: 4, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  // a stretch of quay with the gondolas tied up below it: open ground, on the water, a few blocks apart
  gondolaStation: { w: 1, d: 1, zone: "any", lot: 7 /* Plaza */, solid: false, waterside: true, apart: 7 },
  // London
  bigBen: { w: 2, d: 3, zone: "core", lot: 7 /* Plaza */, waterside: "prefer", apart: 80 },
  gherkin: { w: 2, d: 2, floors: [30, 44], zone: "core", lot: 7 /* Plaza */, apart: 40 },
  // New Orleans
  jazzClub: { w: 1, d: 1, floors: 2, zone: "core", lot: 2 /* Paved */, apart: 4 },
  tombYard: { w: 4, d: 4, zone: "edge", lot: 12 /* Gravel */, apart: 20 },
  // Havana
  cigarFactory: { w: 3, d: 2, zone: "mid", lot: 2 /* Paved */, apart: 20 },
  morroFort: { w: 4, d: 4, zone: "edge", lot: 12 /* Gravel */, waterside: "prefer", apart: 60 },
  // Hong Kong
  bankTower: { w: 2, d: 2, floors: [60, 80], zone: "core", lot: 7 /* Plaza */, apart: 40 },
  walledCity: { w: 3, d: 3, floors: [10, 14], zone: "mid", lot: 2 /* Paved */, apart: 16 },
  // Las Vegas
  casinoPyramid: { w: 4, d: 4, zone: "mid", lot: 7 /* Plaza */, apart: 40 },
  welcomeSign: { w: 1, d: 1, zone: "edge", lot: 1 /* Lawn */, roadside: true, apart: 80 },
  needleTower: { w: 2, d: 2, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  fountainResort: { w: 5, d: 4, zone: "mid", lot: 7 /* Plaza */, apart: 40 },
  // the Sphere: a theatre that is one great ball of LEDs on a low podium (on the Strip, towns.ts STRIP_KINDS)
  sphereTheatre: { w: 4, d: 4, zone: "mid", lot: 7 /* Plaza */, apart: 40 },
  // the Gulf
  supertall: { w: 2, d: 2, floors: [140, 170], zone: "core", lot: 7 /* Plaza */, apart: 80 },
  sailHotel: { w: 3, d: 3, zone: "any", lot: 7 /* Plaza */, waterside: "prefer", apart: 60 },
  // the Mother Road
  neonMotel: { w: 3, d: 1, zone: "edge", lot: 6 /* Asphalt */, roadside: true, apart: 8 },
  wigwamMotel: { w: 3, d: 2, zone: "edge", lot: 12 /* Gravel */, roadside: true, apart: 20 },
  giantArrows: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, roadside: true, apart: 30 },
  // the Shire
  hobbitHole: { w: 1, d: 1, zone: "any", lot: 1 /* Lawn */ },
  partyTree: { w: 3, d: 3, zone: "mid", lot: 1 /* Lawn */, apart: 20 },
  hobbitInn: { w: 2, d: 1, zone: "core", lot: 1 /* Lawn */, apart: 8 },
  // the White City
  whiteCitadel: { w: [6, 7], d: [5, 6], zone: "core", lot: 7 /* Plaza */, gap: 1, highest: true, apart: 80 },
  beaconTower: { w: 1, d: 1, zone: "edge", lot: 12 /* Gravel */, highest: true, apart: 12 },
  // the spirit town
  grandBathhouse: { w: [4, 5], d: 4, zone: "mid", lot: 7 /* Plaza */, waterside: "prefer", apart: 40 },
  clockGate: { w: 2, d: 2, zone: "core", lot: 7 /* Plaza */, apart: 30 },
  // steampunk
  clockworkTower: { w: 2, d: 2, zone: "core", lot: 7 /* Plaza */, apart: 40 },
  boilerWorks: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 14 },
  skyDock: { w: 3, d: 3, zone: "edge", lot: 12 /* Gravel */, apart: 40 },
  // solarpunk
  gardenTower: { w: 2, d: 2, floors: [24, 36], zone: "mid", lot: 1 /* Lawn */, apart: 5 },
  solarCanopy: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 8 },
  bioDome: { w: 3, d: 3, zone: "mid", lot: 3 /* Park */, apart: 30 },
  // cyberpunk
  megablock: { w: 3, d: 3, floors: [34, 46], zone: "mid", lot: 2 /* Paved */, apart: 12 },
  holoTower: { w: 2, d: 2, floors: [50, 70], zone: "core", lot: 7 /* Plaza */, apart: 14 },
  nightMarket: { w: 3, d: 1, zone: "mid", lot: 2 /* Paved */, apart: 6 },
  // Chicago: one of each landmark a town (the Bean is a plaza with the sculpture standing solid in its middle)
  willisTower: { w: 2, d: 2, floors: 108, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  hancockCenter: { w: 2, d: 2, floors: 100, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  marinaCity: { w: 2, d: 2, floors: 65, zone: "core", lot: 2 /* Paved */, waterside: "prefer", apart: 60 },
  wrigleyBuilding: { w: 2, d: 2, floors: 8, zone: "core", lot: 7 /* Plaza */, waterside: "prefer", apart: 60 },
  tribuneTower: { w: 2, d: 2, floors: 13, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  chicagoTheatre: { w: 2, d: 2, floors: 6, zone: "core", lot: 2 /* Paved */, apart: 60 },
  cloudGate: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  // Amsterdam: the museum and the church on their squares, the station out on the water, the mill, the canal houses
  rijksmuseum: { w: 3, d: 2, zone: "core", lot: 7 /* Plaza */, waterside: "prefer", apart: 60 },
  centraalStation: { w: 4, d: 2, zone: "edge", lot: 2 /* Paved */, roadside: true, waterside: "prefer", apart: 60 },
  westerkerk: { w: 2, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 30 },
  deGooyer: { w: 1, d: 1, zone: "mid", lot: 2 /* Paved */, waterside: "prefer", apart: 30 },
  canalHouses: { w: [3, 4], d: 1, floors: [2, 3], zone: "any", lot: 2 /* Paved */ },
  // Rome: one of each a city, however big (apart is wider than any town; world/towns.ts STYLE_CORE puts the big ones down early, before the filler packs the
  // middle); the chariot course is long and runs its front along a street out at the edge
  circusMaximus: { w: [9, 11], d: [3, 4], zone: "edge", lot: 12 /* Gravel */, gap: 1, apart: 400 },
  pantheon: { w: 2, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 400 },
  stPeters: { w: 4, d: [5, 6], zone: "mid", lot: 7 /* Plaza */, gap: 1, apart: 400 },
  vittoriano: { w: 4, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 400 },
  treviFountain: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 400 },
  spanishSteps: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, apart: 400 },
  // Osaka: one canal stretch, tower, castle and pair of sky towers a town; an arcade every few streets
  dotonbori: { w: [5, 6], d: 3, zone: "core", lot: 2 /* Paved */, apart: 400 },
  tsutenkaku: { w: 2, d: 2, zone: "core", lot: 2 /* Paved */, apart: 400 },
  osakaCastle: { w: 4, d: 4, zone: "core", lot: 3 /* Park */, gap: 1, apart: 400 },
  shotengai: { w: 3, d: 2, floors: 2, zone: "mid", lot: 2 /* Paved */, apart: 10 },
  umedaSky: { w: 2, d: 2, floors: 40, zone: "core", lot: 7 /* Plaza */, apart: 400 },
  // places to eat out and the arcade (world/eateries.ts says which a town gets): small fronts on the shopping streets
  arcade: { w: 2, d: 1, zone: "mid", lot: 2 /* Paved */, apart: 10 },
  ramenShop: { w: 1, d: 1, floors: [1, 2], zone: "mid", lot: 2 /* Paved */, apart: 4 },
  sushiBar: { w: 1, d: 1, floors: [1, 2], zone: "core", lot: 2 /* Paved */, apart: 4 },
  dumplingHouse: { w: 1, d: 1, floors: [1, 2], zone: "mid", lot: 2 /* Paved */, apart: 4 },
  noodleBar: { w: 1, d: 1, floors: 1, zone: "mid", lot: 2 /* Paved */, apart: 4 },
  trattoria: { w: 1, d: 1, floors: [1, 2], zone: "core", lot: 7 /* Plaza */, apart: 4 },
  curryHouse: { w: 1, d: 1, floors: [1, 2], zone: "mid", lot: 2 /* Paved */, apart: 4 },
  deli: { w: 1, d: 1, floors: [1, 2], zone: "core", lot: 2 /* Paved */, apart: 4 },
  taqueria: { w: 1, d: 1, floors: 1, zone: "mid", lot: 2 /* Paved */, apart: 4 },
  kebabShop: { w: 1, d: 1, floors: [1, 2], zone: "mid", lot: 2 /* Paved */, apart: 4 },
  chippy: { w: 1, d: 1, floors: [1, 2], zone: "mid", lot: 2 /* Paved */, apart: 4 }
};

// src/world/eateries.ts
var FOOD_KINDS = [
  "ramenShop",
  "sushiBar",
  "dumplingHouse",
  "noodleBar",
  "trattoria",
  "curryHouse",
  "deli",
  "taqueria",
  "kebabShop",
  "chippy"
];
var EATERY_KINDS = ["arcade", ...FOOD_KINDS];
var EATERY_INFO = {
  arcade: { label: "Arcade", icon: "\u{1F579}\uFE0F", cuisine: "games" },
  ramenShop: { label: "Ramen shop", icon: "\u{1F35C}", cuisine: "Japanese" },
  sushiBar: { label: "Sushi bar", icon: "\u{1F363}", cuisine: "Japanese" },
  dumplingHouse: { label: "Dumpling house", icon: "\u{1F95F}", cuisine: "Chinese" },
  noodleBar: { label: "Noodle bar", icon: "\u{1F372}", cuisine: "Southeast Asian" },
  trattoria: { label: "Trattoria", icon: "\u{1F355}", cuisine: "Italian" },
  curryHouse: { label: "Curry house", icon: "\u{1F35B}", cuisine: "Indian" },
  deli: { label: "Deli", icon: "\u{1F96A}", cuisine: "sandwiches" },
  taqueria: { label: "Taqueria", icon: "\u{1F32E}", cuisine: "Mexican" },
  kebabShop: { label: "Kebab shop", icon: "\u{1F959}", cuisine: "Turkish" },
  chippy: { label: "Fish & chips", icon: "\u{1F41F}", cuisine: "fish and chips" }
};
var EATERY_LABEL = Object.fromEntries(EATERY_KINDS.map((k) => [k, EATERY_INFO[k].label]));
var EATERY_ICON = Object.fromEntries(EATERY_KINDS.map((k) => [k, EATERY_INFO[k].icon]));

// src/world/skiFields.ts
var FAR_AIR = 0.45 + STEP;

// src/world/railSurface.ts
var DECK_HALF = {
  [3 /* Trestle */]: 0.24,
  [4 /* Viaduct */]: 0.31,
  [5 /* Truss */]: 0.27,
  [6 /* Sea */]: 0.29
};

// src/world/roadside.ts
var ROWS = [
  { kind: "truckStop", count: [0, 2], w: 5, d: 3, where: "road", lane: true },
  { kind: "roadsideGiant", count: [0, 2], w: 1, d: 1, where: "road" },
  { kind: "driveIn", count: [0, 1], w: 6, d: 5, where: "road", lane: true },
  { kind: "lumberYard", count: [0, 1], w: 5, d: 4, where: "forest", lane: true },
  { kind: "grainElevator", count: [0, 1], w: 4, d: 3, where: "field", lane: true },
  { kind: "waterMill", count: [0, 1], w: 2, d: 2, where: "water", lane: true },
  { kind: "oilRig", count: [0, 1], w: 3, d: 3, where: "shore", lane: true },
  { kind: "coolingTowers", count: [0, 1], w: 6, d: 5, where: "water", lane: true },
  { kind: "stoneCircle", count: [0, 1], w: 3, d: 3, where: "high" },
  { kind: "greatWall", count: [0, 3], w: 6, d: 2, where: "ridge" },
  // ---- things to find: rare, and out of the way ----
  { kind: "ghostTown", count: [1, 1], w: 6, d: 5, where: "remote", lane: true, chance: 0.18 },
  { kind: "abandonedMine", count: [1, 1], w: 3, d: 3, where: "high", lane: true, chance: 0.2 },
  { kind: "shipwreck", count: [1, 1], w: 3, d: 2, where: "shore", chance: 0.2 },
  { kind: "planeWreck", count: [1, 1], w: 3, d: 3, where: "forest", chance: 0.15 },
  { kind: "ruinedAbbey", count: [1, 1], w: 4, d: 5, where: "remote", lane: true, chance: 0.18 },
  { kind: "bunker", count: [1, 1], w: 2, d: 2, where: "remote", chance: 0.18 },
  { kind: "crashedSatellite", count: [1, 1], w: 2, d: 2, where: "remote", chance: 0.08 },
  { kind: "capeLight", count: [1, 1], w: 2, d: 2, where: "shore", lane: true, chance: 0.25 },
  { kind: "hermitCabin", count: [0, 1], w: 1, d: 1, where: "forest" },
  // ---- heavy industry and energy that stands out of town ----
  { kind: "refinery", count: [1, 1], w: 6, d: 5, where: "shore", lane: true, chance: 0.12 },
  { kind: "cementPlant", count: [1, 1], w: 5, d: 4, where: "any", lane: true, chance: 0.12 },
  { kind: "paperMill", count: [1, 1], w: 5, d: 4, where: "forest", lane: true, chance: 0.15 },
  { kind: "solarFarm", count: [1, 1], w: 6, d: 5, where: "any", lane: true, chance: 0.25, climate: "warm" },
  { kind: "offshoreWind", count: [1, 3], w: 3, d: 3, where: "shore", chance: 0.2 },
  { kind: "tidalBarrage", count: [1, 1], w: 8, d: 2, where: "shore", lane: true, chance: 0.1 },
  { kind: "pumpedHydro", count: [1, 1], w: 5, d: 4, where: "high", lane: true, chance: 0.12 },
  { kind: "fishFarm", count: [1, 1], w: 3, d: 3, where: "shore", chance: 0.15, climate: "cold" },
  { kind: "seaplaneBase", count: [1, 1], w: 3, d: 2, where: "water", lane: true, chance: 0.12 },
  // ---- the driver's destinations ----
  { kind: "summitCafe", count: [1, 1], w: 2, d: 2, where: "high", lane: true, chance: 0.3 },
  { kind: "scenicOverlook", count: [0, 1], w: 2, d: 2, where: "ridge", lane: true },
  { kind: "hillClimb", count: [1, 1], w: 3, d: 2, where: "road", chance: 0.2 },
  { kind: "rallyStage", count: [1, 1], w: 4, d: 3, where: "forest", lane: true, chance: 0.2 },
  { kind: "dragStrip", count: [1, 1], w: 3, d: 14, where: "any", lane: true, chance: 0.12 },
  { kind: "provingGround", count: [1, 1], w: 10, d: 8, where: "any", lane: true, chance: 0.08 },
  { kind: "iceRoadCheckpoint", count: [1, 1], w: 2, d: 2, where: "water", lane: true, chance: 0.5, climate: "cold" },
  { kind: "motorwayServices", count: [1, 1], w: 6, d: 4, where: "road", lane: true, chance: 0.2 },
  // ---- institutions ----
  { kind: "militaryBase", count: [1, 1], w: 8, d: 6, where: "remote", lane: true, chance: 0.1 },
  { kind: "borderFort", count: [1, 1], w: 4, d: 4, where: "ridge", lane: true, chance: 0.12 },
  { kind: "cableCarStation", count: [1, 1], w: 2, d: 2, where: "high", lane: true, chance: 0.15 },
  { kind: "researchDome", count: [1, 1], w: 3, d: 3, where: "remote", lane: true, chance: 0.12 },
  { kind: "festivalGrounds", count: [1, 1], w: 6, d: 6, where: "field", lane: true, chance: 0.2 },
  // ---- the factions' (docs/factions.html): last, so nothing placed before them moves ----
  { kind: "highwayPatrol", count: [1, 1], w: 2, d: 2, where: "road", lane: true, chance: 0.35 },
  { kind: "checkpoint", count: [1, 1], w: 1, d: 2, where: "road", chance: 0.2 },
  { kind: "asphaltPlant", count: [1, 1], w: 3, d: 3, where: "any", lane: true, chance: 0.15 },
  { kind: "armyDepot", count: [1, 1], w: 4, d: 3, where: "remote", lane: true, chance: 0.15 },
  { kind: "airbase", count: [1, 1], w: 7, d: 5, where: "remote", lane: true, chance: 0.06 },
  { kind: "hangar18", count: [1, 1], w: 4, d: 3, where: "remote", lane: true, chance: 0.07 },
  { kind: "crashSite", count: [1, 1], w: 3, d: 3, where: "remote", lane: true, chance: 0.14 },
  { kind: "landingRing", count: [1, 1], w: 3, d: 3, where: "high", chance: 0.3 },
  { kind: "cropCircle", count: [1, 1], w: 3, d: 3, where: "any", chance: 0.3 },
  { kind: "mailDock", count: [1, 1], w: 2, d: 1, where: "shore", lane: true, chance: 0.2 },
  // ---- out in the middle of nowhere (world/wildKinds.ts, GEN 51): last again, so nothing before them moves ----
  ...WILD_KINDS.map((kind) => {
    const i = WILD_INFO[kind];
    return { kind, count: [1, 1], w: i.size[0], d: i.size[1], where: i.where, chance: i.chance, ...i.climate ? { climate: i.climate } : {}, ...i.lane ? { lane: true } : {} };
  })
];
var NAVAL_DOCK_CHANCE = 0.7;
var navalDockRolled = (seedHash2, cx, cz) => mulberry32(hashString(`${seedHash2}|${cx},${cz}|navalDock`))() < NAVAL_DOCK_CHANCE;
var spacePadRolled = (seedHash2, cx, cz) => mulberry32(hashString(`${seedHash2}|${cx},${cz}|spacePad`))() < SPACEPORT_CHANCE;
var SPACEPORT_CHANCE = 0.14;

// src/world/ports.ts
var QUAY = SEA + 1;

// src/world/railPlan.ts
var RAIL_GRADE = 1 / 6;
var RACK_GRADE = 1 / 2;
var MIN_RADIUS = 6;
var SEA_DECK = SEA + 3;
var HEADINGS = 16;
var DA = Math.PI * 2 / HEADINGS;
var ARC = MIN_RADIUS * DA;
var COS = new Float64Array(HEADINGS);
var SIN = new Float64Array(HEADINGS);
for (let h = 0; h < HEADINGS; h++) {
  COS[h] = Math.cos(h * DA);
  SIN[h] = Math.sin(h * DA);
}
var KG = 2;
var KN = N4 / KG;
var NS = KN * KN * HEADINGS;

// src/world/wonders.ts
var FINISH = {
  pyramid: 13 /* Sand */,
  lostTemple: 12 /* Gravel */,
  greatKopje: 5 /* Yard */,
  plantationManor: 1 /* Lawn */,
  iceHotel: 12 /* Gravel */,
  pleasurePier: 8 /* Deck */,
  dam: 2 /* Paved */,
  castle: 12 /* Gravel */,
  hilltopCastle: 12 /* Gravel */,
  capeTemple: 12 /* Gravel */,
  moatedCastle: 1 /* Lawn */,
  mission: 5 /* Yard */,
  goldenTemple: 7 /* Plaza */,
  launchPad: 12 /* Gravel */,
  greatGeyser: 12 /* Gravel */,
  arcticCathedral: 2 /* Paved */,
  giantTree: 3 /* Park */,
  monumentButtes: 13 /* Sand */,
  cloudTemple: 12 /* Gravel */,
  khanStatue: 2 /* Paved */,
  speedStrip: 13 /* Sand */,
  floatingMarket: 8 /* Deck */,
  lavenderAbbey: 5 /* Yard */
};

// src/world/fences.ts
var RAIL_KEEP_CLEAR = 1 /* Track */ | 4 /* Deck */ | 16 /* Pad */ | 32 /* Loop */ | 128 /* Reserved */;
var FENCE_RULES = [null, ...REGION_KINDS.map((k) => {
  const c = REGIONS[k].clutter;
  return { lantern: (c.stoneLantern ?? 0) * 12, snowFence: (c.snowFence ?? 0) * 12, drystone: c.drystoneWall !== void 0 };
})];

// src/world/geology.ts
var SOURCE_MIN = SEA + 13;

// src/world/generate.ts
var NN = N4 * N4;
var GEN_VERSION = 52;
var PAD = 40;
var P = N4 + 2 * PAD;
var PP = P * P;
var RIM_DEPTH = SEA - 5;

// src/world/spawn.ts
var SPAWN = {
  /** Cells a car must be able to reach from the start, the start's own included. */
  cells: 10,
  /** Land dealers that must be reachable within `dealerReach` cells of the start. */
  dealers: 2,
  dealerReach: 3,
  /** Auto Shops that must be reachable within `shopReach` cells of the start. */
  shops: 2,
  shopReach: 2,
  /** How far (cells, by road) the reach is counted. */
  depth: 6,
  /** The share of passing candidates the start is chosen among, best first. */
  top: 0.1
};
var START_KINDS = /* @__PURE__ */ new Set(["continent", "full"]);
var SHOP_KINDS = /* @__PURE__ */ new Set(["continent", "full", "small"]);
var STEPS = [[1, 0], [0, 1], [-1, 0], [0, -1]];
function carAcross(seedHash2, cx, cz, d) {
  return landBorder(seedHash2, cx, cz, d) || straitAt(seedHash2, cx, cz, d) >= 0;
}
var Lattice = class {
  constructor(seedHash2) {
    this.seedHash = seedHash2;
  }
  kinds = /* @__PURE__ */ new Map();
  edges = /* @__PURE__ */ new Map();
  kind(cx, cz) {
    const k = `${cx},${cz}`;
    let v = this.kinds.get(k);
    if (v === void 0) {
      v = cellKind(this.seedHash, cx, cz);
      this.kinds.set(k, v);
    }
    return v;
  }
  across(cx, cz, d) {
    const [ax, az, ad] = d === 2 ? [cx - 1, cz, 0] : d === 3 ? [cx, cz - 1, 1] : [cx, cz, d];
    const k = `${ax},${az},${ad}`;
    let v = this.edges.get(k);
    if (v === void 0) {
      v = carAcross(this.seedHash, ax, az, ad);
      this.edges.set(k, v);
    }
    return v;
  }
  dealer(cx, cz) {
    return plannedDealer(this.seedHash, cx, cz);
  }
};
function scoreSpawn(seedHash2, cx, cz, lat = new Lattice(seedHash2)) {
  const out = { cx, cz, ok: false, why: null, cells: 0, dealers: 0, shops: 0, score: 0 };
  if (!START_KINDS.has(lat.kind(cx, cz))) {
    out.why = "not a whole island or a county";
    return out;
  }
  const dist = /* @__PURE__ */ new Map([[`${cx},${cz}`, 0]]);
  const queue = [[cx, cz, 0]];
  let score = 0;
  for (let h = 0; h < queue.length; h++) {
    const [x, z, d] = queue[h];
    const dealer = lat.dealer(x, z), shop = SHOP_KINDS.has(lat.kind(x, z));
    if (dealer && d <= SPAWN.dealerReach) out.dealers++;
    if (shop && d <= SPAWN.shopReach) out.shops++;
    score += ((dealer ? 3 : 0) + (shop ? 1 : 0) + 0.4) / (1 + d);
    if (d >= SPAWN.depth) continue;
    for (let s = 0; s < 4; s++) {
      if (!lat.across(x, z, s)) continue;
      const nx = x + STEPS[s][0], nz = z + STEPS[s][1], k = `${nx},${nz}`;
      if (dist.has(k)) continue;
      dist.set(k, d + 1);
      queue.push([nx, nz, d + 1]);
    }
  }
  out.cells = queue.length;
  out.score = score + Math.min(out.cells, 30) * 0.05;
  out.why = out.cells < SPAWN.cells ? `only ${out.cells} cells reachable by car` : out.dealers < SPAWN.dealers ? `${out.dealers} land dealer${out.dealers === 1 ? "" : "s"} within ${SPAWN.dealerReach} cells` : out.shops < SPAWN.shops ? `${out.shops} shop${out.shops === 1 ? "" : "s"} within ${SPAWN.shopReach} cells` : !stagesPlanned(seedHash2, cx, cz, lat) ? "no launch pad or naval dock planned next door" : null;
  out.ok = out.why === null;
  return out;
}
function stagesPlanned(seedHash2, cx, cz, lat) {
  let pad = false, dock = false;
  for (let dz = -1; dz <= 1; dz++) for (let dx = -1; dx <= 1; dx++) {
    const x = cx + dx, z = cz + dz, k = lat.kind(x, z);
    if (k === "sea") continue;
    pad ||= spacePadRolled(seedHash2, x, z);
    dock ||= k !== "continent" || [0, 1, 2, 3].some((d) => lat.kind(x + STEPS[d][0], z + STEPS[d][1]) === "sea") ? navalDockRolled(seedHash2, x, z) : false;
  }
  return pad && dock;
}
function rankSpawns(seedHash2, around, radius, pick3, allow = () => true) {
  const lat = new Lattice(seedHash2);
  let all = [];
  for (const r of [radius, radius * 2, radius * 4]) {
    all = [];
    for (let dz = -r; dz <= r; dz++) {
      for (let dx = -r; dx <= r; dx++) {
        const cx = around[0] + dx, cz = around[1] + dz;
        if (allow(cx, cz)) all.push(scoreSpawn(seedHash2, cx, cz, lat));
      }
    }
    if (all.some((s) => s.ok)) break;
  }
  const ok = all.filter((s) => s.ok).sort((a, b) => b.score - a.score || a.cx - b.cx || a.cz - b.cz);
  if (!ok.length) return all.filter((s) => s.why !== "not a whole island or a county").sort((a, b) => b.score - a.score);
  const top = Math.max(1, Math.ceil(ok.length * SPAWN.top));
  const i = Math.min(top - 1, Math.floor(pick3 * top));
  return [ok[i], ...ok.slice(0, i), ...ok.slice(i + 1)];
}

// server/index.ts
var PORT = Number(process.env.PORT) || 8787;
var SEED = process.env.WORLD_SEED || "online";
var PEPPER = process.env.PIN_PEPPER || "tinyfleet";
var MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 100;
var HERE = fileURLToPath(new URL(".", import.meta.url));
var SITE = existsSync(join2(HERE, "play", "index.html")) && !process.env.PUBLIC_DIR ? HERE : null;
var PUBLIC = SITE ? join2(HERE, "play") : resolve(process.env.PUBLIC_DIR || [join2(HERE, "public"), join2(HERE, "..", "..", "dist"), join2(process.cwd(), "dist")].find((d) => existsSync(join2(d, "index.html"))) || join2(HERE, "public"));
var guests = /* @__PURE__ */ new Set();
var MAX_GUESTS = Number(process.env.MAX_GUESTS) || GUEST.max;
var AUTO_PER_HOUR = 3;
var AUTO_KEEP_DAYS = 14;
var autoMade = /* @__PURE__ */ new Map();
var AUTO_A = ["Amber", "Brisk", "Copper", "Dusty", "Ember", "Fable", "Gale", "Hazel", "Indigo", "Jolly", "Kestrel", "Lucky", "Maple", "Nimble", "Olive", "Plucky", "Quick", "Rusty", "Sunny", "Tidy", "Umber", "Velvet", "Willow", "Zesty"];
var AUTO_B = ["Heron", "Otter", "Badger", "Finch", "Marten", "Plover", "Stoat", "Wren", "Lark", "Hare", "Vole", "Gull", "Newt", "Crane", "Robin", "Fox", "Tern", "Mole", "Owl", "Pike", "Swift", "Teal", "Yak", "Elk"];
var funnel = /* @__PURE__ */ new Map();
function count(kind) {
  const day = (/* @__PURE__ */ new Date()).toISOString().slice(0, 10);
  const d = funnel.get(day) ?? {};
  d[kind] = (d[kind] ?? 0) + 1;
  funnel.set(day, d);
}
function step(doc, kind) {
  if (!doc.via || doc.viaDone?.includes(kind)) return;
  (doc.viaDone ??= []).push(kind);
  players.touch(doc.id);
  count(kind);
}
var store;
var world;
var worldDirty = false;
var players;
var cells;
var community;
var marks;
var postcards;
var crews;
var crewDocs;
var cards;
var pics;
var board = { players: {}, cpu: {} };
var boardDirty = false;
var conns = /* @__PURE__ */ new Map();
var rooms = /* @__PURE__ */ new Map();
var rentToday = /* @__PURE__ */ new Map();
var signups = /* @__PURE__ */ new Map();
var PIN_WINDOW = 9e5;
var PIN_PER_NAME = 5;
var PIN_PER_IP = 20;
var PIN_NAME_BACKSTOP = 40;
var pinFails = /* @__PURE__ */ new Map();
function recentFails(key, now) {
  const a = (pinFails.get(key) ?? []).filter((t) => now - t < PIN_WINDOW);
  if (a.length) pinFails.set(key, a);
  else pinFails.delete(key);
  return a;
}
function failPin(key, now) {
  pinFails.set(key, [...recentFails(key, now), now]);
  if (pinFails.size > 5e3) for (const k of [...pinFails.keys()]) recentFails(k, now);
}
var seedHash = 0;
var CELL2 = /^-?\d{1,5},-?\d{1,5}$/;
var NAME = /^[A-Za-z0-9_-]{3,16}$/;
var LOT3 = /^(-?\d{1,5},-?\d{1,5})#[bp]\d{1,7}$/;
var TOWN2 = /^(-?\d{1,5},-?\d{1,5})#\d{1,4}$/;
var EDIT_KINDS = /* @__PURE__ */ new Set(["road", "raze", "build", "lift", "wear", "ring", "claim"]);
var freshCell = () => ({ edits: [] });
var sha = (s) => createHash2("sha256").update(s).digest("hex");
var pinHash = (pin, salt) => new Promise((ok, no) => scrypt(`${pin}|${PEPPER}`, salt, 32, (e, k) => e ? no(e) : ok(k.toString("hex"))));
var sameHex = (a, b) => a.length === b.length && timingSafeEqual2(Buffer.from(a, "hex"), Buffer.from(b, "hex"));
var cleanBrand = (b) => {
  const x = b;
  if (!x || typeof x.name !== "string" || typeof x.primary !== "number" || typeof x.accent !== "number" || typeof x.logo !== "string") return null;
  return { name: x.name.replace(/[<>&"`\u0000-\u001f]/g, "").trim().slice(0, 24) || "A fleet", primary: x.primary & 16777215, accent: x.accent & 16777215, logo: [...x.logo].slice(0, 2).join("") };
};
var cheb = (a, b) => {
  const [ax, az] = cellOfKey(a), [bx, bz] = cellOfKey(b);
  return Math.max(Math.abs(ax - bx), Math.abs(az - bz));
};
var whoOf = (pid) => {
  const p = world.people[pid];
  return p ? { id: pid, name: p.name, brand: p.brand } : null;
};
function send(c, m) {
  const ws = "ws" in c ? c.ws : c;
  if (ws.readyState === 1) ws.send(JSON.stringify(m));
}
function landAt(cx, cz, strict) {
  const k = cellKind(seedHash, cx, cz);
  return strict ? k === "full" || k === "continent" : k !== "sea";
}
function hqCells() {
  return new Set(Object.values(world.hqs ?? {}));
}
function turnedDown() {
  return new Set(Object.entries(world.homes).filter(([, v]) => v === "none").map(([k]) => k));
}
function pickHome(skip = turnedDown()) {
  const taken = hqCells();
  for (let r = 0; r <= SPAWN_REACH; r++) {
    let best = null;
    for (let dz = -r; dz <= r; dz++) for (let dx = -r; dx <= r; dx++) {
      if (Math.max(Math.abs(dx), Math.abs(dz)) !== r) continue;
      const key = cellKey(dx, dz);
      if (taken.has(key) || skip.has(key)) continue;
      const sc = scoreSpawn(seedHash, dx, dz);
      if (!sc.ok) continue;
      const d = Math.hypot(dx, dz);
      if (!best || d < best.d - 1e-9 || Math.abs(d - best.d) < 1e-9 && sc.score > best.score) best = { cx: dx, cz: dz, d, score: sc.score };
    }
    if (best) return [best.cx, best.cz];
  }
  return pickHomeAnywhere();
}
var nextHome = null;
function upcomingHome() {
  const k = nextHome ? cellKey(nextHome.at[0], nextHome.at[1]) : "";
  if (!nextHome || hqCells().has(k) || world.homes[k] === "none" || Date.now() - nextHome.t > 6e5) nextHome = { at: pickHome(), t: Date.now() };
  return nextHome.at;
}
function takeHome(near) {
  if (near) {
    const taken = hqCells(), down = turnedDown();
    const open = (cx, cz) => Math.max(Math.abs(cx), Math.abs(cz)) <= SPAWN_REACH && !taken.has(cellKey(cx, cz)) && !down.has(cellKey(cx, cz)) && Math.max(Math.abs(cx - near[0]), Math.abs(cz - near[1])) <= 3;
    const best = rankSpawns(seedHash, near, 3, Math.random(), open)[0];
    if (best?.ok) return [best.cx, best.cz];
  }
  const at = upcomingHome();
  nextHome = null;
  return at;
}
function pickHomeAnywhere() {
  const free = (cx, cz, strict) => Math.max(Math.abs(cx), Math.abs(cz)) <= SPAWN_REACH && !hqCells().has(cellKey(cx, cz)) && landAt(cx, cz, strict);
  const near = (ax, az, r, strict) => {
    const out = [];
    for (let dz = -r; dz <= r; dz++) for (let dx = -r; dx <= r; dx++) if (free(ax + dx, az + dz, strict)) out.push([ax + dx, az + dz]);
    return out.length ? out[Math.floor(Math.random() * out.length)] : null;
  };
  const anchors = [...conns.values()].map((c) => cellOfKey(c.cell)).sort(() => Math.random() - 0.5);
  for (const strict of [true, false]) {
    for (const [ax, az] of anchors) {
      const h = near(ax, az, SPAWN_NEAR, strict);
      if (h) return h;
    }
  }
  for (let k = 0; k < 400; k++) {
    const cx = Math.floor((Math.random() * 2 - 1) * (SPAWN_REACH + 0.999)), cz = Math.floor((Math.random() * 2 - 1) * (SPAWN_REACH + 0.999));
    if (free(cx, cz, true)) return [cx, cz];
  }
  return near(0, 0, SPAWN_REACH, false) ?? [0, 0];
}
function roomOf(c, make) {
  let r = rooms.get(c);
  if (!r && make) rooms.set(c, r = { subs: /* @__PURE__ */ new Map(), host: null, watchers: /* @__PURE__ */ new Set() });
  return r ?? null;
}
var awake = (x) => Date.now() - x.lastMove < 6e3;
function rehost(c) {
  const r = rooms.get(c);
  if (!r) return;
  if (!r.subs.size) {
    if (r.watchers.size) r.host = null;
    else rooms.delete(c);
    return;
  }
  const inside = (x) => x.cell === c;
  const others = [...r.subs.keys()];
  let host = r.host && r.subs.has(r.host) ? r.host : null;
  if (host && !awake(host) && others.some(awake)) host = null;
  if (host && !inside(host) && others.some((x) => inside(x) && awake(x))) host = null;
  if (!host) {
    const ranked = [...r.subs].sort((a, b) => Number(awake(b[0])) - Number(awake(a[0])) || Number(inside(b[0])) - Number(inside(a[0])) || a[1] - b[1]);
    host = ranked[0][0];
  }
  if (host === r.host) return;
  r.host = host;
  for (const x of r.subs.keys()) send(x, { t: "host", c, host: host.pid });
}
function toRoom(c, m, but) {
  const r = rooms.get(c);
  if (!r) return;
  const text = JSON.stringify(m);
  for (const x of r.subs.keys()) if (x !== but && x.ws.readyState === 1) x.ws.send(text);
  for (const g of r.watchers) if (g.ws.readyState === 1) g.ws.send(text);
}
function toGuests(m) {
  if (!guests.size) return;
  const text = JSON.stringify(m);
  for (const g of guests) if (g.ws.readyState === 1) g.ws.send(text);
}
function peopleIn(doc) {
  const ids = /* @__PURE__ */ new Set();
  for (const d of Object.values(doc.deeds ?? {})) ids.add(d.by);
  for (const e of doc.edits) if (e.k === "road" && !e.g && typeof e.by === "string") ids.add(e.by);
  for (const rows of Object.values(doc.landBy ?? {})) if (rows[0]) ids.add(rows[0][0]);
  for (const by of Object.values(doc.pathsBy ?? {})) if (by[0]) ids.add(by[0]);
  for (const r of Object.values(doc.signs ?? {})) ids.add(r.by);
  for (const r of Object.values(doc.drifts ?? {})) ids.add(r.by);
  for (const r of Object.values(doc.laps ?? {})) ids.add(r.by);
  for (const r of Object.values(doc.guests ?? {})) for (const x of r.list) ids.add(x[0]);
  for (const r of Object.values(doc.holders ?? {})) for (const x of r) if (isPid(x[0])) ids.add(x[0]);
  for (const row of Object.values(doc.carriage ?? {})) {
    if (isPid(row[1])) ids.add(row[1]);
    for (const k of Object.keys(row[3] ?? {})) if (isPid(k)) ids.add(k);
  }
  return [...ids].map(whoOf).filter((w) => !!w);
}
async function subscribe(c, want) {
  const next = new Set(want.filter((k) => CELL2.test(k)).slice(0, 80));
  for (const k of [...c.subs]) {
    if (next.has(k)) continue;
    c.subs.delete(k);
    rooms.get(k)?.subs.delete(c);
    if (c.held.delete(k)) cells.hold(`${world.epoch}:${k}`, -1);
    rehost(k);
  }
  for (const k of next) {
    if (c.subs.has(k)) continue;
    c.subs.add(k);
    const doc = await cells.open(`${world.epoch}:${k}`);
    if (conns.get(c.pid) !== c || !c.subs.has(k) || c.held.has(k)) continue;
    c.held.add(k);
    cells.hold(`${world.epoch}:${k}`, 1);
    const r = roomOf(k, true);
    r.subs.set(c, Date.now());
    rehost(k);
    const { usedBy: _kept, laps, ...rest } = doc;
    const shared = laps ? { ...rest, laps: Object.fromEntries(Object.entries(laps).map(([id, { g: _g, ...row }]) => [id, row])) } : rest;
    send(c, { t: "cell", c: k, doc: shared, host: (r.host ?? c).pid, people: peopleIn(doc) });
  }
}
async function guestSubscribe(g, want) {
  const next = new Set(want.filter((k) => CELL2.test(k)).slice(0, GUEST.cells));
  for (const k of [...g.subs]) {
    if (next.has(k)) continue;
    g.subs.delete(k);
    const r = rooms.get(k);
    if (r) {
      r.watchers.delete(g);
      if (!r.subs.size && !r.watchers.size) rooms.delete(k);
    }
    if (g.held.delete(k)) cells.hold(`${world.epoch}:${k}`, -1);
  }
  for (const k of next) {
    if (g.subs.has(k)) continue;
    g.subs.add(k);
    const doc = await cells.open(`${world.epoch}:${k}`);
    if (!guests.has(g) || !g.subs.has(k) || g.held.has(k)) continue;
    g.held.add(k);
    cells.hold(`${world.epoch}:${k}`, 1);
    const r = roomOf(k, true);
    r.watchers.add(g);
    const { usedBy: _kept, laps, ...rest } = doc;
    const shared = laps ? { ...rest, laps: Object.fromEntries(Object.entries(laps).map(([id, { g: _g, ...row }]) => [id, row])) } : rest;
    send(g.ws, { t: "cell", c: k, doc: shared, host: r.host?.pid ?? "", people: peopleIn(doc) });
  }
}
function guestHello(ws, ip, m) {
  if (m.v !== PROTOCOL) {
    refuse(ws, "version", m.v < PROTOCOL ? "The game has been updated: reload." : "This server is running an older version of the game.");
    return null;
  }
  if (m.gen !== world.gen) {
    refuse(ws, "gen", m.gen < world.gen ? "The game has been updated: reload." : "This server\u2019s world was made with an older version of the game.");
    return null;
  }
  if (guests.size >= MAX_GUESTS) {
    refuse(ws, "full", "There are a lot of visitors just now.");
    return null;
  }
  if ([...guests].filter((g2) => g2.ip === ip).length >= GUEST.perIp) {
    refuse(ws, "full", "This address is already watching in a few windows.");
    return null;
  }
  const now = Date.now();
  const g = { ws, ip, subs: /* @__PURE__ */ new Set(), held: /* @__PURE__ */ new Set(), at: null, last: now, msgs: 0, msgsAt: now };
  guests.add(g);
  count("card-guest");
  send(ws, {
    t: "guest",
    world: { seed: world.seed, gen: world.gen, hours: world.hours },
    players: conns.size,
    online: [...conns.values()].map((x) => ({ who: x.who, c: x.cell, look: x.look ?? void 0 }))
  });
  return g;
}
async function guestHandle(g, m) {
  g.last = Date.now();
  switch (m.t) {
    case "sub":
      await guestSubscribe(g, Array.isArray(m.cells) ? m.cells.map(String) : []);
      return;
    case "watch":
      g.at = typeof m.c === "string" && CELL2.test(m.c) ? m.c : g.at;
      return;
    case "bye":
      g.ws.close();
      return;
    default:
      return;
  }
}
function dropGuest(g) {
  if (!guests.delete(g)) return;
  for (const k of g.subs) {
    const r = rooms.get(k);
    if (r) {
      r.watchers.delete(g);
      if (!r.subs.size && !r.watchers.size) rooms.delete(k);
    }
    if (g.held.delete(k)) cells.hold(`${world.epoch}:${k}`, -1);
  }
  g.subs.clear();
}
function autoName() {
  for (let i = 0; i < 60; i++) {
    const n = `${AUTO_A[Math.floor(Math.random() * AUTO_A.length)]}${AUTO_B[Math.floor(Math.random() * AUTO_B.length)]}${i >= 20 ? Math.floor(Math.random() * 90 + 10) : ""}`;
    if (!world.names[n.toLowerCase()]) return n;
  }
  return `Fleet${randomBytes3(3).toString("hex")}`;
}
function moved(c, to) {
  if (to === c.cell) return;
  const was = c.cell;
  c.cell = to;
  rehost(was);
  rehost(to);
}
function refuse(ws, code, why, close = true) {
  send(ws, { t: "refuse", code, why });
  if (close) setTimeout(() => ws.close(), 50);
}
async function hello(ws, ip, m) {
  if (m.v !== PROTOCOL) {
    refuse(ws, "version", m.v < PROTOCOL ? "The game has been updated: reload to play online." : "This server is running an older version of the game.");
    return null;
  }
  if (m.gen !== world.gen) {
    refuse(ws, "gen", m.gen < world.gen ? "The game has been updated: reload to play online." : "This server\u2019s world was made with an older version of the game.");
    return null;
  }
  const now = Date.now();
  const auto = !!m.create?.auto;
  if (auto) {
    const made = (autoMade.get(ip) ?? []).filter((t) => now - t < 36e5);
    if (made.length >= AUTO_PER_HOUR) {
      refuse(ws, "locked", "A few fleets have been made from here this hour: make one with a name and a PIN instead.", false);
      return null;
    }
    autoMade.set(ip, [...made, now]);
  }
  const name = auto ? autoName() : String(m.name ?? "").trim();
  if (!NAME.test(name)) {
    refuse(ws, "name", "A name is 3 to 16 letters, digits, - or _.", false);
    return null;
  }
  const lower = name.toLowerCase();
  let pid = world.names[lower];
  let doc = pid ? await players.open(pid) : null;
  if (!doc || !doc.hash && !doc.auto) {
    const pin = String(m.pin ?? "");
    if (!m.create) {
      refuse(ws, "new", `Nobody is called ${name} here yet.`, false);
      return null;
    }
    if (!auto && (pin.length < 4 || pin.length > 32)) {
      refuse(ws, "pin", "A PIN is 4 to 32 characters.", false);
      return null;
    }
    const made = (signups.get(ip) ?? []).filter((t) => now - t < 36e5);
    if (made.length >= 10) {
      refuse(ws, "locked", "Too many new fleets from here this hour: try again later.", false);
      return null;
    }
    signups.set(ip, [...made, now]);
    pid = `p:${world.nextId++}`;
    const salt = randomBytes3(12).toString("hex");
    const via = auto && isCardId(m.create.from) ? m.create.from : void 0;
    const near = via ? await postcards.ride(via, cleanBrand(m.create.brand)?.name ?? name) : null;
    const home = takeHome(near ?? void 0);
    doc = await players.open(pid);
    Object.assign(doc, {
      id: pid,
      name,
      salt,
      hash: auto ? "" : await pinHash(pin, salt),
      brand: cleanBrand(m.create.brand),
      home,
      epoch: world.epoch,
      created: now,
      seen: now,
      owed: 0,
      record: null,
      ...auto ? { auto: true } : {},
      ...via ? { via } : {}
    });
    if (auto) {
      (world.autos ??= {})[pid] = now;
      count("card-car");
    }
    world.names[lower] = pid;
    world.homes[cellKey(home[0], home[1])] = pid;
    world.people[pid] = { name, brand: doc.brand };
    worldDirty = true;
    players.touch(pid);
    console.log(`new fleet: ${name} (${pid}) at ${home.join(",")}`);
  } else {
    const byToken = !!m.token && !!doc.token && now - (doc.tokenAt ?? 0) < 90 * 864e5 && sha(m.token) === doc.token;
    if (!byToken && doc.auto) {
      refuse(ws, "pin", `${doc.name} has no PIN yet: it can only be played in the browser that made it.`, false);
      return null;
    }
    if (!byToken) {
      const mine = `${ip}|${doc.id}`;
      doc.fails = (doc.fails ?? []).filter((t) => now - t < PIN_WINDOW);
      if (recentFails(mine, now).length >= PIN_PER_NAME || recentFails(ip, now).length >= PIN_PER_IP) {
        refuse(ws, "locked", `Too many wrong PINs from here: wait a quarter of an hour, or pick another name to start a fleet of your own.`, false);
        return null;
      }
      if (doc.fails.length >= PIN_NAME_BACKSTOP) {
        refuse(ws, "locked", `${doc.name} is getting too many wrong PINs just now: try again in a quarter of an hour.`, false);
        return null;
      }
      const ok = typeof m.pin === "string" && m.pin.length > 0 && sameHex(await pinHash(m.pin, doc.salt), doc.hash);
      if (!ok) {
        if (m.pin) {
          failPin(mine, now);
          failPin(ip, now);
          doc.fails.push(now);
          players.touch(doc.id);
        }
        refuse(ws, "pin", m.pin ? `That isn\u2019t the PIN for ${doc.name}. If ${doc.name} isn\u2019t you, that name belongs to another player: pick another to start your own fleet.` : `${doc.name} already belongs to a player here. Type its PIN to sign in, or pick another name to start your own fleet.`, false);
        return null;
      }
      pinFails.delete(mine);
      doc.fails = [];
    }
  }
  pid = doc.id;
  if (doc.epoch !== world.epoch) {
    doc.home = takeHome();
    doc.epoch = world.epoch;
    world.homes[cellKey(doc.home[0], doc.home[1])] = pid;
    worldDirty = true;
    if (doc.record && typeof doc.record === "object") doc.record.gen = -1;
  }
  const old = conns.get(pid);
  if (old) {
    refuse(old.ws, "elsewhere", "Signed in somewhere else.");
    drop(old, false);
  } else if (conns.size >= MAX_PLAYERS) {
    refuse(ws, "full", "The world is full just now: try again in a little while.");
    return null;
  }
  const token = randomBytes3(24).toString("base64url");
  doc.token = sha(token);
  doc.tokenAt = now;
  doc.seen = now;
  players.touch(pid);
  players.hold(pid, 1);
  const rec = doc.record;
  const at = rec && Array.isArray(rec.cell) ? rec.cell : doc.home;
  const c = {
    ws,
    ip,
    pid,
    doc,
    who: { id: pid, name: doc.name, brand: doc.brand },
    cell: cellKey(at[0], at[1]),
    pose: null,
    fleet: [],
    moved: false,
    poses: [],
    lastMove: now,
    look: null,
    subs: /* @__PURE__ */ new Set(),
    held: /* @__PURE__ */ new Set(),
    joined: now,
    chat: [],
    msgs: 0,
    msgsAt: now,
    watch: null
  };
  const owed = Math.round(doc.owed || 0);
  doc.owed = 0;
  if (doc.auto) (world.autos ??= {})[pid] = now;
  c.token = token;
  c.owedNow = owed;
  const crew = await crews.signedIn(c);
  const offer = doc.auto ? void 0 : await crews.offer(c, m.invite);
  send(ws, { ...welcomeOf(c), ...crew ? { crew } : {}, ...offer ? { offer } : {} });
  for (const x of conns.values()) send(x, { t: "on", who: c.who, c: c.cell });
  toGuests({ t: "on", who: c.who, c: c.cell });
  conns.set(pid, c);
  if (board.players[pid]) {
    board.players[pid].online = true;
  }
  console.log(`+ ${doc.name} (${conns.size} online)`);
  return c;
}
function welcomeOf(c) {
  const doc = c.doc;
  return {
    t: "welcome",
    you: c.who,
    world: { seed: world.seed, gen: world.gen, hours: world.hours },
    record: doc.record,
    home: doc.home,
    token: c.token ?? "",
    owed: c.owedNow ?? 0,
    ...doc.auto ? { auto: true } : {},
    online: [...conns.values()].filter((x) => x !== c).map((x) => ({ who: x.who, c: x.cell, look: x.look ?? void 0 })),
    community: community.welcome(doc),
    marks: marks.welcome(doc)
  };
}
function drop(c, tell = true) {
  if (conns.get(c.pid) !== c) return;
  conns.delete(c.pid);
  crews.signedOut(c);
  for (const k of c.subs) {
    rooms.get(k)?.subs.delete(c);
    if (c.held.delete(k)) cells.hold(`${world.epoch}:${k}`, -1);
    rehost(k);
  }
  c.subs.clear();
  c.doc.seen = Date.now();
  if (c.doc.auto && world.autos) world.autos[c.pid] = c.doc.seen;
  if (c.doc.seen - c.joined >= 6e5) step(c.doc, "card-stayed");
  players.touch(c.pid);
  players.hold(c.pid, -1);
  if (tell) {
    for (const x of conns.values()) send(x, { t: "off", id: c.pid });
    toGuests({ t: "off", id: c.pid });
  }
  console.log(`- ${c.doc.name} (${conns.size} online)`);
}
var num3 = (v) => typeof v === "number" && Number.isFinite(v) ? v : 0;
var poseOf = (p) => Array.isArray(p) && p.length >= 7 && p.every((v) => typeof v === "number" && Number.isFinite(v)) ? p.slice(0, 10) : null;
var POSES_KEPT = 3;
async function handle(c, m) {
  if (c.parted && m.t !== "bye") return;
  switch (m.t) {
    case "m": {
      const p = poseOf(m.p);
      if (!p) return;
      c.pose = p;
      c.poses.push(p);
      if (c.poses.length > POSES_KEPT) c.poses.shift();
      c.moved = true;
      const slept = !awake(c);
      c.lastMove = Date.now();
      if (slept) for (const k of c.subs) rehost(k);
      c.fleet = Array.isArray(m.f) ? m.f.filter((f) => Array.isArray(f) && f.length >= 8).slice(0, 16) : c.fleet;
      moved(c, cellKey(Math.round(p[0]), Math.round(p[1])));
      return;
    }
    case "sub":
      await subscribe(c, Array.isArray(m.cells) ? m.cells.map(String) : []);
      return;
    case "look": {
      if (!m.look || typeof m.look.m !== "string") return;
      c.look = { m: m.look.m.slice(0, 40), p: m.look.p, units: Array.isArray(m.look.units) ? m.look.units.slice(0, 24) : [] };
      for (const x of conns.values()) if (x !== c) send(x, { t: "look", id: c.pid, look: c.look });
      toGuests({ t: "look", id: c.pid, look: c.look });
      return;
    }
    case "brand": {
      const b = cleanBrand(m.brand);
      if (!b) return;
      if (crews.crewOf(c.pid)) {
        if (!crews.mayBrand(c.pid)) {
          send(c, { t: "deny", what: "brand", why: "Only the fleet\u2019s founder can change its name, colours or logo." });
          return;
        }
        crews.setBrand(c.pid, b);
        c.doc.brand = b;
        players.touch(c.pid);
        for (const x of conns.values()) if (x !== c) send(x, { t: "on", who: c.who, c: c.cell, look: c.look ?? void 0 });
        return;
      }
      c.doc.brand = b;
      c.who = { ...c.who, brand: b };
      world.people[c.pid] = { name: c.doc.name, brand: b };
      worldDirty = true;
      players.touch(c.pid);
      for (const x of conns.values()) if (x !== c) send(x, { t: "on", who: c.who, c: c.cell, look: c.look ?? void 0 });
      return;
    }
    case "edit": {
      const k = String(m.c);
      const e = m.e;
      if (!c.subs.has(k) || !e || typeof e !== "object" || !EDIT_KINDS.has(String(e.k)) || JSON.stringify(e).length > 16e3) return;
      const doc = cells.peek(`${world.epoch}:${k}`);
      if (!doc) return;
      if (e.k === "raze") {
        const { x, z } = e;
        const named = typeof m.lot === "string" ? doc.deeds?.[m.lot] : void 0;
        const d = named && !crews.same(named.by, c.pid) ? named : Object.values(doc.deeds ?? {}).find((row) => !crews.same(row.by, c.pid) && (row.rects ?? []).some(([rx, rz, rw, rd]) => typeof x === "number" && typeof z === "number" && x >= rx && z >= rz && x < rx + rw && z < rz + rd));
        if (d) {
          send(c, { t: "deny", what: "raze", c: k, why: `That belongs to ${whoOf(d.by)?.brand?.name ?? whoOf(d.by)?.name ?? "another fleet"}.` });
          return;
        }
        const listed = typeof x === "number" && typeof z === "number" ? community.razeBarred(doc, x, z, c.pid) : null;
        if (listed) {
          send(c, { t: "deny", what: "raze", c: k, why: listed });
          return;
        }
      }
      if (doc.edits.length >= 3e4) {
        send(c, { t: "deny", what: "edit", c: k, why: "This county has been changed as much as it can be." });
        return;
      }
      const kept = { ...e, by: c.pid, at: Math.round(world.hours * 10) / 10 };
      doc.edits.push(kept);
      cells.touch(`${world.epoch}:${k}`);
      toRoom(k, { t: "edit", c: k, e: kept }, c);
      community.edited(c, k, doc, kept);
      return;
    }
    case "land": {
      const k = String(m.c);
      const doc = c.subs.has(k) ? cells.peek(`${world.epoch}:${k}`) : null;
      if (!doc || !m.chunks || typeof m.chunks !== "object") return;
      const out = {};
      const was = { ...doc.land ?? {} };
      for (const [ck, v] of Object.entries(m.chunks)) {
        const n = Number(ck);
        if (!(n >= 0 && n < 256)) continue;
        if (typeof v === "string" && v.length < 8e3) {
          (doc.land ??= {})[String(n)] = v;
          out[String(n)] = v;
        } else if (v === null) {
          if (doc.land) delete doc.land[String(n)];
          out[String(n)] = null;
        }
      }
      if (doc.land && !Object.keys(doc.land).length) delete doc.land;
      cells.touch(`${world.epoch}:${k}`);
      toRoom(k, { t: "land", c: k, chunks: out }, c);
      community.landed(c, k, doc, was, out);
      return;
    }
    case "state": {
      const k = String(m.c);
      const r = rooms.get(k);
      const doc = cells.peek(`${world.epoch}:${k}`);
      if (!r || r.host !== c || !doc) return;
      const own = (key) => key.startsWith(`${k}#`) && key.length < 40;
      const pass = { t: "state", c: k };
      if (m.towns) {
        for (const [key, v] of Object.entries(m.towns)) if (own(key) && v && typeof v === "object") {
          (doc.towns ??= {})[key] = v;
          (pass.towns ??= {})[key] = v;
        }
      }
      if (m.lots) {
        for (const [key, v] of Object.entries(m.lots)) if (own(key) && Array.isArray(v) && v.length <= 8) {
          (doc.lots ??= {})[key] = v;
          (pass.lots ??= {})[key] = v;
        }
      }
      if (m.carriage) for (const [key, v] of Object.entries(m.carriage)) {
        if (!own(key) || !Array.isArray(v) || v.length !== 4) continue;
        await marks.carriage(k, doc, key, v);
        (doc.carriage ??= {})[key] = v;
        (pass.carriage ??= {})[key] = v;
      }
      if (m.firms) for (const [id, v] of Object.entries(m.firms)) {
        if (!id.startsWith(`${k}/`) || id.length > 40) continue;
        if (v === null) {
          if (doc.firms) delete doc.firms[id];
        } else if (typeof v === "object" && JSON.stringify(v).length < 4e4) (doc.firms ??= {})[id] = v;
      }
      cells.touch(`${world.epoch}:${k}`);
      if (pass.towns || pass.lots || pass.carriage) toRoom(k, pass, c);
      return;
    }
    case "credit": {
      const t = TOWN2.exec(String(m.town));
      const host = t ? rooms.get(t[1])?.host : null;
      const amt = num3(m.amt);
      if (!host || host === c || !(amt > 0) || amt > 1e6) return;
      if (m.rent === true) {
        send(host, { t: "credit", town: m.town, by: c.pid, amt, rent: true });
        return;
      }
      send(host, { t: "credit", town: m.town, by: c.pid, amt, lot: typeof m.lot === "string" && LOT3.test(m.lot) ? m.lot : void 0, lotAmt: num3(m.lotAmt) || void 0, dir: m.dir === "out" ? "out" : "in" });
      return;
    }
    case "deeds": {
      for (const [key, v] of Object.entries(m.set ?? {})) {
        const lot = LOT3.exec(key);
        if (!lot || !v) continue;
        const dk = `${world.epoch}:${lot[1]}`;
        const doc = await cells.open(dk);
        const have = doc.deeds?.[key];
        if (have && !crews.same(have.by, c.pid)) {
          const w = whoOf(have.by);
          if (w) send(c, { t: "deedDeny", key, who: w });
          continue;
        }
        const row = { by: have?.by ?? c.pid, name: String(v.name ?? "").replace(/[<>&"`\u0000-\u001f]/g, "").slice(0, 60), ceil: Math.max(0, Math.min(1e7, num3(v.ceil))) };
        if (v.hq !== void 0) row.hq = Math.max(0, Math.min(9, Math.round(num3(v.hq))));
        const rects = Array.isArray(v.rects) ? v.rects.filter((r) => Array.isArray(r) && r.length === 4 && r.every((n) => Number.isInteger(n) && n >= 0 && n <= 256) && r[2] <= 64 && r[3] <= 64).slice(0, 24) : have?.rects;
        if (rects?.length) row.rects = rects;
        (doc.deeds ??= {})[key] = row;
        cells.touch(dk);
        toRoom(lot[1], { t: "deed", key, row, who: c.who }, c);
        if (row.hq !== void 0) {
          c.doc.hq = { key, name: row.name, epoch: world.epoch };
          players.touch(c.pid);
          (world.hqs ??= {})[c.pid] = lot[1];
          worldDirty = true;
        } else if (c.doc.hq?.key === key) {
          delete c.doc.hq;
          players.touch(c.pid);
          if (world.hqs?.[c.pid]) {
            delete world.hqs[c.pid];
            worldDirty = true;
          }
        }
      }
      for (const key of m.del ?? []) {
        const lot = LOT3.exec(String(key));
        if (!lot) continue;
        const dk = `${world.epoch}:${lot[1]}`;
        const doc = await cells.open(dk);
        if (!doc.deeds?.[key] || !crews.same(doc.deeds[key].by, c.pid)) continue;
        delete doc.deeds[key];
        cells.touch(dk);
        toRoom(lot[1], { t: "deed", key, row: null }, c);
        if (c.doc.hq?.key === key) {
          delete c.doc.hq;
          players.touch(c.pid);
          if (world.hqs?.[c.pid]) {
            delete world.hqs[c.pid];
            worldDirty = true;
          }
        }
      }
      return;
    }
    case "rent": {
      const day = Math.floor(world.hours / 24);
      for (const [key, v] of Object.entries(m.k ?? {})) {
        const lot = LOT3.exec(key);
        if (!lot) continue;
        const doc = cells.peek(`${world.epoch}:${lot[1]}`);
        const row = doc?.deeds?.[key];
        if (!row || crews.same(row.by, c.pid)) continue;
        const t = rentToday.get(key);
        const taken = t && t[0] === day ? t[1] : 0;
        const amt = Math.floor(Math.min(num3(v), Math.max(0, row.ceil - taken)));
        if (!(amt > 0)) continue;
        rentToday.set(key, [day, taken + amt]);
        const owner2 = conns.get(row.by);
        await marks.rent(c.pid, row.by, amt, !!owner2);
        if (owner2) send(owner2, { t: "rent", key, amt, from: c.pid });
        else {
          const od = await players.open(row.by);
          if (od.id) {
            od.owed = (od.owed || 0) + amt;
            players.touch(row.by);
          }
        }
      }
      return;
    }
    case "record": {
      if (m.record && typeof m.record === "object") {
        c.doc.record = m.record;
        players.touch(c.pid);
        if (Number(m.record.delivered) > 0) step(c.doc, "card-delivered");
      }
      const me = m.board?.me;
      if (me) {
        board.players[c.pid] = {
          id: c.pid,
          name: c.doc.brand?.name ?? c.doc.name,
          by: c.doc.name,
          cpu: false,
          worth: Math.round(num3(me.worth)),
          units: Math.round(num3(me.units)),
          towns: Math.round(num3(me.towns)),
          primary: c.doc.brand?.primary ?? 14827823,
          accent: c.doc.brand?.accent ?? 16777215,
          logo: c.doc.brand?.logo ?? "",
          v: metricValues(me.v),
          ...community.boardBits(c.doc)
        };
        boardDirty = true;
        marks.worth(c.pid, Math.round(num3(me.worth)));
      }
      for (const row of m.board?.cpu ?? []) {
        const home = String(row?.id ?? "").split("/")[0];
        if (!CELL2.test(home) || rooms.get(home)?.host !== c) continue;
        board.cpu[String(row.id).slice(0, 40)] = {
          id: String(row.id).slice(0, 40),
          name: String(row.name ?? "").slice(0, 40),
          cpu: true,
          worth: Math.round(num3(row.worth)),
          units: Math.round(num3(row.units)),
          towns: Math.round(num3(row.towns)),
          primary: num3(row.primary) & 16777215,
          accent: num3(row.accent) & 16777215,
          logo: [...String(row.logo ?? "")].slice(0, 2).join(""),
          v: metricValues(row.v)
        };
        boardDirty = true;
      }
      send(c, { t: "saved" });
      return;
    }
    case "reroll": {
      if (c.doc.record || (c.doc.rerolls ?? 0) >= 16) return;
      c.doc.rerolls = (c.doc.rerolls ?? 0) + 1;
      const was = cellKey(c.doc.home[0], c.doc.home[1]);
      if (world.homes[was] === c.pid) delete world.homes[was];
      world.homes[was] = "none";
      c.doc.home = pickHome();
      world.homes[cellKey(c.doc.home[0], c.doc.home[1])] = c.pid;
      worldDirty = true;
      players.touch(c.pid);
      send(c, { t: "home", home: c.doc.home });
      return;
    }
    case "chat":
      chat(c, String(m.text ?? ""));
      return;
    case "keep": {
      const nm = String(m.name ?? "").trim(), pin = String(m.pin ?? "");
      const no = (why) => send(c, { t: "kept", you: null, why });
      if (!c.doc.auto) {
        no("This fleet already has a PIN.");
        return;
      }
      if (!NAME.test(nm)) {
        no("A name is 3 to 16 letters, digits, - or _.");
        return;
      }
      const lower = nm.toLowerCase();
      if (world.names[lower] && world.names[lower] !== c.pid) {
        no(`Somebody is already called ${nm}: pick another.`);
        return;
      }
      if (pin.length < 4 || pin.length > 32) {
        no("A PIN is 4 to 32 characters.");
        return;
      }
      delete world.names[c.doc.name.toLowerCase()];
      world.names[lower] = c.pid;
      c.doc.salt = randomBytes3(12).toString("hex");
      c.doc.hash = await pinHash(pin, c.doc.salt);
      c.doc.name = nm;
      delete c.doc.auto;
      if (world.autos) delete world.autos[c.pid];
      c.who = { ...c.who, name: nm };
      world.people[c.pid] = { name: nm, brand: c.doc.brand };
      if (board.players[c.pid]) {
        board.players[c.pid].by = nm;
        boardDirty = true;
      }
      worldDirty = true;
      players.touch(c.pid);
      step(c.doc, "card-kept");
      send(c, { t: "kept", you: c.who });
      for (const x of conns.values()) if (x !== c) send(x, { t: "on", who: c.who, c: c.cell });
      return;
    }
    case "friend": {
      const id = String(m.id ?? "");
      const list = c.doc.friends ??= [];
      if (m.on) {
        if (id === c.pid || !world.people[id] || list.includes(id) || list.length >= FRIENDS_MAX) {
          send(c, { t: "friends", rows: await friendRows(c) });
          return;
        }
        list.push(id);
        const to = conns.get(id);
        if (to) send(to, { t: "chat", kind: "sys", text: `${c.doc.name} added you as a friend${to.doc.friends?.includes(c.pid) ? "" : " \xB7 right-click their name to add them back"}` });
      } else c.doc.friends = list.filter((x) => x !== id);
      players.touch(c.pid);
      send(c, { t: "friends", rows: await friendRows(c) });
      return;
    }
    case "friends":
      send(c, { t: "friends", rows: await friendRows(c) });
      return;
    case "watch":
      c.watch = typeof m.c === "string" && CELL2.test(m.c) ? m.c : null;
      return;
    case "board":
      send(c, { t: "board", rows: boardRows() });
      return;
    case "bye":
      c.ws.close();
      return;
    case "join": {
      if (c.doc.auto) {
        send(c, { t: "joined", welcome: null, why: "Keep your fleet first (pause \u2192 Keep this fleet): then you can join another." });
        return;
      }
      const r = await crews.join(c, String(m.code ?? ""));
      if ("why" in r) {
        send(c, { t: "joined", welcome: null, why: r.why });
        return;
      }
      const crew = await crews.signedIn(c);
      send(c, { t: "joined", welcome: { ...welcomeOf(c), ...crew ? { crew } : {}, ...r.spawn ? { spawn: r.spawn } : {} } });
      return;
    }
    case "invite":
    case "purse":
    case "roster":
    case "boot":
    case "leaveCrew":
      await crews.handle(c, m);
      return;
    default:
      if (c.doc.auto && (m.t === "sign" || m.t === "suggest" || m.t === "name")) {
        send(c, { t: "chat", kind: "sys", text: "Keep your fleet first (pause \u2192 Keep this fleet): then you can put up signs and name things." });
        return;
      }
      if (!await community.handle(c, m) && !await postcards.handle(c, m)) await marks.handle(c, m);
      return;
  }
}
function metricValues(v) {
  if (!v || typeof v !== "object") return void 0;
  const out = {};
  for (const [k, n] of Object.entries(v).slice(0, 24)) {
    if (/^[a-zA-Z]{1,16}$/.test(k) && typeof n === "number" && Number.isFinite(n)) out[k] = Math.round(n * 100) / 100;
  }
  return out;
}
async function friendRows(c) {
  const rows = [];
  for (const id of c.doc.friends ?? []) {
    const p = world.people[id];
    if (!p) continue;
    const on = conns.get(id);
    const doc = on?.doc ?? players.peek(id) ?? await players.open(id).catch(() => null);
    const hq = doc?.hq && doc.hq.epoch === world.epoch ? { key: doc.hq.key, name: doc.hq.name } : void 0;
    rows.push({ id, name: p.name, brand: on?.who.brand ?? p.brand, online: !!on, c: on?.cell, hq, mutual: !!doc?.friends?.includes(c.pid) });
  }
  return rows.sort((a, b) => Number(b.online) - Number(a.online) || a.name.localeCompare(b.name));
}
function boardRows() {
  const rows = [
    ...Object.values(board.players).map((r) => ({ ...r, online: conns.has(r.id) })),
    ...Object.values(board.cpu)
  ];
  return rows.sort((a, b) => b.worth - a.worth).slice(0, 60);
}
function chat(c, raw) {
  const text = raw.replace(/[\u0000-\u001f]/g, " ").trim().slice(0, CHAT_MAX);
  if (!text) return;
  const now = Date.now();
  c.chat = c.chat.filter((t) => now - t < 6e3);
  if (c.chat.length >= 5) {
    send(c, { t: "chat", kind: "sys", text: "Slow down a little." });
    return;
  }
  c.chat.push(now);
  const w = /^\/(?:w|whisper|tell|msg)\s+(\S+)\s+([\s\S]+)$/i.exec(text);
  if (w && c.doc.auto) {
    send(c, { t: "chat", kind: "sys", text: "Keep your fleet first (pause \u2192 Keep this fleet): then you can whisper." });
    return;
  }
  if (w) {
    const to = [...conns.values()].find((x) => x.doc.name.toLowerCase() === w[1].toLowerCase());
    if (!to) {
      send(c, { t: "chat", kind: "sys", text: `${w[1]} isn\u2019t signed in.` });
      return;
    }
    send(to, { t: "chat", kind: "w", from: c.pid, name: c.doc.name, text: w[2] });
    send(c, { t: "chat", kind: "wto", from: c.pid, name: c.doc.name, to: to.doc.name, text: w[2] });
    return;
  }
  if (/^\/who\b/i.test(text)) {
    const list = [...conns.values()].map((x) => `${x.doc.name}${x === c ? " (you)" : ` \xB7 ${cheb(c.cell, x.cell)} ${cheb(c.cell, x.cell) === 1 ? "county" : "counties"} away`}`);
    send(c, { t: "chat", kind: "sys", text: `${conns.size} signed in: ${list.join(", ")}` });
    return;
  }
  if (text.startsWith("/")) {
    send(c, { t: "chat", kind: "sys", text: "Commands: /w name message \xB7 /who" });
    return;
  }
  let heard = 0;
  const reach = c.doc.auto ? 0 : CHAT_CELLS;
  for (const x of conns.values()) {
    if (cheb(c.cell, x.cell) > reach) continue;
    send(x, { t: "chat", kind: "say", from: c.pid, name: c.doc.name, text });
    if (x !== c) heard++;
  }
  if (!heard && conns.size > 1) send(c, { t: "chat", kind: "sys", text: `Nobody is within ${CHAT_CELLS} counties to hear that: /w name message reaches anyone signed in.` });
}
function beat() {
  const movers = [...conns.values()].filter((c) => c.moved && c.pose);
  if (!movers.length) return;
  for (const to of conns.values()) {
    const a = [], f = [];
    for (const m of movers) {
      if (m === to || cheb(m.cell, to.cell) > SEE_CELLS && !(to.watch && cheb(m.cell, to.watch) <= SEE_CELLS)) continue;
      for (const p of m.poses.length ? m.poses : [m.pose]) a.push([m.pid, ...p]);
      for (const u of m.fleet) f.push([m.pid, ...u]);
    }
    if (a.length) send(to, { t: "mv", a, f });
  }
  for (const g of guests) {
    if (!g.at) continue;
    const a = [], f = [];
    for (const m of movers) {
      if (cheb(m.cell, g.at) > SEE_CELLS) continue;
      for (const p of m.poses.length ? m.poses : [m.pose]) a.push([m.pid, ...p]);
      for (const u of m.fleet) f.push([m.pid, ...u]);
    }
    if (a.length) send(g.ws, { t: "mv", a, f });
  }
  for (const m of movers) {
    m.moved = false;
    m.poses = [];
  }
}
var lastClock = Date.now();
function clock() {
  const now = Date.now();
  const dt = Math.min(5, (now - lastClock) / 1e3);
  lastClock = now;
  if (!conns.size) return;
  world.hours += dt * HOURS_PER_SECOND;
  worldDirty = true;
}
function tick() {
  const now = Date.now();
  for (const g of [...guests]) if (now - g.last > GUEST.idle) {
    g.ws.close();
    dropGuest(g);
  }
  if (!conns.size) {
    if (guests.size) toGuests({ t: "tick", hours: world.hours, at: {} });
    return;
  }
  for (const [k, r] of rooms) if (r.host && !awake(r.host)) rehost(k);
  const at = {};
  for (const c of conns.values()) at[c.pid] = c.cell;
  const text = JSON.stringify({ t: "tick", hours: world.hours, at });
  for (const c of conns.values()) if (c.ws.readyState === 1) c.ws.send(text);
  for (const g of guests) if (g.ws.readyState === 1) g.ws.send(text);
  community.beat();
  marks.beat();
}
var flushing = false;
async function flush(evict = true) {
  if (flushing) return;
  flushing = true;
  try {
    sweepAutos();
    if (worldDirty) {
      worldDirty = false;
      await store.set("world", world);
    }
    for (const [day, n] of [...funnel]) {
      funnel.delete(day);
      const doc = await store.get(`visits:${day}`) ?? { day, n: {}, rows: [] };
      for (const [k, v] of Object.entries(n)) doc.n[k] = (doc.n[k] ?? 0) + v;
      await store.set(`visits:${day}`, doc);
    }
    if (boardDirty) {
      boardDirty = false;
      await store.set("board", board);
    }
    await community.flush();
    await marks.flush();
    await crewDocs.flush(evict);
    await cards.flush(evict);
    await pics.flush(evict);
    await players.flush(evict);
    await cells.flush(evict);
  } catch (e) {
    console.error("flush failed:", e.message);
  } finally {
    flushing = false;
  }
}
var sweptAt = 0;
function sweepAutos() {
  const now = Date.now();
  if (now - sweptAt < 36e5 || !world.autos) return;
  sweptAt = now;
  for (const [pid, seen] of Object.entries(world.autos)) {
    if (conns.has(pid) || now - seen < AUTO_KEEP_DAYS * 864e5) continue;
    const name = world.people[pid]?.name;
    if (name && world.names[name.toLowerCase()] === pid) delete world.names[name.toLowerCase()];
    for (const [k, v] of Object.entries(world.homes)) if (v === pid) delete world.homes[k];
    delete world.autos[pid];
    if (board.players[pid]) {
      delete board.players[pid];
      boardDirty = true;
    }
    worldDirty = true;
    console.log(`let go: ${name ?? pid}, a one-click fleet never kept`);
  }
}
var TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
  ".wav": "audio/wav",
  ".webm": "video/webm",
  ".mp4": "video/mp4",
  ".wasm": "application/wasm",
  ".txt": "text/plain; charset=utf-8"
};
var SITE_FILES = /^\/(index\.html|version\.json|favicon\.svg|media\/[\w.-]+\.(mp4|webm|jpg|png)|downloads\/[\w.-]+\.zip)$/;
function sendFile(req, res, file, head) {
  let size;
  try {
    const st = statSync(file);
    if (!st.isFile()) throw new Error("not a file");
    size = st.size;
  } catch {
    res.writeHead(404, { "content-type": "text/plain" }).end("Not found");
    return;
  }
  const range = /^bytes=(\d*)-(\d*)$/.exec(String(req.headers.range ?? ""));
  if (range && (range[1] || range[2])) {
    let from = range[1] ? Number(range[1]) : size - Number(range[2]);
    let to = range[1] && range[2] ? Number(range[2]) : size - 1;
    from = Math.max(0, from);
    to = Math.min(size - 1, to);
    if (from > to || from >= size) {
      res.writeHead(416, { "content-range": `bytes */${size}` }).end();
      return;
    }
    res.writeHead(206, { ...head, "accept-ranges": "bytes", "content-range": `bytes ${from}-${to}/${size}`, "content-length": to - from + 1 });
    if (req.method === "HEAD") {
      res.end();
      return;
    }
    createReadStream(file, { start: from, end: to }).pipe(res);
    return;
  }
  res.writeHead(200, { ...head, "accept-ranges": "bytes", "content-length": size });
  if (req.method === "HEAD") {
    res.end();
    return;
  }
  createReadStream(file).pipe(res);
}
function ipOf2(req) {
  const fwd = String(req.headers["x-forwarded-for"] ?? "").split(",").map((h) => h.trim()).filter(Boolean);
  return String(req.headers["x-real-ip"] ?? "").trim() || fwd[fwd.length - 1] || String(req.socket.remoteAddress ?? "");
}
function http(req, res) {
  let path;
  try {
    path = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname);
  } catch {
    res.writeHead(400).end("Bad request");
    return;
  }
  if (path === "/healthz" || path === "/health") {
    res.writeHead(200, { "content-type": "text/plain" }).end("ok");
    return;
  }
  if (path.startsWith("/join/") || path.startsWith("/api/invite/")) {
    crews.http(req, res, path, SITE ? "/play/" : "/").then((done) => {
      if (!done && !res.headersSent) res.writeHead(404, { "content-type": "text/plain" }).end("Not found");
    }).catch((e) => {
      console.error("crews:", e.message);
      if (!res.headersSent) res.writeHead(500).end();
    });
    return;
  }
  if (path.startsWith("/p/") || path.startsWith("/api/postcard/")) {
    postcards.http(req, res, path, ipOf2(req), SITE ? "/play/" : "/").then((done) => {
      if (!done && !res.headersSent) res.writeHead(404, { "content-type": "text/plain" }).end("Not found");
    }).catch((e) => {
      console.error("postcards:", e.message);
      if (!res.headersSent) res.writeHead(500).end();
    });
    return;
  }
  if (path.startsWith("/api/") || path === ADMIN_PATH || path === `${ADMIN_PATH}/` || path === `${ADMIN_PATH}/missions`) {
    adminHttp(req, res, path, store, {
      clearName: (k) => community.clearName(k),
      // postcards' pictures (docs/postcards.html): the list, and taking one down or putting it back
      pictures: async () => postcards.withPictures((await store.list("postcard:")).map((e) => cards.peek(e.key.slice("postcard:".length)) ?? e.value)),
      picture: (id, show) => postcards.setPicture(id, show)
    }).then((done) => {
      if (!done && !res.headersSent) res.writeHead(404, { "content-type": "text/plain" }).end("Not found");
    }).catch((e) => {
      console.error("admin:", e.message);
      if (!res.headersSent) res.writeHead(500).end();
    });
    return;
  }
  if (path === "/online.json" || path === "/play/online.json") {
    res.writeHead(200, { "content-type": "application/json", "access-control-allow-origin": "*", "cache-control": "no-store" }).end(JSON.stringify({ tinyfleet: true, protocol: PROTOCOL, gen: world.gen, seed: world.seed, start: upcomingHome(), picks: marks.picksNow(), players: conns.size, max: MAX_PLAYERS, guests: guests.size, day: Math.floor(world.hours / 24) + 1 }));
    return;
  }
  if (SITE) {
    if (path === "/play") {
      res.writeHead(308, { location: "/play/" }).end();
      return;
    }
    if (!path.startsWith("/play/")) {
      if (path === "/") path = "/index.html";
      const file2 = normalize(join2(SITE, path));
      if (!SITE_FILES.test(path) || !file2.startsWith(SITE)) {
        res.writeHead(404, { "content-type": "text/plain" }).end("Not found");
        return;
      }
      const ext = extname(file2);
      const head = { "content-type": ext === ".zip" ? "application/zip" : TYPES[ext] ?? "application/octet-stream", "cache-control": path.startsWith("/media/") ? "public, max-age=86400" : "no-cache" };
      if (ext === ".zip") head["content-disposition"] = `attachment; filename="${path.split("/").pop()}"`;
      sendFile(req, res, file2, head);
      return;
    }
    path = path.slice("/play".length);
  }
  if (path === "/") path = "/index.html";
  const file = normalize(join2(PUBLIC, path));
  if (!file.startsWith(PUBLIC)) {
    res.writeHead(404).end("Not found");
    return;
  }
  const hashed = /\/assets\//.test(path);
  sendFile(req, res, file, { "content-type": TYPES[extname(file)] ?? "application/octet-stream", "cache-control": hashed ? "public, max-age=31536000, immutable" : "no-cache" });
}
async function main() {
  store = await openStore();
  const had = await store.get("world");
  world = had ?? { seed: SEED, gen: GEN_VERSION, epoch: 1, hours: 8, nextId: 1, names: {}, homes: {}, people: {} };
  world.people ??= {};
  if (world.seed !== SEED || world.gen !== GEN_VERSION) {
    console.log(`the world begins again: ${world.seed} gen ${world.gen} \u2192 ${SEED} gen ${GEN_VERSION}`);
    world = { ...world, seed: SEED, gen: GEN_VERSION, epoch: world.epoch + 1, homes: {}, hqs: {}, hours: 8 };
    board = { players: board.players, cpu: {} };
    worldDirty = boardDirty = true;
  }
  if (!had) worldDirty = true;
  board = await store.get("board") ?? board;
  if (worldDirty && had) board.cpu = {};
  setWorldMode(parseSeed(world.seed).mode);
  seedHash = hashString(world.seed);
  players = new Docs(store, "player:", () => ({}));
  cells = new Docs(store, "cell:", freshCell);
  community = new Community({
    hours: () => world.hours,
    epoch: () => world.epoch,
    conns: () => conns.values(),
    conn: (pid) => conns.get(pid),
    cells,
    players,
    store,
    whoOf,
    cheb,
    send: (c, m) => send(c, m),
    toRoom: (c, m) => toRoom(c, m)
  });
  await community.load();
  marks = new Marks({
    hours: () => world.hours,
    epoch: () => world.epoch,
    conns: () => conns.values(),
    conn: (pid) => conns.get(pid),
    cells,
    players,
    store,
    whoOf,
    cheb,
    send: (c, m) => send(c, m),
    toRoom: (c, m) => toRoom(c, m),
    worths: () => Object.fromEntries(Object.values(board.players).map((r) => [r.id, r.worth]))
  });
  community.onHeart = (line, on) => marks.heart(line, on);
  community.onRoad = (line) => marks.road(line);
  community.tell = (pid, n, from) => void marks.news(pid, n, from);
  marks.onLike = (pid, n) => void community.likes(pid, n);
  await marks.load();
  crewDocs = new Docs(store, "crew:", () => ({}));
  crews = new Crews({
    epoch: () => world.epoch,
    docs: crewDocs,
    players,
    cells,
    conn: (pid) => conns.get(pid),
    conns: () => conns.values(),
    send: (c, m) => send(c, m),
    toRoom: (c, m) => toRoom(c, m),
    people: () => world.people,
    index: () => world.crews ??= {},
    invites: () => world.invites ??= {},
    nextId: () => {
      world.nextCrew = (world.nextCrew ?? 0) + 1;
      worldDirty = true;
      return world.nextCrew;
    },
    worldChanged: () => {
      worldDirty = true;
    },
    retag: (pid) => {
      const c = conns.get(pid);
      if (!c) return;
      for (const x of conns.values()) if (x !== c) send(x, { t: "on", who: c.who, c: c.cell, look: c.look ?? void 0 });
      toGuests({ t: "on", who: c.who, c: c.cell, look: c.look ?? void 0 });
    }
  });
  cards = new Docs(store, "postcard:", () => null);
  pics = new Docs(store, "postcardpic:", () => null);
  postcards = new Postcards({
    epoch: () => world.epoch,
    cards,
    pics,
    touch: (pid) => players.touch(pid),
    send: (c, m) => send(c, m),
    whoOf,
    count,
    news: (pid, n) => void marks.news(pid, n),
    likes: (pid, n) => void community.likes(pid, n),
    week: (id, by, title, x, z, open) => marks.card(id, by, title, x, z, open)
  });
  const server = createServer(http);
  const wss = new import_websocket_server.default({ server, maxPayload: 6 * 1024 * 1024 });
  wss.on("connection", (ws, req) => {
    const ip = ipOf2(req);
    let conn = null;
    let guest = null;
    let busy = Promise.resolve();
    let alive = true;
    ws.on("pong", () => {
      alive = true;
    });
    const beatT = setInterval(() => {
      if (!alive) {
        ws.terminate();
        return;
      }
      alive = false;
      try {
        ws.ping();
      } catch {
      }
    }, 25e3);
    ws.on("message", (data) => {
      let m;
      try {
        m = JSON.parse(String(data));
      } catch {
        ws.close();
        return;
      }
      if (!m || typeof m !== "object" || typeof m.t !== "string") {
        ws.close();
        return;
      }
      const counted = conn ?? guest;
      if (counted) {
        const now = Date.now();
        if (now - counted.msgsAt > 1e3) {
          counted.msgsAt = now;
          counted.msgs = 0;
        }
        if (++counted.msgs > (guest ? 40 : 400)) {
          ws.close();
          return;
        }
      }
      busy = busy.then(async () => {
        if (guest) {
          if (guests.has(guest)) await guestHandle(guest, m);
          return;
        }
        if (!conn) {
          if (m.t === "hello") {
            if (m.guest) guest = guestHello(ws, ip, m);
            else conn = await hello(ws, ip, m);
          }
          return;
        }
        if (conns.get(conn.pid) !== conn) return;
        await handle(conn, m);
      }).catch((e) => console.error("message failed:", e.stack ?? e));
    });
    ws.on("close", () => {
      clearInterval(beatT);
      if (conn) drop(conn);
      if (guest) dropGuest(guest);
    });
    ws.on("error", () => {
    });
  });
  setInterval(beat, 1e3 / MOVE_HZ);
  setInterval(clock, 1e3);
  setInterval(tick, 5e3);
  setInterval(() => void flush(), 5e3);
  const stop = async () => {
    console.log("stopping: writing everything down");
    for (const c of [...conns.values()]) {
      send(c, { t: "chat", kind: "sys", text: "The server is restarting: back in a moment." });
      c.ws.close();
    }
    await new Promise((r) => setTimeout(r, 300));
    await flush(false);
    await flush(false);
    await store.close();
    process.exit(0);
  };
  process.on("SIGTERM", () => void stop());
  process.on("SIGINT", () => void stop());
  server.listen(PORT, () => console.log(`TinyFleet Online on :${PORT} \xB7 world "${world.seed}" gen ${world.gen} \xB7 day ${Math.floor(world.hours / 24) + 1} \xB7 ${Object.keys(world.names).length} fleets \xB7 ${store.kind} \xB7 game from ${PUBLIC}${SITE ? " at /play/, beside the download page" : ""}`));
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
