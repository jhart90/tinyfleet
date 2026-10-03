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
                const num2 = +value;
                if (!Number.isInteger(num2) || num2 < 8 || num2 > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num2;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num2 = +value;
              if (!Number.isInteger(num2) || num2 < 8 || num2 > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num2;
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
        const num2 = buf.readUInt32BE(0);
        if (num2 > Math.pow(2, 53 - 32) - 1) {
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
        this._payloadLength = num2 * Math.pow(2, 32) + buf.readUInt32BE(4);
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
    var { randomBytes: randomBytes3, createHash: createHash3 } = __require("crypto");
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
      const key = randomBytes3(16).toString("base64");
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
    function is0To99(num2) {
      return num2 >= 0 && num2 < 100;
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
    var DAY = NUMBER + "\\s+days?";
    var TIME = "([+-])?([\\d]*):(\\d\\d):(\\d\\d)\\.?(\\d{1,6})?";
    var INTERVAL = new RegExp([YEAR, MONTH, DAY, TIME].map(function(regexString) {
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
          var count = dimension.shift();
          for (i2 = 0; i2 < count; i2++) {
            array[i2] = parse(dimension, elementType2);
          }
          dimension.unshift(count);
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
      randomBytes: randomBytes3,
      deriveKey,
      sha256,
      hashByName,
      hmacSha256,
      md5
    };
    var webCrypto = nodeCrypto.webcrypto || globalThis.crypto;
    var subtleCrypto = webCrypto.subtle;
    var textEncoder = new TextEncoder();
    function randomBytes3(length) {
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
      addInt32(num2) {
        this.ensure(4);
        this.buffer[this.offset++] = num2 >>> 24 & 255;
        this.buffer[this.offset++] = num2 >>> 16 & 255;
        this.buffer[this.offset++] = num2 >>> 8 & 255;
        this.buffer[this.offset++] = num2 >>> 0 & 255;
        return this;
      }
      addInt16(num2) {
        this.ensure(2);
        this.buffer[this.offset++] = num2 >>> 8 & 255;
        this.buffer[this.offset++] = num2 >>> 0 & 255;
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
import { createHash as createHash2, randomBytes as randomBytes2, scrypt, timingSafeEqual as timingSafeEqual2 } from "node:crypto";
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
    <div class="tabs"><button id="t-games" class="on">Games</button><button id="t-fb">Feedback</button></div>
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
function cellTxt(c) { return c && c.length === 2 ? c[0] + ', ' + c[1] : '—'; }
function gameHour(h) { if (h == null) return '—'; var d = Math.floor(h / 24) + 1, hh = Math.floor(h % 24), mm = Math.round((h % 1) * 60); return 'day ' + d + ' ' + String(hh).padStart(2, '0') + ':' + String(mm).padStart(2, '0'); }

async function api(path, body) {
  var r = await fetch('/api/admin/' + path, body ? { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {});
  var j = {}; try { j = await r.json(); } catch (e) {}
  j.status = r.status;
  return j;
}

var games = [], feedback = [], sortKey = 'last', sortDir = -1, openGid = null;
var COLS = [
  ['fleet', 'Fleet'], ['player', 'Player'], ['seed', 'Seed'], ['start', 'Start cell'], ['played', 'Played', 1], ['day', 'Game day', 1],
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
function tab(t) {
  $('t-games').classList.toggle('on', t === 'games'); $('t-fb').classList.toggle('on', t === 'fb');
  $('games').hidden = t !== 'games'; $('feedback').hidden = t !== 'fb';
}

async function load() {
  var a = await api('games'), b = await api('feedback');
  if (a.status === 401 || b.status === 401) { showApp(false); return; }
  games = a.games || []; feedback = b.feedback || [];
  $('t-fb').textContent = 'Feedback (' + feedback.length + ')';
  $('t-games').textContent = 'Games (' + games.length + ')';
  renderGames(); renderFeedback();
  if (openGid) openGame(openGid);
}

function matches(text) { var q = $('search').value.trim().toLowerCase(); return !q || text.toLowerCase().indexOf(q) >= 0; }

function renderGames() {
  var list = games.filter(function (g) { return matches([g.fleet, g.player, g.seed, g.online, g.gid].join(' ')); });
  list.sort(function (a, b) { var x = a[sortKey], y = b[sortKey]; if (x == null) return 1; if (y == null) return -1; return (typeof x === 'string' ? x.localeCompare(y) : x - y) * sortDir; });
  var tot = { play: 0, earned: 0, you: 0, hired: 0, done: 0, players: {} };
  games.forEach(function (g) { tot.play += g.played || 0; tot.earned += g.earned || 0; tot.you += g.youTiles || 0; tot.hired += g.hiredTiles || 0; tot.done += g.done || 0; tot.players[(g.player || '') + '|' + (g.online || '')] = 1; });
  var week = games.filter(function (g) { return Date.now() - g.last < 7 * 86400000; }).length;
  $('stats').innerHTML = [
    [games.length, 'games'], [Object.keys(tot.players).length, 'player profiles'], [week, 'played this week'], [hrs(tot.play), 'total play'],
    [money(tot.earned), 'earned, all games'], [num(tot.you), 'tiles driven by players'], [num(tot.hired), 'tiles by hired drivers'], [num(tot.done), 'missions completed'], [feedback.length, 'feedback']
  ].map(function (s) { return '<div class="stat"><b>' + esc(s[0]) + '</b><span>' + esc(s[1]) + '</span></div>'; }).join('');
  $('ghead').innerHTML = '<tr>' + COLS.map(function (c) { return '<th data-k="' + c[0] + '"' + (c[2] ? ' class="n"' : '') + '>' + esc(c[1]) + (sortKey === c[0] ? (sortDir > 0 ? ' ▲' : ' ▼') : '') + '</th>'; }).join('') + '</tr>';
  Array.prototype.forEach.call($('ghead').querySelectorAll('th'), function (th) {
    th.onclick = function () { var k = th.dataset.k; if (sortKey === k) sortDir = -sortDir; else { sortKey = k; sortDir = typeof (games[0] || {})[k] === 'string' ? 1 : -1; } renderGames(); };
  });
  $('gbody').innerHTML = list.map(function (g) {
    var cells = {
      fleet: '<b>' + esc(g.fleet || '(unnamed)') + '</b>' + (g.online ? ' <span class="chip">online: ' + esc(g.online) + '</span>' : ''),
      player: esc(g.player), seed: '<span class="mute">' + esc(g.seed) + '</span>', start: cellTxt(g.start), played: hrs(g.played), day: num(g.day),
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

function fbHtml(f) {
  return '<div class="fb"><h4>' + esc(f.subject) + '</h4><div class="meta">' + esc(when(f.at)) + ' · ' + esc(f.fleet || 'no fleet') + ' · ' + esc(f.player || '?') +
    (f.online ? ' · online as ' + esc(f.online) : '') + (f.seed ? ' · ' + esc(f.seed) + ' cell ' + cellTxt(f.cell) : '') + (f.day ? ' · day ' + esc(f.day) : '') +
    (f.gid ? ' · <a class="link" data-gid="' + esc(f.gid) + '">open game</a>' : '') + '</div><p>' + esc(f.text) + '</p></div>';
}
function renderFeedback() {
  var list = feedback.filter(function (f) { return matches([f.subject, f.text, f.fleet, f.player, f.online].join(' ')); });
  $('feedback').innerHTML = list.map(fbHtml).join('') || '<div class="mute">No feedback yet.</div>';
  Array.prototype.forEach.call($('feedback').querySelectorAll('a[data-gid]'), function (a) { a.onclick = function () { tab('games'); openGame(a.dataset.gid); }; });
}

api('games').then(function (j) { if (j.ok) { showApp(true); load(); } else showApp(false); });
</script>
</body></html>`;

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
    gen: r.gen
  };
}
async function adminHttp(req, res, path, store2) {
  const ip = ipOf(req);
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
  if (what === "feedback") {
    const all = await store2.list("feedback:");
    const list = all.map((e) => ({ id: e.key.slice("feedback:".length), ...e.value }));
    json(res, 200, { ok: true, feedback: list.sort((a, b) => b.at - a.at) });
    return true;
  }
  json(res, 404, { ok: false });
  return true;
}

// shared/protocol.ts
var PROTOCOL = 1;
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

// src/world/tiles.ts
var N = 256;
var SEA = 2;
var CHUNK = 16;
var CHUNKS = N / CHUNK;
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
  corners = new Int8Array(N * N * 4);
  water = new Int8Array(N * N).fill(-1);
  // water surface height in steps, -1 = none
  biome = new Uint8Array(N * N);
  flags = new Uint8Array(N * N);
  fieldId = new Int16Array(N * N).fill(-1);
  coastDist = new Uint16Array(N * N);
  // tiles from the ocean (0 = ocean)
  elev = new Float32Array(N * N);
  // continuous elevation used during generation
  temp = new Float32Array(N * N);
  edges = new Uint8Array(N * N);
  // car-agnostic drivable edge bits (bit d)
  road = new Uint8Array(N * N);
  // widest RoadType covering each tile
  roadEdges = new Uint8Array(N * N);
  // bit d: a road surface runs across that edge
  roadCover = new Uint8Array(N * N);
  // how many roads cover the tile
  roadAxis = new Uint8Array(N * N);
  // axes those roads run along (1 = x, 2 = z); both + 2 roads = junction
  laneSide = new Uint8Array(N * N);
  // four-lane tiles: bit 0/1 low/high-z half of an x-running road, bit 2/3 low/high-x half of a z-running one
  roads = [];
  /** Bumped whenever the road tables are rebuilt, so what is worked out from them (the curves) is redone. */
  roadStamp = 0;
  deckAxis = new Uint8Array(N * N);
  // a bridge deck crosses this tile along x (1) or z (2)
  deckToll = new Uint8Array(N * N);
  // that deck is a toll bridge of the player's
  deckLo = new Float32Array(N * N);
  // deck world height at the tile's low-x / low-z edge
  deckHi = new Float32Array(N * N);
  // ... and at its high edge
  deckRoad = new Uint8Array(N * N);
  // the road that deck carries: its surface is painted ROAD_INFO.lift above the deck
  townId = new Int8Array(N * N).fill(-1);
  lot = new Uint8Array(N * N);
  // Lot finish on town tiles
  blocked = new Uint8Array(N * N);
  // a building stands here: nothing drives through
  cleared = new Uint8Array(N * N);
  // bulldozed: no trees, rocks or cacti grow here any more
  /** Fences a car has knocked flat, by tile * 4 + edge: which way each fell (+1 or -1 along its local z). Not saved. */
  knocked = /* @__PURE__ */ new Map();
  feature = new Uint8Array(N * N);
  // Feature: a broad beach, a quarry, a piste, an airfield
  roof = new Float32Array(N * N);
  // world y of the building top on the tile (0 = none)
  surface = new Uint8Array(N * N);
  // Surface: dune sand, salt, ice, lava, marsh, rock, cave
  /** Which biome reaches this tile: 1 + its index in REGION_KINDS, or 0 for none. */
  regionK = new Uint8Array(N * N);
  /** How far into that biome the tile is, 0..255: 255 inside it, fading over a cell at a continent's blob edge. */
  regionW = new Uint8Array(N * N);
  /**
   * The height, in steps, above which snow lies all year: 0 for the ordinary line. High country and a
   * continent's lifted heart carry their own, so a tableland at forty steps is not white from edge to edge.
   */
  snowLine = new Uint8Array(N * N);
  /** The highest corner the generator made on this county, in steps. */
  peak = 24;
  /** Named landforms on this cell, for the map, the labels, the props and the tourist board. */
  landmarks = [];
  towns = [];
  ports = [];
  /** Every town and harbour building, in the order they were built; a building's `uid` indexes this. */
  allBuildings = [];
  /** Buildings out in the country that belong to no town: wind turbines, orchards, windmills. */
  countryside = [];
  /** The roads generated between towns, and whether each could be built. */
  links = [];
  portId = new Int8Array(N * N).fill(-1);
  /** RailBit per tile: track, crossing, deck overhead, tunnel beneath, station pad, loop, pier, reserved margin. */
  rail = new Uint8Array(N * N);
  /** At-grade straight track a road may cross at right angles: 1 when the rails run along x, 2 along z. */
  railAxis = new Uint8Array(N * N);
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
  spawn = { x: N / 2 + 0.5, z: N / 2 + 0.5, heading: 0 };
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
  stats = { land: 0, drivable: 0, reachable: 0, genMs: 0, canyons: 0, fields: 0, lakes: 0, maxSlope: 0, passes: 0, riverCrossings: 0, railMs: 0 };
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
  return Math.round(N * 0.25 + cell01(seedHash2, ax, az, 400 + axis) * N * 0.5);
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
function cached(cache, key, make) {
  if (cache.has(key)) return cache.get(key);
  const v = make();
  cache.set(key, v);
  if (cache.size > 2e4) cache.delete(cache.keys().next().value);
  return v;
}
function regionAnchor(seedHash2, cx, cz) {
  return cached(anchorCache, `${seedHash2}:${cx},${cz}`, () => {
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
  return cached(blobCache, `${seedHash2}:${ax},${az}`, () => {
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
  return cached(regionCache, `${seedHash2}:${cx},${cz}`, () => {
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

// src/world/roads.ts
var ROAD_RANK = {
  [0 /* None */]: 0,
  [1 /* Dirt */]: 1,
  [6 /* Gravel */]: 2,
  [2 /* Brick */]: 3,
  [7 /* Cobble */]: 4,
  [3 /* Paved */]: 5,
  [4 /* TwoLane */]: 6,
  [5 /* FourLane */]: 7
};

// src/world/townKinds.ts
var OPEN2 = { solid: false, apart: 8 };
var KIND_SPEC = {
  // ---- parks and plazas ----
  meadow: { w: [5, 8], d: [5, 7], zone: "edge", lot: 1 /* Lawn */, ...OPEN2, frontage: false },
  civicSquare: { w: [5, 6], d: [4, 5], zone: "core", lot: 7 /* Plaza */, ...OPEN2 },
  fountainSquare: { w: 3, d: 3, zone: "core", lot: 7 /* Plaza */, ...OPEN2 },
  greenway: { w: [6, 10], d: 2, zone: "any", lot: 3 /* Park */, ...OPEN2, waterside: true, frontage: false },
  picnicGround: { w: 3, d: 3, zone: "edge", lot: 1 /* Lawn */, ...OPEN2 },
  dogRun: { w: 2, d: 3, zone: "mid", lot: 12 /* Gravel */, ...OPEN2 },
  plazaSteps: { w: [3, 4], d: 3, zone: "core", lot: 7 /* Plaza */, ...OPEN2, allow: 3 },
  sculpturePark: { w: 4, d: 4, zone: "mid", lot: 3 /* Park */, ...OPEN2 },
  bandstandGreen: { w: 4, d: 4, zone: "mid", lot: 3 /* Park */, ...OPEN2 },
  memorial: { w: [2, 3], d: 2, zone: "core", lot: 12 /* Gravel */, ...OPEN2 },
  skatePark: { w: 2, d: 3, zone: "mid", lot: 6 /* Asphalt */, ...OPEN2 },
  promenade: { w: [6, 10], d: 1, zone: "any", lot: 7 /* Plaza */, ...OPEN2, along: "wide" },
  cemetery: { w: [4, 6], d: [4, 5], zone: "edge", lot: 1 /* Lawn */, solid: true, apart: 10 },
  amphitheatre: { w: 4, d: 4, zone: "edge", lot: 1 /* Lawn */, ...OPEN2, allow: 3 },
  marketSquare: { w: [4, 5], d: 4, zone: "core", lot: 7 /* Plaza */, ...OPEN2 },
  lakePark: { w: [7, 9], d: [6, 8], zone: "mid", lot: 3 /* Park */, solid: true, apart: 12, waterside: "prefer", frontage: false },
  courts: { w: [3, 4], d: 3, zone: "mid", lot: 4 /* Pitch */, ...OPEN2 },
  formalGarden: { w: [4, 5], d: [4, 5], zone: "core", lot: 12 /* Gravel */, ...OPEN2 },
  allotments: { w: [3, 4], d: 3, zone: "edge", lot: 5 /* Yard */, ...OPEN2 },
  botanicalGarden: { w: [5, 6], d: [5, 6], zone: "mid", lot: 3 /* Park */, solid: true, apart: 12 },
  playground: { w: [2, 3], d: [2, 3], zone: "mid", lot: 13 /* Sand */, ...OPEN2 },
  grove: { w: [3, 4], d: [3, 4], zone: "edge", lot: 3 /* Park */, ...OPEN2 },
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
  souk: { w: [4, 5], d: 2, zone: "core", lot: 7 /* Plaza */, ...OPEN2, apart: 10 },
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
  colosseum: { w: [6, 7], d: [5, 6], zone: "edge", lot: 12 /* Gravel */, gap: 1, apart: 40 },
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
  scrambleCrossing: { w: 3, d: 3, zone: "core", lot: 6 /* Asphalt */, ...OPEN2, apart: 40 },
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
  stoneAmphitheatre: { w: 4, d: 4, zone: "edge", lot: 12 /* Gravel */, ...OPEN2, allow: 3 },
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
  welcomeSign: { w: 1, d: 1, zone: "edge", lot: 1 /* Lawn */, roadside: true, apart: 40 },
  needleTower: { w: 2, d: 2, zone: "core", lot: 7 /* Plaza */, apart: 60 },
  fountainResort: { w: 5, d: 4, zone: "mid", lot: 7 /* Plaza */, apart: 40 },
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
  nightMarket: { w: 3, d: 1, zone: "mid", lot: 2 /* Paved */, apart: 6 }
};

// src/world/railSurface.ts
var DECK_HALF = {
  [3 /* Trestle */]: 0.24,
  [4 /* Viaduct */]: 0.31,
  [5 /* Truss */]: 0.27,
  [6 /* Sea */]: 0.29
};

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
var KN = N / KG;
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

// src/world/geology.ts
var SOURCE_MIN = SEA + 13;

// src/world/generate.ts
var NN = N * N;
var GEN_VERSION = 41;
var PAD = 40;
var P = N + 2 * PAD;
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
  out.why = out.cells < SPAWN.cells ? `only ${out.cells} cells reachable by car` : out.dealers < SPAWN.dealers ? `${out.dealers} land dealer${out.dealers === 1 ? "" : "s"} within ${SPAWN.dealerReach} cells` : out.shops < SPAWN.shops ? `${out.shops} shop${out.shops === 1 ? "" : "s"} within ${SPAWN.shopReach} cells` : null;
  out.ok = out.why === null;
  return out;
}
function rankSpawns(seedHash2, around, radius, pick, allow = () => true) {
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
  const i = Math.min(top - 1, Math.floor(pick * top));
  return [ok[i], ...ok.slice(0, i), ...ok.slice(i + 1)];
}
var START_RADIUS = 8;

// server/index.ts
var PORT = Number(process.env.PORT) || 8787;
var SEED = process.env.WORLD_SEED || "tiny-fleet-online";
var PEPPER = process.env.PIN_PEPPER || "tinyfleet";
var MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 100;
var HERE = fileURLToPath(new URL(".", import.meta.url));
var SITE = existsSync(join2(HERE, "play", "index.html")) && !process.env.PUBLIC_DIR ? HERE : null;
var PUBLIC = SITE ? join2(HERE, "play") : resolve(process.env.PUBLIC_DIR || [join2(HERE, "public"), join2(HERE, "..", "..", "dist"), join2(process.cwd(), "dist")].find((d) => existsSync(join2(d, "index.html"))) || join2(HERE, "public"));
var store;
var world;
var worldDirty = false;
var players;
var cells;
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
var CELL = /^-?\d{1,5},-?\d{1,5}$/;
var NAME = /^[A-Za-z0-9_-]{3,16}$/;
var LOT = /^(-?\d{1,5},-?\d{1,5})#[bp]\d{1,7}$/;
var TOWN = /^(-?\d{1,5},-?\d{1,5})#\d{1,4}$/;
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
function pickHome() {
  const open = (cx, cz) => Math.max(Math.abs(cx), Math.abs(cz)) <= SPAWN_REACH && !world.homes[cellKey(cx, cz)];
  const spots = [...conns.values()].map((c) => cellOfKey(c.cell)).sort(() => Math.random() - 0.5);
  for (const [ax, az] of spots) {
    const best2 = rankSpawns(
      seedHash,
      [ax, az],
      SPAWN_NEAR,
      Math.random(),
      (cx, cz) => open(cx, cz) && Math.max(Math.abs(cx - ax), Math.abs(cz - az)) <= SPAWN_NEAR
    )[0];
    if (best2?.ok) return [best2.cx, best2.cz];
  }
  const best = rankSpawns(seedHash, [0, 0], START_RADIUS, Math.random(), open)[0];
  if (best?.ok) return [best.cx, best.cz];
  return pickHomeAnywhere();
}
var nextHome = null;
function upcomingHome() {
  if (!nextHome || world.homes[cellKey(nextHome.at[0], nextHome.at[1])] || Date.now() - nextHome.t > 6e5) nextHome = { at: pickHome(), t: Date.now() };
  return nextHome.at;
}
function takeHome() {
  const at = upcomingHome();
  nextHome = null;
  return at;
}
function pickHomeAnywhere() {
  const free = (cx, cz, strict) => Math.max(Math.abs(cx), Math.abs(cz)) <= SPAWN_REACH && !world.homes[cellKey(cx, cz)] && landAt(cx, cz, strict);
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
  if (!r && make) rooms.set(c, r = { subs: /* @__PURE__ */ new Map(), host: null });
  return r ?? null;
}
var awake = (x) => Date.now() - x.lastMove < 6e3;
function rehost(c) {
  const r = rooms.get(c);
  if (!r) return;
  if (!r.subs.size) {
    rooms.delete(c);
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
}
function peopleIn(doc) {
  const ids = /* @__PURE__ */ new Set();
  for (const d of Object.values(doc.deeds ?? {})) ids.add(d.by);
  for (const row of Object.values(doc.carriage ?? {})) {
    if (isPid(row[1])) ids.add(row[1]);
    for (const k of Object.keys(row[3] ?? {})) if (isPid(k)) ids.add(k);
  }
  return [...ids].map(whoOf).filter((w) => !!w);
}
async function subscribe(c, want) {
  const next = new Set(want.filter((k) => CELL.test(k)).slice(0, 80));
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
    send(c, { t: "cell", c: k, doc, host: (r.host ?? c).pid, people: peopleIn(doc) });
  }
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
  const name = String(m.name ?? "").trim();
  if (!NAME.test(name)) {
    refuse(ws, "name", "A name is 3 to 16 letters, digits, - or _.", false);
    return null;
  }
  const lower = name.toLowerCase();
  let pid = world.names[lower];
  let doc = pid ? await players.open(pid) : null;
  const now = Date.now();
  if (!doc || !doc.hash) {
    const pin = String(m.pin ?? "");
    if (!m.create) {
      refuse(ws, "new", `Nobody is called ${name} here yet.`, false);
      return null;
    }
    if (pin.length < 4 || pin.length > 32) {
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
    const salt = randomBytes2(12).toString("hex");
    const home = takeHome();
    doc = await players.open(pid);
    Object.assign(doc, {
      id: pid,
      name,
      salt,
      hash: await pinHash(pin, salt),
      brand: cleanBrand(m.create.brand),
      home,
      epoch: world.epoch,
      created: now,
      seen: now,
      owed: 0,
      record: null
    });
    world.names[lower] = pid;
    world.homes[cellKey(home[0], home[1])] = pid;
    world.people[pid] = { name, brand: doc.brand };
    worldDirty = true;
    players.touch(pid);
    console.log(`new fleet: ${name} (${pid}) at ${home.join(",")}`);
  } else {
    const byToken = !!m.token && !!doc.token && now - (doc.tokenAt ?? 0) < 90 * 864e5 && sha(m.token) === doc.token;
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
  const token = randomBytes2(24).toString("base64url");
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
    lastMove: now,
    look: null,
    subs: /* @__PURE__ */ new Set(),
    held: /* @__PURE__ */ new Set(),
    joined: now,
    chat: [],
    msgs: 0,
    msgsAt: now
  };
  const owed = Math.round(doc.owed || 0);
  doc.owed = 0;
  send(ws, {
    t: "welcome",
    you: c.who,
    world: { seed: world.seed, gen: world.gen, hours: world.hours },
    record: doc.record,
    home: doc.home,
    token,
    owed,
    online: [...conns.values()].map((x) => ({ who: x.who, c: x.cell, look: x.look ?? void 0 }))
  });
  for (const x of conns.values()) send(x, { t: "on", who: c.who, c: c.cell });
  conns.set(pid, c);
  if (board.players[pid]) {
    board.players[pid].online = true;
  }
  console.log(`+ ${doc.name} (${conns.size} online)`);
  return c;
}
function drop(c, tell = true) {
  if (conns.get(c.pid) !== c) return;
  conns.delete(c.pid);
  for (const k of c.subs) {
    rooms.get(k)?.subs.delete(c);
    if (c.held.delete(k)) cells.hold(`${world.epoch}:${k}`, -1);
    rehost(k);
  }
  c.subs.clear();
  c.doc.seen = Date.now();
  players.touch(c.pid);
  players.hold(c.pid, -1);
  if (tell) for (const x of conns.values()) send(x, { t: "off", id: c.pid });
  console.log(`- ${c.doc.name} (${conns.size} online)`);
}
var num = (v) => typeof v === "number" && Number.isFinite(v) ? v : 0;
var poseOf = (p) => Array.isArray(p) && p.length >= 7 && p.every((v) => typeof v === "number" && Number.isFinite(v)) ? p.slice(0, 9) : null;
async function handle(c, m) {
  switch (m.t) {
    case "m": {
      const p = poseOf(m.p);
      if (!p) return;
      c.pose = p;
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
      return;
    }
    case "brand": {
      const b = cleanBrand(m.brand);
      if (!b) return;
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
        const d = named && named.by !== c.pid ? named : Object.values(doc.deeds ?? {}).find((row) => row.by !== c.pid && (row.rects ?? []).some(([rx, rz, rw, rd]) => typeof x === "number" && typeof z === "number" && x >= rx && z >= rz && x < rx + rw && z < rz + rd));
        if (d) {
          send(c, { t: "deny", what: "raze", c: k, why: `That belongs to ${whoOf(d.by)?.brand?.name ?? whoOf(d.by)?.name ?? "another fleet"}.` });
          return;
        }
      }
      if (doc.edits.length >= 3e4) {
        send(c, { t: "deny", what: "edit", c: k, why: "This county has been changed as much as it can be." });
        return;
      }
      const kept = { ...e, by: c.pid };
      doc.edits.push(kept);
      cells.touch(`${world.epoch}:${k}`);
      toRoom(k, { t: "edit", c: k, e: kept }, c);
      return;
    }
    case "land": {
      const k = String(m.c);
      const doc = c.subs.has(k) ? cells.peek(`${world.epoch}:${k}`) : null;
      if (!doc || !m.chunks || typeof m.chunks !== "object") return;
      const out = {};
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
      if (m.carriage) {
        for (const [key, v] of Object.entries(m.carriage)) if (own(key) && Array.isArray(v) && v.length === 4) {
          (doc.carriage ??= {})[key] = v;
          (pass.carriage ??= {})[key] = v;
        }
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
      const t = TOWN.exec(String(m.town));
      const host = t ? rooms.get(t[1])?.host : null;
      const amt = num(m.amt);
      if (!host || host === c || !(amt > 0) || amt > 1e6) return;
      send(host, { t: "credit", town: m.town, by: c.pid, amt, lot: typeof m.lot === "string" && LOT.test(m.lot) ? m.lot : void 0, lotAmt: num(m.lotAmt) || void 0, dir: m.dir === "out" ? "out" : "in" });
      return;
    }
    case "deeds": {
      for (const [key, v] of Object.entries(m.set ?? {})) {
        const lot = LOT.exec(key);
        if (!lot || !v) continue;
        const dk = `${world.epoch}:${lot[1]}`;
        const doc = await cells.open(dk);
        const have = doc.deeds?.[key];
        if (have && have.by !== c.pid) {
          const w = whoOf(have.by);
          if (w) send(c, { t: "deedDeny", key, who: w });
          continue;
        }
        const row = { by: c.pid, name: String(v.name ?? "").replace(/[<>&"`\u0000-\u001f]/g, "").slice(0, 60), ceil: Math.max(0, Math.min(1e7, num(v.ceil))) };
        if (v.hq !== void 0) row.hq = Math.max(0, Math.min(9, Math.round(num(v.hq))));
        const rects = Array.isArray(v.rects) ? v.rects.filter((r) => Array.isArray(r) && r.length === 4 && r.every((n) => Number.isInteger(n) && n >= 0 && n <= 256) && r[2] <= 64 && r[3] <= 64).slice(0, 24) : have?.rects;
        if (rects?.length) row.rects = rects;
        (doc.deeds ??= {})[key] = row;
        cells.touch(dk);
        toRoom(lot[1], { t: "deed", key, row, who: c.who }, c);
      }
      for (const key of m.del ?? []) {
        const lot = LOT.exec(String(key));
        if (!lot) continue;
        const dk = `${world.epoch}:${lot[1]}`;
        const doc = await cells.open(dk);
        if (doc.deeds?.[key]?.by !== c.pid) continue;
        delete doc.deeds[key];
        cells.touch(dk);
        toRoom(lot[1], { t: "deed", key, row: null }, c);
      }
      return;
    }
    case "rent": {
      const day = Math.floor(world.hours / 24);
      for (const [key, v] of Object.entries(m.k ?? {})) {
        const lot = LOT.exec(key);
        if (!lot) continue;
        const doc = cells.peek(`${world.epoch}:${lot[1]}`);
        const row = doc?.deeds?.[key];
        if (!row || row.by === c.pid) continue;
        const t = rentToday.get(key);
        const taken = t && t[0] === day ? t[1] : 0;
        const amt = Math.floor(Math.min(num(v), Math.max(0, row.ceil - taken)));
        if (!(amt > 0)) continue;
        rentToday.set(key, [day, taken + amt]);
        const owner2 = conns.get(row.by);
        if (owner2) send(owner2, { t: "rent", key, amt });
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
      }
      const me = m.board?.me;
      if (me) {
        board.players[c.pid] = {
          id: c.pid,
          name: c.doc.brand?.name ?? c.doc.name,
          by: c.doc.name,
          cpu: false,
          worth: Math.round(num(me.worth)),
          units: Math.round(num(me.units)),
          towns: Math.round(num(me.towns)),
          primary: c.doc.brand?.primary ?? 14827823,
          accent: c.doc.brand?.accent ?? 16777215,
          logo: c.doc.brand?.logo ?? "",
          v: metricValues(me.v)
        };
        boardDirty = true;
      }
      for (const row of m.board?.cpu ?? []) {
        const home = String(row?.id ?? "").split("/")[0];
        if (!CELL.test(home) || rooms.get(home)?.host !== c) continue;
        board.cpu[String(row.id).slice(0, 40)] = {
          id: String(row.id).slice(0, 40),
          name: String(row.name ?? "").slice(0, 40),
          cpu: true,
          worth: Math.round(num(row.worth)),
          units: Math.round(num(row.units)),
          towns: Math.round(num(row.towns)),
          primary: num(row.primary) & 16777215,
          accent: num(row.accent) & 16777215,
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
    case "board":
      send(c, { t: "board", rows: boardRows() });
      return;
    case "bye":
      c.ws.close();
      return;
    default:
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
  for (const x of conns.values()) {
    if (cheb(c.cell, x.cell) > CHAT_CELLS) continue;
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
      if (m === to || cheb(m.cell, to.cell) > SEE_CELLS) continue;
      a.push([m.pid, ...m.pose]);
      for (const u of m.fleet) f.push([m.pid, ...u]);
    }
    if (a.length) send(to, { t: "mv", a, f });
  }
  for (const m of movers) m.moved = false;
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
  if (!conns.size) return;
  for (const [k, r] of rooms) if (r.host && !awake(r.host)) rehost(k);
  const at = {};
  for (const c of conns.values()) at[c.pid] = c.cell;
  const text = JSON.stringify({ t: "tick", hours: world.hours, at });
  for (const c of conns.values()) if (c.ws.readyState === 1) c.ws.send(text);
}
var flushing = false;
async function flush(evict = true) {
  if (flushing) return;
  flushing = true;
  try {
    if (worldDirty) {
      worldDirty = false;
      await store.set("world", world);
    }
    if (boardDirty) {
      boardDirty = false;
      await store.set("board", board);
    }
    await players.flush(evict);
    await cells.flush(evict);
  } catch (e) {
    console.error("flush failed:", e.message);
  } finally {
    flushing = false;
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
  if (path.startsWith("/api/") || path === ADMIN_PATH || path === `${ADMIN_PATH}/`) {
    adminHttp(req, res, path, store).then((done) => {
      if (!done && !res.headersSent) res.writeHead(404, { "content-type": "text/plain" }).end("Not found");
    }).catch((e) => {
      console.error("admin:", e.message);
      if (!res.headersSent) res.writeHead(500).end();
    });
    return;
  }
  if (path === "/online.json" || path === "/play/online.json") {
    res.writeHead(200, { "content-type": "application/json", "access-control-allow-origin": "*", "cache-control": "no-store" }).end(JSON.stringify({ tinyfleet: true, protocol: PROTOCOL, gen: world.gen, seed: world.seed, start: upcomingHome(), players: conns.size, max: MAX_PLAYERS, day: Math.floor(world.hours / 24) + 1 }));
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
    world = { ...world, seed: SEED, gen: GEN_VERSION, epoch: world.epoch + 1, homes: {}, hours: 8 };
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
  const server = createServer(http);
  const wss = new import_websocket_server.default({ server, maxPayload: 6 * 1024 * 1024 });
  wss.on("connection", (ws, req) => {
    const fwd = String(req.headers["x-forwarded-for"] ?? "").split(",").map((h) => h.trim()).filter(Boolean);
    const ip = String(req.headers["x-real-ip"] ?? "").trim() || fwd[fwd.length - 1] || String(req.socket.remoteAddress ?? "");
    let conn = null;
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
      if (conn) {
        const now = Date.now();
        if (now - conn.msgsAt > 1e3) {
          conn.msgsAt = now;
          conn.msgs = 0;
        }
        if (++conn.msgs > 400) {
          ws.close();
          return;
        }
      }
      busy = busy.then(async () => {
        if (!conn) {
          if (m.t === "hello") conn = await hello(ws, ip, m);
          return;
        }
        if (conns.get(conn.pid) !== conn) return;
        await handle(conn, m);
      }).catch((e) => console.error("message failed:", e.stack ?? e));
    });
    ws.on("close", () => {
      clearInterval(beatT);
      if (conn) drop(conn);
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
