'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});
exports.Crud = exports.ConsumedCapacity = exports.ReturnValues = exports.Select = exports.Util = exports.Request = exports.ExpressionsHelper = exports.DynamoDB = undefined;

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var _rxjs = require('rxjs');

var _Util = require('./Util');

var _Request = require('./Request');

var _ExpressionsHelper = require('./ExpressionsHelper');

var _constants = require('./constants');

var _Crud = require('./Crud');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

class DynamoDB {
	constructor(deps = {}) {
		if (!deps.client) {
			throw new Error('no dynamoDb client provided.');
		}

		this.client = deps.client;
	}

	get request() {
		return new _Request.Request(this.client);
	}

	get util() {
		return new _Util.Util();
	}

	table(name, schema) {
		return this.request.table(name, schema);
	}

	call(method, args) {
		return this.request.routeCall(method, args);
	}

	S(data) {
		return this.util.raw({
			S: data
		});
	}

	N(data) {
		if (_lodash2.default.isNumber(data)) {
			data = data.toString();
		}

		return this.util.raw({
			N: data
		});
	}

	SS(data) {
		return this.util.raw({
			SS: data
		});
	}

	NS(data) {
		return this.util.raw({
			NS: _lodash2.default.map(data, n => n.toString())
		});
	}

	L(data) {
		return this.util.raw({
			L: _lodash2.default.reduce(data, (result, value) => {
				result.push(this.util.anormalizeValue(value));

				return result;
			}, [])
		});
	}
}

exports.DynamoDB = DynamoDB;
exports.ExpressionsHelper = _ExpressionsHelper.ExpressionsHelper;
exports.Request = _Request.Request;
exports.Util = _Util.Util;
exports.Select = _constants.Select;
exports.ReturnValues = _constants.ReturnValues;
exports.ConsumedCapacity = _constants.ConsumedCapacity;
exports.Crud = _Crud.Crud;