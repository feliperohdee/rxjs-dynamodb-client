'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});
exports.Util = undefined;

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

class Util {
	constructor() {
		this.data = {};
	}

	raw(data) {
		this.data = data;

		return this;
	}

	anormalizeValue(value) {
		if (_lodash2.default.isBoolean(value)) {
			return {
				BOOL: value
			};
		}

		if (_lodash2.default.isNumber(value)) {
			return {
				N: value.toString()
			};
		}

		if (_lodash2.default.isString(value)) {
			return {
				S: _lodash2.default.isEmpty(value) ? ':empty' : value
			};
		}

		if (_lodash2.default.isNull(value)) {
			return {
				NULL: true
			};
		}

		// SS or NS
		if (_lodash2.default.isObject(value) && value instanceof Util) {
			return value.data;
		}

		if (_lodash2.default.isArray(value)) {
			return _lodash2.default.reduce(value, (result, value) => {
				result.L.push(this.anormalizeValue(value));

				return result;
			}, {
				L: []
			});
		}

		if (_lodash2.default.isObject(value)) {
			return _lodash2.default.reduce(value, (result, value, key) => {
				result.M[key] = this.anormalizeValue(value);

				return result;
			}, {
				M: {}
			});
		}
	}

	anormalizeItem(item) {
		return _lodash2.default.reduce(item, (result, value, key) => {
			result[key] = this.anormalizeValue(value);

			return result;
		}, {});
	}

	anormalizeList(list) {
		return _lodash2.default.reduce(list, (result, value) => {
			result.push(this.anormalizeItem(value));

			return result;
		}, []);
	}

	anormalizeType(value) {
		if (_lodash2.default.isBoolean(value)) {
			return 'BOOL';
		}

		if (_lodash2.default.isNumber(value)) {
			return 'N';
		}

		if (_lodash2.default.isString(value)) {
			return 'S';
		}

		if (_lodash2.default.isNull(value)) {
			return 'NULL';
		}

		if (_lodash2.default.isArray(value)) {
			return 'L';
		}

		if (_lodash2.default.isObject(value)) {
			return 'M';
		}
	}

	normalizeValue(value) {
		if (_lodash2.default.has(value, 'BOOL')) {
			return value.BOOL;
		}

		if (_lodash2.default.has(value, 'N')) {
			return parseFloat(value.N);
		}

		if (_lodash2.default.has(value, 'S')) {
			return value.S === ':empty' ? '' : value.S;
		}

		if (_lodash2.default.has(value, 'NULL')) {
			return null;
		}

		if (_lodash2.default.has(value, 'SS')) {
			return value.SS;
		}

		if (_lodash2.default.has(value, 'NS')) {
			return _lodash2.default.reduce(value.NS, (result, value) => {
				result.push(parseFloat(value));

				return result;
			}, []).sort();
		}

		if (_lodash2.default.has(value, 'L')) {
			return _lodash2.default.reduce(value.L, (result, value) => {
				result.push(this.normalizeValue(value));

				return result;
			}, []);
		}

		if (_lodash2.default.has(value, 'M')) {
			return _lodash2.default.reduce(value.M, (result, value, key) => {
				result[key] = this.normalizeValue(value);

				return result;
			}, {});
		}
	}

	normalizeItem(item) {
		if (!item) {
			return null;
		}

		return _lodash2.default.reduce(item, (result, value, key) => {
			result[key] = this.normalizeValue(value);

			return result;
		}, {});
	}

	normalizeList(items) {
		return _lodash2.default.reduce(items, (result, value) => {
			result.push(this.normalizeItem(value));

			return result;
		}, []);
	}
}
exports.Util = Util;