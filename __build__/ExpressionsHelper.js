'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});
exports.ExpressionsHelper = undefined;

var _cuid = require('cuid');

var _cuid2 = _interopRequireDefault(_cuid);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var _rxjs = require('rxjs');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

class ExpressionsHelper {
	constructor(request) {
		this.request = request;
	}

	getTokens(value) {
		return value.match(/([a-zA-Z-_]\d*)+/g);
	}

	attrNotExists(path) {
		return this.attrExists(path, true);
	}

	attrExists(path, not = false) {
		path = _lodash2.default.reduce(this.getTokens(path), (reduction, match) => {
			this.request.addPlaceholderName(match);

			return reduction = reduction.replace(match, `#${match}`);
		}, path);

		return not ? `attribute_not_exists(${path})` : `attribute_exists(${path})`;
	}

	prependList(path, value) {
		return this.appendList(path, value, true);
	}

	appendList(path, value, prepend = false) {
		const placeholder = `appendList_${_cuid2.default.slug()}`;

		path = _lodash2.default.reduce(this.getTokens(path), (reduction, match) => {
			this.request.addPlaceholderName(match);

			return reduction = reduction.replace(match, `#${match}`);
		}, path);

		this.request.addPlaceholderValue({
			[placeholder]: _lodash2.default.isArray(value) ? value : [value]
		}).addPlaceholderValue({
			emptyList: []
		});

		return prepend ? `${path} = list_append(:${placeholder}, if_not_exists(${path}, :emptyList))` : `${path} = list_append(if_not_exists(${path}, :emptyList), :${placeholder})`;
	}

	ifNotExists(path, value) {
		const placeholder = `ifNotExists_${_cuid2.default.slug()}`;

		path = _lodash2.default.reduce(this.getTokens(path), (reduction, match) => {
			this.request.addPlaceholderName(match);

			return reduction = reduction.replace(match, `#${match}`);
		}, path);

		this.request.addPlaceholderValue({
			[placeholder]: value
		});

		return `${path} = if_not_exists(${path}, :${placeholder})`;
	}

	contains(attribute, values = [], condition = 'OR') {
		if (!_lodash2.default.isArray(values)) {
			values = [values];
		}

		if (_lodash2.default.isEmpty(values)) {
			return;
		}

		const placeholders = _lodash2.default.reduce(values, (reduction, value, index) => {
			reduction[`cFilter_${_cuid2.default.slug()}`] = value;

			return reduction;
		}, {});

		const expression = _lodash2.default.map(placeholders, (value, key) => {
			return `contains(#${attribute}, :${key})`;
		}, '').join(` ${condition} `);

		this.request.addPlaceholderName(attribute);
		this.request.addPlaceholderValue(placeholders);

		return `(${expression})`;
	}

	between(attribute, min, max) {
		this.request.addPlaceholderName(attribute);

		if (min) {
			this.request.addPlaceholderValue({
				min
			});
		}

		if (max) {
			this.request.addPlaceholderValue({
				max
			});
		}

		if (min && max) {
			return `#${attribute} BETWEEN :min AND :max`;
		} else if (min) {
			return `#${attribute} >= :min`;
		} else {
			return `#${attribute} <= :max`;
		}
	}

	update(attributes, timestamp = true) {
		const updateAttributes = _lodash2.default.omit(attributes, [this.request.partitionAttr, this.request.sortAttr, 'createdAt', 'updatedAt']);

		return _lodash2.default.reduce(updateAttributes, (reduction, value, key) => {
			if (!_lodash2.default.isObject(value)) {
				value = {
					value
				};
			}

			if (_lodash2.default.isUndefined(value.value) || value.value === '') {
				return reduction;
			}

			this.request.addPlaceholderName(key).addPlaceholderValue({
				[key]: value.value
			});

			if (value.ifNotExists) {
				reduction.unshift(`#${key} = if_not_exists(#${key}, :${key})`);
			} else {
				reduction.unshift(`#${key} = :${key}`);
			}

			return reduction;
		}, timestamp ? [this.timestamp()] : []).join(', ');
	}

	timestamp() {
		this.request.addPlaceholderName(['createdAt', 'updatedAt']).addPlaceholderValue({
			now: _lodash2.default.now()
		});

		return '#createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now';
	}
}
exports.ExpressionsHelper = ExpressionsHelper;