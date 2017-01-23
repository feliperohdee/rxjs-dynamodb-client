import _ from 'lodash';

export class Util {
	data = {};

	raw(data) {
		this.data = data;

		return this;
	}

	anormalizeValue(value) {
		if (_.isBoolean(value)) {
			return {
				BOOL: value
			};
		}

		if (_.isNumber(value)) {
			return {
				N: value.toString()
			};
		}

		if (_.isString(value)) {
			return {
				S: _.isEmpty(value) ? ':empty' : value
			};
		}

		if (_.isNull(value)) {
			return {
				NULL: true
			};
		}

		// SS or NS
		if (_.isObject(value) && value instanceof Util) {
			return value.data;
		}

		if (_.isArray(value)) {
			return _.reduce(value, (result, value) => {
				result.L.push(this.anormalizeValue(value));

				return result;
			}, {
				L: []
			});
		}

		if (_.isObject(value)) {
			return _.reduce(value, (result, value, key) => {
				result.M[key] = this.anormalizeValue(value);

				return result;
			}, {
				M: {}
			});
		}
	}

	anormalizeItem(item) {
		return _.reduce(item, (result, value, key) => {
			result[key] = this.anormalizeValue(value);

			return result;
		}, {});
	}

	anormalizeList(list) {
		return _.reduce(list, (result, value) => {
			result.push(this.anormalizeItem(value));

			return result;
		}, []);
	}

	anormalizeType(value) {
		if (_.isBoolean(value)) {
			return 'BOOL';
		}

		if (_.isNumber(value)) {
			return 'N';
		}

		if (_.isString(value)) {
			return 'S';
		}

		if (_.isNull(value)) {
			return 'NULL';
		}

		if (_.isArray(value)) {
			return 'L';
		}

		if (_.isObject(value)) {
			return 'M';
		}
	}

	normalizeValue(value) {
		if (_.has(value, 'BOOL')) {
			return value.BOOL;
		}

		if (_.has(value, 'N')) {
			return parseFloat(value.N);
		}

		if (_.has(value, 'S')) {
			return value.S === ':empty' ? '' : value.S;
		}

		if (_.has(value, 'NULL')) {
			return null;
		}

		if (_.has(value, 'SS')) {
			return value.SS;
		}

		if (_.has(value, 'NS')) {
			return _.reduce(value.NS, (result, value) => {
				result.push(parseFloat(value));

				return result;
			}, []).sort();
		}

		if (_.has(value, 'L')) {
			return _.reduce(value.L, (result, value) => {
				result.push(this.normalizeValue(value));

				return result;
			}, []);
		}

		if (_.has(value, 'M')) {
			return _.reduce(value.M, (result, value, key) => {
				result[key] = this.normalizeValue(value);

				return result;
			}, {});
		}
	}

	normalizeItem(item) {
		if (!item) {
			return null;
		}

		return _.reduce(item, (result, value, key) => {
			result[key] = this.normalizeValue(value);

			return result;
		}, {});
	}

	normalizeList(items) {
		return _.reduce(items, (result, value) => {
			result.push(this.normalizeItem(value));

			return result;
		}, []);
	}
}
