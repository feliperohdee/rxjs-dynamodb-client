'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});
exports.Crud = undefined;

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var _cuid = require('cuid');

var _cuid2 = _interopRequireDefault(_cuid);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

class Crud {
	constructor(name, schema, deps = {}) {
		if (!deps.dynamoDb) {
			throw new Error('no dynamoDb client provided.');
		}

		const {
			primaryKeys,
			indexes = {}
		} = schema;

		this.dynamoDb = deps.dynamoDb;
		this.tableName = name;
		this.tableSchema = schema;
		this.primaryKeys = primaryKeys;
		this.indexes = indexes;
	}

	get request() {
		return this.dynamoDb.table(this.tableName, this.tableSchema);
	}

	get partitionAttr() {
		const {
			partition = null
		} = this.primaryKeys;

		return partition;
	}

	get sortAttr() {
		const {
			sort = null
		} = this.primaryKeys;

		return sort;
	}

	globalIndexPartitionAttr(indexName) {
		if (this.indexes && indexName) {
			const {
				partition = null
			} = this.primaryKeys;

			const {
				partition: indexPartition = null
			} = this.indexes[indexName] || {};

			// is global index
			if (indexPartition !== partition) {
				return indexPartition;
			}
		}

		return null;
	}

	globalIndexSortAttr(indexName) {
		if (this.indexes && indexName) {
			const {
				partition = null
			} = this.primaryKeys;

			const {
				partition: indexPartition = null,
				sort: indexSort = null
			} = this.indexes[indexName] || {};

			// is global index
			if (indexPartition !== partition && indexSort) {
				return indexSort;
			}
		}

		return null;
	}

	localIndexSortAttr(indexName) {
		if (this.indexes && indexName) {
			const {
				partition = null
			} = this.primaryKeys;

			const {
				partition: indexPartition = null,
				sort: indexSort = null
			} = this.indexes[indexName] || {};

			// is local index
			if (indexPartition === partition && indexSort) {
				return indexSort;
			}
		}

		return null;
	}

	fetch(args, hook = false, itemSelector = null, customReducer = null) {
		const {
			resume,
			before,
			after,
			select,
			limit,
			desc,
			indexName,
			consistent
		} = args;

		const partitionAttr = this.globalIndexPartitionAttr(indexName) || this.partitionAttr;
		const sortAttr = this.globalIndexSortAttr(indexName) || this.sortAttr;
		const localIndexSortAttr = this.localIndexSortAttr(indexName);
		const partition = args[partitionAttr];
		const sort = args[sortAttr];
		const localIndex = args[localIndexSortAttr];

		let request = this.request;
		let expression = `#partition = :partition`;
		let hookArgs;

		request.addPlaceholderName({
			partition: partitionAttr
		}).addPlaceholderValue({
			partition
		});

		if (localIndexSortAttr && localIndex) {
			request.addPlaceholderName({
				index: localIndexSortAttr
			}).addPlaceholderValue({
				index: localIndex
			});

			expression += _lodash2.default.isNumber(localIndex) ? ` AND #index = :index` : ` AND begins_with(#index, :index)`;
		} else if (sort) {
			request.addPlaceholderName({
				sort: sortAttr
			}).addPlaceholderValue({
				sort
			});

			expression += _lodash2.default.isNumber(sort) ? ` AND #sort = :sort` : ` AND begins_with(#sort, :sort)`;
		}

		if (indexName) {
			request.index(indexName);
		}

		if (before) {
			if (before !== 'last') {
				request.resume(JSON.parse(this.fromBase64(before)));
			}

			request.desc();
		}

		if ((resume || after) && after !== 'first') {
			request.resume(JSON.parse(this.fromBase64(resume || after)));
		}

		if (!before && !after && desc === true) {
			request.desc();
		}

		if (select) {
			request.select(select);
		}

		if (limit) {
			request.limit(limit);
		}

		if (consistent) {
			request.consistent();
		}

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort
			};

			hookArgs = hook.call(this, hookParams);
		}

		let items = request.query.apply(request, hookArgs || [expression]);

		if (_lodash2.default.isFunction(itemSelector)) {
			items = itemSelector(items);
		}

		if (_lodash2.default.isFunction(customReducer)) {
			return customReducer(items);
		}

		return items.toArray().map(items => {
			const {
				queryStats
			} = request;

			if (before) {
				items = desc ? items : _lodash2.default.reverse(items);
				[queryStats.before, queryStats.after] = [queryStats.after, queryStats.before];
			}

			if (after) {
				items = !desc ? items : _lodash2.default.reverse(items);
			}

			return {
				items,
				stats: _extends({}, queryStats, {
					before: queryStats.before ? this.toBase64(JSON.stringify(queryStats.before)) : null,
					after: queryStats.after ? this.toBase64(JSON.stringify(queryStats.after)) : null
				})
			};
		}, {});
	}

	get(args, hook = false) {
		const {
			select
		} = args;

		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (select) {
			request.select(select);
		}

		if (hook) {
			const hookParams = {
				request,
				partition,
				sort
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.get.apply(request, hookArgs || [{
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	insert(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr] || (0, _cuid2.default)();

		let request = this.request;
		let hookArgs;

		args[sortAttr] = sort;

		if (hook) {
			const hookParams = {
				request,
				partition,
				sort,
				args
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.insert.apply(request, hookArgs || [args]);
	}

	insertOrReplace(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (hook) {
			const hookParams = {
				request,
				partition,
				sort,
				args
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.insertOrReplace.apply(request, hookArgs || [args]);
	}

	insertOrUpdate(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (hook) {
			const hookParams = {
				request,
				partition,
				sort,
				args
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).insertOrUpdate.apply(request, hookArgs || [args]);
	}

	update(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (hook) {
			const hookParams = {
				request,
				partition,
				sort,
				args
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [args]);
	}

	delete(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (hook) {
			const hookParams = {
				request,
				partition,
				sort
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return('ALL_OLD').delete.apply(request, hookArgs || [{
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	appendToList(args, returns = 'ALL_NEW', hook = false) {
		return this.addToList(args, false, returns, hook);
	}

	prependToList(args, returns = 'ALL_NEW', hook = false) {
		return this.addToList(args, true, returns, hook);
	}

	addToList(args, prepend = false, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'SET ';
		let hookArgs;

		const attributes = _lodash2.default.omit(args, [partitionAttr, sortAttr]);

		expression += _lodash2.default.reduce(attributes, (reduction, value, key) => {
			return reduction += `${request.expHelper.appendList(key, value, prepend)}, `;
		}, '');

		expression += request.expHelper.timestamp();

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [expression, {
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	removeFromList(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'REMOVE ';
		let hookArgs;

		const attributes = _lodash2.default.omit(args, [partitionAttr, sortAttr]);

		expression += _lodash2.default.reduce(attributes, (reduction, values, key) => {
			if (!_lodash2.default.isArray(values)) {
				values = [values];
			}

			request.addPlaceholderName({
				[key]: key
			});

			return _lodash2.default.reduce(values, (reduction, value) => {
				return reduction += `#${key}[${value}], `;
			}, '');
		}, '').slice(0, -2);

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [expression, {
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	updateAtList(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'SET ';
		let hookArgs;

		const attributes = _lodash2.default.omit(args, [partitionAttr, sortAttr]);

		expression += _lodash2.default.reduce(attributes, (reduction, indexes, attribute) => {
			request.addPlaceholderName(attribute);

			_lodash2.default.each(indexes, (fields, index) => {
				const placeholderKey = _cuid2.default.slug();

				if (_lodash2.default.isObject(fields)) {
					_lodash2.default.each(fields, (value, key) => {
						request.addPlaceholderName({
							[placeholderKey]: key
						}).addPlaceholderValue({
							[placeholderKey]: value
						});

						reduction += `#${attribute}[${index}].#${placeholderKey} = :${placeholderKey}, `;
					});
				} else {
					request.addPlaceholderValue({
						[placeholderKey]: fields
					});

					reduction += `#${attribute}[${index}] = :${placeholderKey}, `;
				}
			});

			return reduction;
		}, '');

		expression += request.expHelper.timestamp();

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [expression, {
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	addToSet(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'ADD ';
		let hookArgs;

		const attributes = _lodash2.default.omit(args, [partitionAttr, sortAttr]);

		expression += _lodash2.default.reduce(attributes, (reduction, value, key) => {
			if (!_lodash2.default.isArray(value)) {
				value = [value];
			}

			const isStringSet = _lodash2.default.every(value, _lodash2.default.isString);
			const isNumberSet = _lodash2.default.every(value, _lodash2.default.isNumber);

			if (isStringSet) {
				value = request.util.raw({
					SS: value
				});
			} else if (isNumberSet) {
				value = request.util.raw({
					NS: value.map(_lodash2.default.toString)
				});
			} else {
				return reduction;
			}

			request.addPlaceholderName({
				[key]: key
			}).addPlaceholderValue({
				[key]: value
			});

			reduction.push(`#${key} :${key}`);

			return reduction;
		}, []).join();

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [expression, {
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	removeFromSet(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'DELETE ';
		let hookArgs;

		const attributes = _lodash2.default.omit(args, [partitionAttr, sortAttr]);

		expression += _lodash2.default.reduce(attributes, (reduction, value, key) => {
			if (!_lodash2.default.isArray(value)) {
				value = [value];
			}

			const isStringSet = _lodash2.default.every(value, _lodash2.default.isString);
			const isNumberSet = _lodash2.default.every(value, _lodash2.default.isNumber);

			if (isStringSet) {
				value = request.util.raw({
					SS: value
				});
			} else if (isNumberSet) {
				value = request.util.raw({
					NS: value.map(_lodash2.default.toString)
				});
			} else {
				return reduction;
			}

			request.addPlaceholderName({
				[key]: key
			}).addPlaceholderValue({
				[key]: value
			});

			reduction.push(`#${key} :${key}`);

			return reduction;
		}, []).join();

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [expression, {
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	removeAttributes(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'REMOVE ';
		let hookArgs;

		const attributes = _lodash2.default.omit(args, [partitionAttr, sortAttr]);

		expression += _lodash2.default.reduce(attributes, (reduction, value, key) => {
			request.addPlaceholderName({
				[key]: key
			});

			return reduction += `#${key}, `;
		}, '').slice(0, -2);

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = {
				request,
				expression,
				partition,
				sort,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns).update.apply(request, hookArgs || [expression, {
			[partitionAttr]: partition,
			[sortAttr]: sort
		}]);
	}

	clear(args) {
		var _context;

		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let expression = '#partition = :partition';
		const request = this.request;

		if (partition) {
			request.addPlaceholderName({
				partition: partitionAttr
			}).addPlaceholderValue({
				partition
			});
		}

		if (sort) {
			request.addPlaceholderName({
				sort: sortAttr
			}).addPlaceholderValue({
				sort
			});

			expression += ` AND begins_with(#sort, :sort)`;
		}

		return request.limit(1000).query(expression).toArray().mergeMap((_context = this.request).batchWrite.bind(_context));
	}

	toBase64(value) {
		if (_lodash2.default.isEmpty(value)) {
			return null;
		}

		return new Buffer(value).toString('base64');
	}

	fromBase64(value) {
		if (_lodash2.default.isEmpty(value)) {
			return null;
		}

		return new Buffer(value, 'base64').toString('ascii');
	}
}
exports.Crud = Crud;