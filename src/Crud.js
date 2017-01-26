import _ from 'lodash';
import cuid from 'cuid';

export class Crud {
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
			})
			.addPlaceholderValue({
				partition
			});

		if (localIndexSortAttr && localIndex) {
			request
				.addPlaceholderName({
					index: localIndexSortAttr
				})
				.addPlaceholderValue({
					index: localIndex
				});

			expression += _.isNumber(localIndex) ? ` AND #index = :index` : ` AND begins_with(#index, :index)`;
		} else if (sort) {
			request.addPlaceholderName({
					sort: sortAttr
				})
				.addPlaceholderValue({
					sort
				});

			expression += _.isNumber(sort) ? ` AND #sort = :sort` : ` AND begins_with(#sort, :sort)`;
		}

		if (indexName) {
			request
				.index(indexName);
		}

		if (resume) {
			request.resume(JSON.parse(this.fromBase64(resume)));
		}

		if (after) {
			request.resume(JSON.parse(this.fromBase64(after)));
		}

		if (desc === true) {
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

		let items = request
			.query.apply(request, hookArgs || [expression]);

		if (_.isFunction(itemSelector)) {
			items = itemSelector(items);
		}

		if (_.isFunction(customReducer)) {
			return customReducer(items);
		}

		return items
			.toArray()
			.map(items => {
				const {
					queryStats
				} = request;

				return {
					items,
					stats: {
						...queryStats,
						firstKey: queryStats.firstKey ? this.toBase64(JSON.stringify(queryStats.firstKey)) : null,
						lastKey: queryStats.lastKey ? this.toBase64(JSON.stringify(queryStats.lastKey)) : null
					}
				}
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

		return request
			.get.apply(request, hookArgs || [{
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	insert(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr] || cuid();

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

		return request
			.insert.apply(request, hookArgs || [args]);

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

		return request
			.insertOrReplace.apply(request, hookArgs || [args]);

	}

	insertOrUpdate(args, hook = false) {
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

		return request
			.return('ALL_NEW')
			.insertOrUpdate.apply(request, hookArgs || [args]);
	}

	update(args, hook = false) {
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [args]);
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

		return request
			.return('ALL_OLD')
			.delete.apply(request, hookArgs || [{
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	prependToList(args, hook = false) {
		return this.appendToList(args, true, hook);
	}

	appendToList(args, prepend = false, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'SET ';
		let hookArgs;

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, value, key) => {
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [expression, {
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	removeFromList(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'REMOVE ';
		let hookArgs;

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, values, key) => {
			if (!_.isArray(values)) {
				values = [values];
			}

			request.addPlaceholderName({
				[key]: key
			});

			return _.reduce(values, (reduction, value) => {
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [expression, {
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	updateAtList(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'SET ';
		let hookArgs;

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, indexes, attribute) => {
			request.addPlaceholderName(attribute);

			_.each(indexes, (fields, index) => {
				const placeholderKey = cuid.slug();

				if (_.isObject(fields)) {
					_.each(fields, (value, key) => {
						request.addPlaceholderName({
								[placeholderKey]: key
							})
							.addPlaceholderValue({
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [expression, {
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);

	}

	addToSet(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'ADD ';
		let hookArgs;

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, value, key) => {
			if (!_.isArray(value)) {
				value = [value];
			}

			const isStringSet = _.every(value, _.isString);
			const isNumberSet = _.every(value, _.isNumber);

			if (isStringSet) {
				value = request.util.raw({
					SS: value
				});
			} else if (isNumberSet) {
				value = request.util.raw({
					NS: value.map(_.toString)
				});
			} else {
				return reduction;
			}

			request
				.addPlaceholderName({
					[key]: key
				})
				.addPlaceholderValue({
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [expression, {
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	removeFromSet(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'DELETE ';
		let hookArgs;

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, value, key) => {
			if (!_.isArray(value)) {
				value = [value];
			}

			const isStringSet = _.every(value, _.isString);
			const isNumberSet = _.every(value, _.isNumber);

			if (isStringSet) {
				value = request.util.raw({
					SS: value
				});
			} else if (isNumberSet) {
				value = request.util.raw({
					NS: value.map(_.toString)
				});
			} else {
				return reduction;
			}

			request
				.addPlaceholderName({
					[key]: key
				})
				.addPlaceholderValue({
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [expression, {
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	removeAttributes(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let expression = 'REMOVE ';
		let hookArgs;

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, value, key) => {
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

		return request
			.return('ALL_NEW')
			.update.apply(request, hookArgs || [expression, {
				[partitionAttr]: partition,
				[sortAttr]: sort
			}]);
	}

	clear(args) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let expression = '#partition = :partition';
		const request = this.request;

		if (partition) {
			request
				.addPlaceholderName({
					partition: partitionAttr
				})
				.addPlaceholderValue({
					partition
				});
		}

		if (sort) {
			request
				.addPlaceholderName({
					sort: sortAttr
				})
				.addPlaceholderValue({
					sort
				});

			expression += ` AND begins_with(#sort, :sort)`
		}

		return request
			.limit(1000)
			.query(expression)
			.toArray()
			.mergeMap(::this.request.batchWrite);
	}

	toBase64(value) {
		if (_.isEmpty(value)) {
			return null;
		}

		return new Buffer(value).toString('base64');
	}

	fromBase64(value) {
		if (_.isEmpty(value)) {
			return null;
		}

		return new Buffer(value, 'base64').toString('ascii');
	}
}
