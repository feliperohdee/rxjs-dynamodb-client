const _ = require('lodash');
const cuid = require('cuid');

module.exports = class Crud {
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
			after,
			before,
			consistent,
			desc,
			indexName,
			limit,
			prefix = true,
			resume,
			select,
			withCursor = false
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

			expression += _.isString(localIndex) && prefix ? ` AND begins_with(#index, :index)` : ` AND #index = :index`;
		} else if (sort) {
			request.addPlaceholderName({
					sort: sortAttr
				})
				.addPlaceholderValue({
					sort
				});

			expression += _.isString(sort) && prefix ? ` AND begins_with(#sort, :sort)` : ` AND #sort = :sort`;
		}

		if (indexName) {
			request
				.index(indexName);
		}

		if (before) {
			if (before !== 'last') {
				request.resume(JSON.parse(this.fromBase64(before)))
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

		if (_.isFunction(itemSelector)) {
			items = itemSelector(items, request);
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

				if (before) {
					items = desc ? items : _.reverse(items);
					[queryStats.before, queryStats.after] = [queryStats.after, queryStats.before];
				}

				if (after) {
					items = !desc ? items : _.reverse(items);
				}

				if (withCursor) {
					items = _.map(items, item => {
						item._cursor = this.toBase64(JSON.stringify(request.getIndexedAttributes(item)));

						return item;
					});
				}

				return {
					items,
					stats: _.extend({}, queryStats, {
						before: queryStats.before ? this.toBase64(JSON.stringify(queryStats.before)) : null,
						after: queryStats.after ? this.toBase64(JSON.stringify(queryStats.after)) : null
					})
				}
			}, {});
	}

	get(args, hook = false) {
		const {
			consistent,
			select
		} = args;

		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (consistent) {
			request.consistent();
		}

		if (select) {
			request.select(select);
		}

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				partition,
				sort
			} : {
				request,
				partition
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.get.apply(request, hookArgs || [sortAttr ? {
			[partitionAttr]: partition,
			[sortAttr]: sort
		} : {
			[partitionAttr]: partition
		}]);
	}

	insert(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr] || cuid();

		let request = this.request;
		let hookArgs;

		if (sortAttr) {
			args[sortAttr] = sort;
		}

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				partition,
				sort,
				args
			} : {
				request,
				partition,
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
			const hookParams = sortAttr ? {
				request,
				partition,
				sort,
				args
			} : {
				request,
				partition,
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
			const hookParams = sortAttr ? {
				request,
				partition,
				sort,
				args
			} : {
				request,
				partition,
				args
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.insertOrUpdate.apply(request, hookArgs || [args]);
	}

	update(args, returns = 'ALL_NEW', hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				partition,
				sort,
				args
			} : {
				request,
				partition,
				args
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [args]);
	}

	updatePrimaryKeys(args, primaryKeys = {}) {
		const request = this.request;

		return this.delete(args)
			.mergeMap(response => {
				const primaryAttrs = _.pick(primaryKeys, [
					this.partitionAttr,
					this.sortAttr
				]);

				response = _.omit(response, ['updatedAt']);

				return request.insert(_.extend({}, response, primaryAttrs), false, true);
			});
	}

	delete(args, hook = false) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let request = this.request;
		let hookArgs;

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				partition,
				sort
			} : {
				request,
				partition
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return('ALL_OLD')
			.delete.apply(request, hookArgs || [sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
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

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, value, key) => {
			return reduction += `${request.expHelper.appendList(key, value, prepend)}, `;
		}, '');

		expression += request.expHelper.timestamp();

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				expression,
				partition,
				sort,
				attributes
			} : {
				request,
				expression,
				partition,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [expression, sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
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
			}, '')
			.slice(0, -2);

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				expression,
				partition,
				sort,
				attributes
			} : {
				request,
				expression,
				partition,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [expression, sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
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

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, indexes, attribute) => {
			request.addPlaceholderName(attribute);

			_.each(indexes, (fields, index) => {
				if (_.isObject(fields)) {
					_.each(fields, (value, key) => {
						const placeholderKey = cuid.slug();

						request.addPlaceholderName({
								[placeholderKey]: key
							})
							.addPlaceholderValue({
								[placeholderKey]: value
							});

						reduction += `#${attribute}[${index}].#${placeholderKey} = :${placeholderKey}, `;
					});
				} else {
					const placeholderKey = cuid.slug();
					
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
			const hookParams = sortAttr ? {
				request,
				expression,
				partition,
				sort,
				attributes
			} : {
				request,
				expression,
				partition,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [expression, sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
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
			}, [])
			.join();

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				expression,
				partition,
				sort,
				attributes
			} : {
				request,
				expression,
				partition,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [expression, sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
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
			}, [])
			.join();

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				expression,
				partition,
				sort,
				attributes
			} : {
				request,
				expression,
				partition,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [expression, sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
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

		const attributes = _.omit(args, [partitionAttr, sortAttr]);

		expression += _.reduce(attributes, (reduction, value, key) => {
				request.addPlaceholderName({
					[key]: key
				});

				return reduction += `#${key}, `;
			}, '')
			.slice(0, -2);

		expression += ` SET ${request.expHelper.timestamp()}`;

		if (hook) {
			const hookParams = sortAttr ? {
				request,
				expression,
				partition,
				sort,
				attributes
			} : {
				request,
				expression,
				partition,
				attributes
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.return(returns)
			.update.apply(request, hookArgs || [expression, sortAttr ? {
				[partitionAttr]: partition,
				[sortAttr]: sort
			} : {
				[partitionAttr]: partition
			}]);
	}

	multiGet(args, hook = false) {
		let {
			items,
			select
		} = args;

		if (!_.isArray(items)) {
			items = [items];
		}

		items = _.map(items, item => _.pick(item, [
			this.partitionAttr,
			this.sortAttr
		]));

		const request = this.request;
		let hookArgs;

		if (select) {
			request.select(select);
		}

		if (hook) {
			const hookParams = {
				request,
				items
			};

			hookArgs = hook.call(this, hookParams);
		}

		return request.batchGet.apply(request, hookArgs || [items]);
	}

	clear(args) {
		const partitionAttr = this.partitionAttr;
		const sortAttr = this.sortAttr;
		const partition = args[partitionAttr];
		const sort = args[sortAttr];

		let expression = '#partition = :partition';
		let request = this.request;

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

		return request.limit(1000)
			.query(expression)
			.toArray()
			.mergeMap(response => this.request.batchWrite(response));
	}

	toBase64(value) {
		if (_.isEmpty(value)) {
			return null;
		}

		return new Buffer(value)
			.toString('base64');
	}

	fromBase64(value) {
		if (_.isEmpty(value)) {
			return null;
		}

		return new Buffer(value, 'base64')
			.toString('ascii');
	}
}
