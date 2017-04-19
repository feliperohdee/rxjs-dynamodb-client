import _ from 'lodash';
import {
	Observable
} from 'rxjs';

import {
	Util
} from './Util';
import {
	Select,
	ReturnValues,
	ConsumedCapacity
} from './constants';
import {
	ExpressionsHelper
} from './ExpressionsHelper';

export class Request {
	conditionExpression = null;
	consistentRead = false;
	exclusiveStartKey = null;
	expressionAttributeNames = null;
	expressionAttributeValues = null;
	filterExpression = null;
	indexName = null;
	keyConditionExpression = null;
	queryLimit = 50;
	projectionSelect = 'ALL_ATTRIBUTES';
	projectionExpression;
	returnConsumedCapacity = ConsumedCapacity.TOTAL;
	returnValues = ReturnValues.NONE;
	scanIndexForward = true;
	primaryKeys = null;
	indexes = null;
	tableName = null;
	updateExpression = null;
	queryStats = {};

	constructor(client) {
		this.client = client;
		this.expHelper = new ExpressionsHelper(this);
	}

	get util() {
		return new Util();
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

	get globalIndexPartitionAttr() {
		if (this.indexes && this.indexName) {
			const {
				partition = null
			} = this.primaryKeys;

			const {
				partition: indexPartition = null
			} = this.indexes[this.indexName] || {};

			// is global index
			if (indexPartition !== partition) {
				return indexPartition;
			}
		}

		return null;
	}

	get globalIndexSortAttr() {
		if (this.indexes && this.indexName) {
			const {
				partition = null
			} = this.primaryKeys;

			const {
				partition: indexPartition = null,
				sort: indexSort = null
			} = this.indexes[this.indexName] || {};

			// is global index
			if (indexPartition !== partition && indexSort) {
				return indexSort;
			}
		}

		return null;
	}

	get localIndexSortAttr() {
		if (this.indexes && this.indexName) {
			const {
				partition = null
			} = this.primaryKeys;

			const {
				partition: indexPartition = null,
				sort: indexSort = null
			} = this.indexes[this.indexName] || {};

			// is local index
			if (indexPartition === partition && indexSort) {
				return indexSort;
			}
		}

		return null;
	}

	routeCall(method, args) {
		return Observable.create(subscriber => {
			// console.log();
			// console.log('routeCall:', method);
			// console.log(args);
			// console.log();

			this.client[method](args, (err, data) => {
				if (err) {
					return subscriber.error(err);
				}

				subscriber.next(data);
				subscriber.complete();
			});
		});
	}

	describe() {
		return this.routeCall('describeTable', {
			TableName: this.tableName
		});
	}

	table(name, schema = {}) {
		const {
			primaryKeys,
			indexes = {}
		} = schema;

		this.tableName = name;
		this.primaryKeys = primaryKeys;
		this.indexes = indexes;

		return this;
	}

	return (returnValues) {
		this.returnValues = returnValues;

		return this;
	}

	consumedCapacity(value) {
		this.returnConsumedCapacity = value;

		return this;
	}

	addPlaceholderName(name) {
		if (!this.expressionAttributeNames) {
			this.expressionAttributeNames = {};
		}

		if (_.isString(name)) {
			name = [name];
		}

		if (_.isArray(name)) {
			_.each(name, value => _.set(this.expressionAttributeNames, `#${value}`, value));
		} else {
			_.each(name, (value, key) => _.set(this.expressionAttributeNames, `#${key}`, value));
		}

		return this;
	}

	addPlaceholderValue(value) {
		if (!this.expressionAttributeValues) {
			this.expressionAttributeValues = {};
		}

		if (!_.isObject(value)) {
			value = [value];
		}

		if (_.isArray(value)) {
			_.each(value, value => _.set(this.expressionAttributeValues, `:${value}`, this.util.anormalizeValue(value)));
		} else {
			_.each(value, (value, key) => _.set(this.expressionAttributeValues, `:${key}`, this.util.anormalizeValue(value)));
		}

		return this;
	}

	select(select) {
		if (_.isNumber(select)) {
			switch (select) {
				case Select.ALL_ATTRIBUTES:
					this.projectionSelect = 'ALL_ATTRIBUTES';
					break;
				case Select.ALL_PROJECTED_ATTRIBUTES:
					this.projectionSelect = 'ALL_PROJECTED_ATTRIBUTES';
					break;
				case Select.SPECIFIC_ATTRIBUTES:
					this.projectionSelect = 'SPECIFIC_ATTRIBUTES';
					break;
				case Select.COUNT:
					this.projectionSelect = 'COUNT';
					break;
			}

			return this;
		}

		// ensure minimals to compose a possible afterKey
		select += `,${this.partitionAttr}`;

		if (this.sortAttr) {
			select += `,${this.sortAttr}`;
		}

		if (this.globalIndexPartitionAttr) {
			select += `,${this.globalIndexPartitionAttr}`;
		}

		if (this.globalIndexSortAttr) {
			select += `,${this.globalIndexSortAttr}`;
		}

		if (this.localIndexSortAttr) {
			select += `,${this.localIndexSortAttr}`;
		}

		// replace select tokens by placeholder attributes
		const tokens = _.uniq(select.match(/([a-zA-Z_0-9])+/g));
		const selectString = _.reduce(tokens, (reduction, token, index) => {
				this.addPlaceholderName({
					[`${token}_${index}`]: token
				});

				reduction.push(`#${token}_${index}`);

				return reduction;
			}, [])
			.join();

		this.projectionSelect = 'SPECIFIC_ATTRIBUTES';
		this.projectionExpression = selectString;

		return this;
	}

	consistent() {
		this.consistentRead = true;

		return this;
	}

	query(queryData) {
		// build expression
		if (_.isString(queryData)) {
			this.keyConditionExpression = queryData;
		} else {
			const primaryAttrs = _.pick(queryData, [
				this.globalIndexPartitionAttr || this.partitionAttr,
				this.globalIndexSortAttr || this.sortAttr,
				this.localIndexSortAttr
			]);

			this.keyConditionExpression = _.reduce(primaryAttrs, (reduction, value, key) => {
					this.addPlaceholderName(key);
					this.addPlaceholderValue({
						[key]: value
					});

					reduction.push(`#${key} = :${key}`);

					return reduction;
				}, [])
				.join(' AND ');
		}

		const queryOperation = (response = {}) => {
			const {
				after
			} = response;

			if (after) {
				this.exclusiveStartKey = after;
			}

			return this.routeCall('query', {
					ConsistentRead: this.consistentRead,
					ExclusiveStartKey: this.exclusiveStartKey,
					ExpressionAttributeNames: this.expressionAttributeNames,
					ExpressionAttributeValues: this.expressionAttributeValues,
					FilterExpression: this.filterExpression,
					IndexName: this.indexName,
					KeyConditionExpression: this.keyConditionExpression,
					// when has filter expression, increase limit to compensate filter deviation
					Limit: this.queryLimit && this.filterExpression ? this.queryLimit * 4 : this.queryLimit + 1,
					ProjectionExpression: this.projectionExpression,
					ReturnConsumedCapacity: this.returnConsumedCapacity,
					ScanIndexForward: this.scanIndexForward,
					Select: this.projectionSelect,
					TableName: this.tableName
				})
				.map(response => {
					const result = {
						items: response.Items,
						after: response.LastEvaluatedKey,
						before: this._queryBeforeKey,
						count: response.Count,
						scannedCount: response.ScannedCount
					};

					// when received more items than queryLimit has after
					if (response.Count > this.queryLimit) {
						const lastItem = _.nth(response.Items, this.queryLimit - 1);

						result.items = _.slice(response.Items, 0, this.queryLimit);
						result.after = lastItem ? this.getIndexedAttributes(lastItem) : null;
						result.count = this.queryLimit;
						result.scannedCount = this.queryLimit;
					}

					// if already paginated, we store first item to allow do "before" queries as lastkey is for "after" queries
					if (!result.before && this.isResumed && result.count) {
						const firstItem = _.first(result.items);

						result.before = this._queryBeforeKey = this.getIndexedAttributes(firstItem);
					}

					return result;
				});
		};

		return queryOperation()
			.expand(response => {
				const {
					after,
					count
				} = response;

				const memoryOverhead = !!(after && !this.queryLimit);
				const lessThanLimit = !!(after && this.queryLimit && (count < this.queryLimit));

				if (memoryOverhead || lessThanLimit) {
					return queryOperation(response);
				}

				return Observable.empty();
			})
			.mergeMap(response => {
				const {
					items,
					count = 0,
					scannedCount = 0,
					before = null,
					after = null,
					iteractions = 1
				} = response;

				this.queryStats = {
					before: this.queryStats.before || (before ? this.util.normalizeItem(before) : null),
					after: after ? this.util.normalizeItem(after) : null,
					count: (this.queryStats.count || 0) + count,
					scannedCount: (this.queryStats.scannedCount || 0) + scannedCount,
					iteractions: (this.queryStats.iteractions || 0) + iteractions
				};

				return Observable.from(items);
			})
			.map(::this.util.normalizeItem)
			.take(this.queryLimit);
	}

	getIndexedAttributes(item) {
		return _.pick(item, [
			this.partitionAttr,
			this.sortAttr,
			this.globalIndexPartitionAttr,
			this.globalIndexSortAttr,
			this.localIndexSortAttr
		]);
	}

	get(item) {
		const primaryAttrs = _.pick(item, [
			this.partitionAttr,
			this.sortAttr
		]);

		return this.routeCall('getItem', {
				ExpressionAttributeNames: this.expressionAttributeNames,
				Key: this.util.anormalizeItem(primaryAttrs),
				ProjectionExpression: this.projectionExpression,
				ReturnConsumedCapacity: this.returnConsumedCapacity,
				TableName: this.tableName
			})
			.map(response => response.Item ? this.util.normalizeItem(response.Item) : null);
	}

	index(index) {
		this.indexName = index;

		return this;
	}

	desc() {
		this.scanIndexForward = false;

		return this;
	}

	limit(limit) {
		limit = parseInt(`${limit}`);

		this.queryLimit = _.isFinite(limit) ? limit : 0;

		return this;
	}

	filter(expression, append = true, condition = 'AND') {
		if (append) {
			expression = this.filterExpression ? `${this.filterExpression} ${condition} ${expression}` : expression;
		}

		this.filterExpression = expression;

		return this;
	}

	resume(args) {
		this.isResumed = true;
		this.exclusiveStartKey = this.util.anormalizeItem(args);

		return this;
	}

	condition(expression, append = true, condition = 'AND') {
		if (append) {
			expression = this.conditionExpression ? `${this.conditionExpression} ${condition} ${expression}` : expression;
		}

		this.conditionExpression = expression;

		return this;
	}

	insert(item, replace = false) {
		// cant replace, has and sort can't exists
		if (!replace) {
			this.condition(this.expHelper.attrNotExists(this.partitionAttr));
		}

		// append timestamp
		const now = _.now();

		item = {
			...item,
			createdAt: now,
			updatedAt: now
		};

		return this.routeCall('putItem', {
				ConditionExpression: this.conditionExpression,
				ExpressionAttributeNames: this.expressionAttributeNames,
				ExpressionAttributeValues: this.expressionAttributeValues,
				Item: this.util.anormalizeItem(item),
				ReturnConsumedCapacity: this.returnConsumedCapacity,
				TableName: this.tableName
			})
			.map(() => {
				return _.reduce(item, (reduction, value, key) => {
					if (value && value instanceof Util) {
						value = this.util.normalizeValue(value.data);
					}

					reduction[key] = value;

					return reduction;
				}, {});
			});
	}

	insertOrReplace(item) {
		return this.insert(item, true);
	}

	insertOrUpdate(item, where) {
		return this.update(item, where, true);
	}

	update(item, where, insert = false) {
		if (!where) {
			where = item;
		}

		if (!_.isObject(where)) {
			return Observable.throw(new Error('Where statement might be provided'));
		}

		const primaryKeys = [
			this.partitionAttr,
			this.sortAttr
		];
		const primaryAttrs = _.pick(where, primaryKeys);

		// build expression
		if (_.isString(item)) {
			this.updateExpression = item;
		} else {
			// omit schema keys from expression
			item = _.omit(item, [
				...primaryKeys
			]);

			if (!_.isEmpty(item)) {
				this.updateExpression = `SET ${this.expHelper.update(item)}`
			}
		}

		// cant insert, partition and sort might exists
		if (!insert) {
			this.condition(this.expHelper.attrExists(this.partitionAttr));
		}

		return this.routeCall('updateItem', {
				ConditionExpression: this.conditionExpression,
				ExpressionAttributeNames: this.expressionAttributeNames,
				ExpressionAttributeValues: this.expressionAttributeValues,
				Key: this.util.anormalizeItem(primaryAttrs),
				ReturnConsumedCapacity: this.returnConsumedCapacity,
				ReturnValues: this.returnValues,
				TableName: this.tableName,
				UpdateExpression: this.updateExpression
			})
			.map(response => this.util.normalizeItem(response.Attributes));
	}

	delete(item) {
		const primaryAttrs = _.pick(item, [
			this.partitionAttr,
			this.sortAttr
		]);

		return this.routeCall('deleteItem', {
				ConditionExpression: this.conditionExpression,
				ExpressionAttributeNames: this.expressionAttributeNames,
				ExpressionAttributeValues: this.expressionAttributeValues,
				Key: this.util.anormalizeItem(primaryAttrs),
				ReturnConsumedCapacity: this.returnConsumedCapacity,
				ReturnValues: this.returnValues,
				TableName: this.tableName
			})
			.map(response => response.Attributes ? this.util.normalizeItem(response.Attributes) : null);
	}

	batchGet(items) {
		let args = _.reduce(items, (reduction, item) => {
			reduction.push(this.util.anormalizeItem(item));

			return reduction;
		}, []);

		const argsChunks = _.chunk(args, 100);
		const transactions = _.map(argsChunks, args => this.routeCall('batchGetItem', {
			RequestItems: {
				[this.tableName]: {
					Keys: args,
					ConsistentRead: this.consistentRead,
					ExpressionAttributeNames: this.expressionAttributeNames,
					ProjectionExpression: this.projectionExpression,
				}
			},
			ReturnConsumedCapacity: this.returnConsumedCapacity
		}));

		return Observable.concat.apply(this, transactions)
			.map(response => _.get(response, `Responses.${this.tableName}`, null))
			.filter(::_.isObject)
			.mergeMap(response => Observable.from(response))
			.map(::this.util.normalizeItem);
	}

	batchWrite(toDelete, toInsert) {
		const now = _.now();
		let args = [];

		if (toDelete) {
			args = _.reduce(toDelete, (reduction, item) => {
				const primaryAttrs = _.pick(item, [
					this.partitionAttr,
					this.sortAttr
				]);

				reduction.push({
					DeleteRequest: {
						Key: this.util.anormalizeItem(primaryAttrs)
					}
				});

				return reduction;
			}, args);
		}

		if (toInsert) {
			args = _.reduce(toInsert, (reduction, item) => {
				// append timestamp
				item = {
					...item,
					createdAt: now,
					updatedAt: now
				};

				reduction.push({
					PutRequest: {
						Item: this.util.anormalizeItem(item)
					}
				});

				return reduction;
			}, args);
		}

		const argsChunks = _.chunk(args, 25);
		const transactions = _.map(argsChunks, args => this.routeCall('batchWriteItem', {
			RequestItems: {
				[this.tableName]: args
			}
		}));

		return Observable.concat.apply(this, transactions)
			.reduce((reduction, value) => ({
				...reduction,
				...value
			}), {});
	}
}
