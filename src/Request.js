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

		if (this.indexes && this.indexName) {
			const {
				partition: indexPartition = null
			} = this.indexes[this.indexName] || {};

			// is global index
			if (indexPartition !== partition) {
				return indexPartition;
			}
		}

		return partition;
	}

	get sortAttr() {
		const {
			partition = null,
			sort = null
		} = this.primaryKeys;

		if (this.indexes && this.indexName) {
			const {
				partition: indexPartition = null,
				sort: indexSort = null
			} = this.indexes[this.indexName] || {};

			// is global index
			if (indexPartition !== partition) {
				return indexSort;
			}
		}

		return sort;
	}

	get localIndexAttr() {
		if (this.indexes && this.indexName) {
			const {
				partition,
				sort
			} = this.indexes[this.indexName] || {};

			// is local index
			if (partition === this.partitionAttr && sort) {
				return sort;
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

		// ensure minimals to compose a possible lastKey
		select += `,${this.partitionAttr}`;

		if (this.sortAttr) {
			select += `,${this.sortAttr}`;
		}

		if (this.localIndexAttr) {
			select += `,${this.localIndexAttr}`;
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
			const primaryAttrs = _.pick(queryData, [this.partitionAttr, this.sortAttr, this.localIndexAttr]);

			this.keyConditionExpression = _.reduce(primaryAttrs, (reduction, value, key) => {
				this.addPlaceholderName(key);
				this.addPlaceholderValue({
					[key]: value
				});

				reduction.push(`#${key} = :${key}`);

				return reduction;
			}, []).join(' AND ');
		}

		const queryOperation = (response = {}) => {
			const {
				lastKey
			} = response;

			if (lastKey) {
				this.exclusiveStartKey = lastKey;
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
					Limit: this.queryLimit && this.filterExpression ? null : this.queryLimit,
					ProjectionExpression: this.projectionExpression,
					ReturnConsumedCapacity: this.returnConsumedCapacity,
					ScanIndexForward: this.scanIndexForward,
					Select: this.projectionSelect,
					TableName: this.tableName
				})
				.map(response => ({
					items: response.Items,
					lastKey: response.LastEvaluatedKey,
					count: response.Count,
					scannedCount: response.ScannedCount
				}));
		};

		return queryOperation()
			.expand(response => {
				const {
					lastKey,
					count
				} = response;

				const memoryOverhead = !!(lastKey && !this.queryLimit);
				const lessThanLimit = !!(lastKey && this.queryLimit && (count < this.queryLimit));

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
					lastKey = null,
					iteractions = 1
				} = response;

				this.queryStats = {
					lastKey: lastKey ? this.util.normalizeItem(lastKey) : null,
					count: (this.queryStats.count || 0) + count,
					scannedCount: (this.queryStats.scannedCount || 0) + scannedCount,
					iteractions: (this.queryStats.iteractions || 0) + iteractions
				};

				// when filterExpression we have no limits
				// if filteres response's length is greater than query limit we need emulate an optimistic lastKey and count
				if (this.filterExpression && this.queryStats.count > this.queryLimit) {
					const lastItem = this.util.normalizeItem(_.nth(items, this.queryLimit - 1));

					this.queryStats.lastKey = _.pick(lastItem, [this.partitionAttr, this.sortAttr, this.localIndexAttr]);
					this.queryStats.count = this.queryLimit;
				}

				return Observable.from(items);
			})
			.map(::this.util.normalizeItem)
			.take(this.queryLimit);
	}

	get(item) {
		const primaryAttrs = _.pick(item, [this.partitionAttr, this.sortAttr]);

		return this.routeCall('getItem', {
				ExpressionAttributeNames: this.expressionAttributeNames,
				Key: this.util.anormalizeItem(primaryAttrs),
				ProjectionExpression: this.projectionExpression,
				ReturnConsumedCapacity: this.returnConsumedCapacity,
				TableName: this.tableName
			})
			.map(response => this.util.normalizeItem(response.Item || {}));
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
		item.createdAt = now;
		item.updatedAt = now;

		return this.routeCall('putItem', {
			ConditionExpression: this.conditionExpression,
			ExpressionAttributeNames: this.expressionAttributeNames,
			ExpressionAttributeValues: this.expressionAttributeValues,
			Item: this.util.anormalizeItem(item),
			ReturnConsumedCapacity: this.returnConsumedCapacity,
			TableName: this.tableName
		}).mapTo(item);
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

		const primaryKeys = [this.partitionAttr, this.sortAttr];
		const primaryAttrs = _.pick(where, primaryKeys);

		// build expression
		if (_.isString(item)) {
			this.updateExpression = item;
		} else {
			// omit schema keys from expression
			item = _.omit(item, primaryKeys);

			if (!_.isEmpty(item)) {
				this.updateExpression = 'SET ' + _.reduce(item, (reduction, value, key) => {
					this.addPlaceholderName(key);
					this.addPlaceholderValue({
						[key]: value
					});

					reduction.push(`#${key} = :${key}`);

					return reduction;
				}, []).join(', ');

				// append timestamp
				this.updateExpression += `, ${this.expHelper.timestamp()}`;
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
		const primaryAttrs = _.pick(item, [this.partitionAttr, this.sortAttr]);

		return this.routeCall('deleteItem', {
				ConditionExpression: this.conditionExpression,
				ExpressionAttributeNames: this.expressionAttributeNames,
				ExpressionAttributeValues: this.expressionAttributeValues,
				Key: this.util.anormalizeItem(primaryAttrs),
				ReturnConsumedCapacity: this.returnConsumedCapacity,
				ReturnValues: this.returnValues,
				TableName: this.tableName
			})
			.map(response => this.util.normalizeItem(response.Attributes || {}));
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
				const primaryAttrs = _.pick(item, [this.partitionAttr, this.sortAttr]);

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
				item.createdAt = now;
				item.updatedAt = now;

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
