const _ = require('lodash');

const ExpressionsHelper = require('./ExpressionsHelper');
const Util = require('./Util');
const rx = require('./rx');
const {
    Select,
    ReturnValues,
    ConsumedCapacity
} = require('./constants');

const DEFAULT_LIMIT = 50;

module.exports = class Request {
    constructor(client) {
        this.client = client;
        this.expHelper = new ExpressionsHelper(this);

        this.conditionExpression = null;
        this.consistentRead = false;
        this.exclusiveStartKey = null;
        this.expressionAttributeNames = null;
        this.expressionAttributeValues = null;
        this.filterExpression = null;
        this.indexName = null;
        this.keyConditionExpression = null;
        this.queryLimit = DEFAULT_LIMIT;
        this.projectionSelect = 'ALL_ATTRIBUTES';
        this.projectionExpression;
        this.returnConsumedCapacity = ConsumedCapacity.TOTAL;
        this.returnValues = ReturnValues.NONE;
        this.scanIndexForward = true;
        this.primaryKeys = null;
        this.indexes = null;
        this.tableName = null;
        this.updateExpression = null;
        this.queryStats = {};
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

            const index = this.indexes[this.indexName] || {};

            // is global index
            if (index.partition !== partition) {
                return index.partition || null;
            }
        }

        return null;
    }

    get globalIndexSortAttr() {
        if (this.indexes && this.indexName) {
            const {
                partition = null
            } = this.primaryKeys;

            const index = this.indexes[this.indexName] || {};

            // is global index
            if (index.partition !== partition && index.sort) {
                return index.sort;
            }
        }

        return null;
    }

    get localIndexSortAttr() {
        if (this.indexes && this.indexName) {
            const {
                partition = null
            } = this.primaryKeys;

            const index = this.indexes[this.indexName] || {};

            // is local index
            if (index.partition === partition && index.sort) {
                return index.sort;
            }
        }

        return null;
    }

    routeCall(method, args) {
        return new rx.Observable(subscriber => {
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

                return reduction.concat(`#${token}_${index}`);
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

    query(args = {}) {
        this.keyConditionExpression = this._queryExpression(args);

        return this._queryOperation()
            .pipe(
                rx.expand(response => {
                    const {
                        after,
                        count
                    } = response;

                    const lessThanLimit = !!(after && (count < this.queryLimit));

                    if (lessThanLimit) {
                        return this._queryOperation(response);
                    }

                    return rx.empty();
                }),
                rx.mergeMap(response => {
                    const {
                        after = null,
                        before = null,
                        consumedCapacity = 0,
                        count = 0,
                        items = [],
                        iteractions = 1,
                        scannedCount = 0
                    } = response;

                    this.queryStats = {
                        after: after ? this.util.normalizeItem(after) : null,
                        before: this.queryStats.before || (before ? this.util.normalizeItem(before) : null),
                        consumedCapacity: (this.queryStats.consumedCapacity || 0) + consumedCapacity,
                        count: (this.queryStats.count || 0) + count,
                        iteractions: (this.queryStats.iteractions || 0) + iteractions,
                        scannedCount: (this.queryStats.scannedCount || 0) + scannedCount
                    };

                    return rx.from(items);
                }),
                rx.map(response => {
                    return this.util.normalizeItem(response);
                }),
                rx.take(this.queryLimit)
            );
    }

    queryScan(args = {}, scan) {
        this.keyConditionExpression = this._queryExpression(args);

        const queryOperation = (after, index = 0) => {
            return this._queryOperation({
                    after
                })
                .pipe(
                    rx.mergeMap(({
                        after,
                        items
                    }) => {
                        const response = this.util.normalizeList(items);

                        if (_.isFunction(scan)) {
                            return scan(response, index)
                                .pipe(
                                    rx.map(response => {
                                        if (_.hasIn(response, 'after') && _.hasIn(response, 'response')) {
                                            return response;
                                        }

                                        return {
                                            after,
                                            response
                                        };
                                    })
                                );
                        }

                        return rx.of({
                            after,
                            response
                        });
                    })
                );
        };

        return queryOperation()
            .pipe(
                rx.expand(({
                    after,
                    response
                }, index) => {
                    index += 1;

                    if (after) {
                        return queryOperation(after, index);
                    }

                    return rx.empty();
                }),
                rx.pluck('response')
            );
    }

    _queryExpression(args) {
        if (_.isString(args)) {
            return args;
        }

        const primaryAttrs = _.pick(args, [
            this.globalIndexPartitionAttr || this.partitionAttr,
            this.globalIndexSortAttr || this.sortAttr,
            this.localIndexSortAttr
        ]);

        return _.reduce(primaryAttrs, (reduction, value, key) => {
                this.addPlaceholderName(key);
                this.addPlaceholderValue({
                    [key]: value
                });

                return reduction.concat(`#${key} = :${key}`);
            }, [])
            .join(' AND ');
    }

    _queryOperation(response = {}) {
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
            .pipe(
                rx.map(response => {
                    const result = {
                        after: response.LastEvaluatedKey,
                        before: this._queryBeforeKey,
                        consumedCapacity: (response.ConsumedCapacity && response.ConsumedCapacity.CapacityUnits) || 0,
                        count: response.Count,
                        items: response.Items,
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
                })
            );
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
            .pipe(
                rx.map(response => response.Item ? this.util.normalizeItem(response.Item) : null)
            );
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

        this.queryLimit = (_.isFinite(limit) && limit > 0) ? limit : DEFAULT_LIMIT;

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

    insert(item, replace = false, overrideTimestamp = false) {
        // cant replace, has and sort can't exists
        if (!replace) {
            this.condition(this.expHelper.attrNotExists(this.partitionAttr));
        }

        // append timestamp
        const now = _.now();

        item = {
            ...item,
            createdAt: (overrideTimestamp && item.createdAt) || now,
            updatedAt: (overrideTimestamp && item.updatedAt) || now
        };

        return this.routeCall('putItem', {
                ConditionExpression: this.conditionExpression,
                ExpressionAttributeNames: this.expressionAttributeNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                Item: this.util.anormalizeItem(item),
                ReturnConsumedCapacity: this.returnConsumedCapacity,
                TableName: this.tableName
            })
            .pipe(
                rx.map(() => {
                    return _.reduce(item, (reduction, value, key) => {
                        if (value && value instanceof Util) {
                            value = this.util.normalizeValue(value.data);
                        }

                        reduction[key] = value;

                        return reduction;
                    }, {});
                })
            );
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
            return rx.throwError(new Error('Where statement might be provided'));
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
            item = _.omit(item, primaryKeys);

            if (!_.isEmpty(item)) {
                this.updateExpression = `SET ${this.expHelper.update(item)}`;
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
            .pipe(
                rx.map(response => this.util.normalizeItem(response.Attributes))
            );
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
            .pipe(
                rx.map(response => response.Attributes ? this.util.normalizeItem(response.Attributes) : null)
            );
    }

    batchGet(items, chunk = 100) {
        const chunks = _.chunk(items, chunk);
        const transactions = _.map(chunks, args => {
            return this._batchGetOperation(items);
        });

        return rx.concat.apply(this, transactions)
            .pipe(
                rx.mergeMap(response => rx.from(response)),
                rx.map(response => this.util.normalizeItem(response))
            );
    }

    batchGetScan(items, scan, chunk = 100, concurrency = 1) {
        const chunks = _.chunk(items, chunk);

        return rx.from(chunks)
            .pipe(
                rx.mergeMap((chunk, index) => {
                    return this._batchGetOperation(chunk)
                        .pipe(
                            rx.mergeMap(response => {
                                response = this.util.normalizeList(response);

                                if (_.isFunction(scan)) {
                                    return scan(response, index);
                                }

                                return rx.of(response);
                            })
                        );
                }, concurrency)
            );
    }

    _batchGetOperation(items) {
        return this.routeCall('batchGetItem', {
                RequestItems: {
                    [this.tableName]: {
                        Keys: _.map(items, item => {
                            return this.util.anormalizeItem(item);
                        }),
                        ConsistentRead: this.consistentRead,
                        ExpressionAttributeNames: this.expressionAttributeNames,
                        ProjectionExpression: this.projectionExpression
                    }
                },
                ReturnConsumedCapacity: this.returnConsumedCapacity
            })
            .pipe(
                rx.map(response => _.get(response, `Responses.${this.tableName}`, null)),
                rx.filter(_.isObject)
            );
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

                return reduction.concat({
                    DeleteRequest: {
                        Key: this.util.anormalizeItem(primaryAttrs)
                    }
                });
            }, args);
        }

        if (toInsert) {
            args = _.reduce(toInsert, (reduction, item) => {
                return reduction.concat({
                    PutRequest: {
                        Item: this.util.anormalizeItem({
                            ...item,
                            createdAt: now,
                            updatedAt: now
                        })
                    }
                });
            }, args);
        }

        const argsChunks = _.chunk(args, 25);
        const transactions = _.map(argsChunks, args => this.routeCall('batchWriteItem', {
            RequestItems: {
                [this.tableName]: args
            }
        }));

        return rx.concat.apply(this, transactions)
            .pipe(
                rx.reduce((reduction, value) => {
                    return {
                        ...reduction,
                        ...value
                    };
                }, {})
            );
    }
};