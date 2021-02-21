const cuid = require('cuid');

const _ = require('./lodash');
const rx = require('./rx');

module.exports = class Crud {
    constructor(name, schema, deps = {}) {
        if (!deps.dynamodb) {
            throw new Error('no dynamodb client provided.');
        }

        const {
            primaryKeys,
            indexes = {}
        } = schema;

        this.dynamodb = deps.dynamodb;
        this.tableName = name;
        this.tableSchema = schema;
        this.primaryKeys = primaryKeys;
        this.indexes = indexes;
        this.lastRequest = null;
    }

    get request() {
        const request = this.dynamodb.table(this.tableName, this.tableSchema);

        return this.lastRequest = request;
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

            const index = this.indexes[indexName] || {};

            // is global index
            if (index.partition !== partition) {
                return index.partition || null;
            }
        }

        return null;
    }

    globalIndexSortAttr(indexName) {
        if (this.indexes && indexName) {
            const {
                partition = null
            } = this.primaryKeys;

            const index = this.indexes[indexName] || {};

            // is global index
            if (index.partition !== partition && index.sort) {
                return index.sort;
            }
        }

        return null;
    }

    localIndexSortAttr(indexName) {
        if (this.indexes && indexName) {
            const {
                partition = null
            } = this.primaryKeys;

            const index = this.indexes[indexName] || {};

            // is local index
            if (index.partition === partition && index.sort) {
                return index.sort;
            }
        }

        return null;
    }

    fetch(args, hook = false, itemSelector = null, reducer = null) {
        const {
            after,
            before,
            consistent,
            desc,
            indexName,
            limit,
            prefix = true,
            resume,
            select
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
            request.addPlaceholderName({
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

        if (_.isFunction(itemSelector)) {
            items = itemSelector(items, request);
        }

        if (_.isFunction(reducer)) {
            return reducer(items);
        }

        return items.pipe(
            rx.toArray(),
            rx.map(items => {
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

                return {
                    ...queryStats,
                    items,
                    before: queryStats.before ? this.toBase64(JSON.stringify(queryStats.before)) : null,
                    after: queryStats.after ? this.toBase64(JSON.stringify(queryStats.after)) : null
                };
            })
        );
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
            .pipe(
                rx.mergeMap(response => {
                    const primaryAttrs = _.pick(primaryKeys, [
                        this.partitionAttr,
                        this.sortAttr
                    ]);

                    response = _.omit(response, ['updatedAt']);

                    return request.insert({
                        ...response,
                        ...primaryAttrs
                    }, false, true);
                })
            );
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

    appendToList(args, create = false, returns = 'ALL_NEW', hook = false) {
        return this.addToList(args, create, false, returns, hook);
    }

    prependToList(args, create = false, returns = 'ALL_NEW', hook = false) {
        return this.addToList(args, create, true, returns, hook);
    }

    addToList(args, create = false, prepend = false, returns = 'ALL_NEW', hook = false) {
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

        return request.return(returns)[create ? 'insertOrUpdate' : 'update'].apply(request, hookArgs || [expression, sortAttr ? {
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
        let expression = '';
        let hookArgs;

        const attributes = _.omit(args, [partitionAttr, sortAttr]);
        const removeAttributes = _.reduce(attributes, (reduction, values, key) => {
            if (!_.isArray(values)) {
                values = [values];
            }

            if (!_.size(values)) {
                return reduction;
            }

            key = _.reduce(request.expHelper.getTokens(key), (reduction, key) => {
                request.addPlaceholderName(key);

                return reduction = reduction.replace(key, `#${key}`);
            }, key);

            return _.reduce(values, (reduction, value) => {
                return reduction.concat(`${key}[${value}]`);
            }, reduction);
        }, []);

        if (_.size(removeAttributes)) {
            expression = `REMOVE ${removeAttributes.join()}`;
        }

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
            attribute = _.reduce(request.expHelper.getTokens(attribute), (reduction, attribute) => {
                request.addPlaceholderName(attribute);

                return reduction = reduction.replace(attribute, `#${attribute}`);
            }, attribute);

            _.forEach(indexes, (fields, index) => {
                if (_.isObject(fields)) {
                    _.forEach(fields, (value, key) => {
                        request.addPlaceholderName(key)
                            .addPlaceholderValue({
                                [key]: value
                            });

                        reduction += `${attribute}[${index}].#${key} = :${key}, `;
                    });
                } else {
                    const placeholderKey = cuid.slug();

                    request.addPlaceholderValue({
                        [placeholderKey]: fields
                    });

                    reduction += `${attribute}[${index}] = :${placeholderKey}, `;
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

    addToSet(args, create = false, returns = 'ALL_NEW', hook = false) {
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

                request.addPlaceholderName(key)
                    .addPlaceholderValue({
                        [key]: value
                    });

                return reduction.concat(`#${key} :${key}`);
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

        return request.return(returns)[create ? 'insertOrUpdate' : 'update'].apply(request, hookArgs || [expression, sortAttr ? {
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

                request.addPlaceholderName(key)
                    .addPlaceholderValue({
                        [key]: value
                    });

                return reduction.concat(`#${key} :${key}`);
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
                key = _.reduce(request.expHelper.getTokens(key), (reduction, key) => {
                    request.addPlaceholderName(key);

                    return reduction = reduction.replace(key, `#${key}`);
                }, key);

                return reduction.concat(key);
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

        if (!_.isUndefined(partition)) {
            request.addPlaceholderName({
                    partition: partitionAttr
                })
                .addPlaceholderValue({
                    partition
                });
        }

        if (!_.isUndefined(sort)) {
            request.addPlaceholderName({
                    sort: sortAttr
                })
                .addPlaceholderValue({
                    sort
                });

            expression += ` AND begins_with(#sort, :sort)`;
        }

        return request.limit(1000)
            .queryScan(expression, response => {
                return this.request.batchWrite(response);
            });
    }

    toBase64(value) {
        if (_.isEmpty(value)) {
            return null;
        }

        return Buffer.from(value)
            .toString('base64');
    }

    fromBase64(value) {
        if (_.isEmpty(value)) {
            return null;
        }

        return Buffer.from(value, 'base64')
            .toString('ascii');
    }
};