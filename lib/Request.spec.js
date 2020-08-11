const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const rx = require('rxjs');
const rxop = require('rxjs/operators');

const {
    Util,
    Request,
    ReturnValues,
    Select,
    ConsumedCapacity
} = require('../');
const {
    dynamoDb
} = require('../testing');

chai.use(sinonChai);

const expect = chai.expect;

const namespace = 'spec';
const tableName = 'tblSpec';
const tableSchema = {
    primaryKeys: {
        partition: 'namespace',
        sort: 'id'
    },
    indexes: {
        localStringIndex: {
            partition: 'namespace',
            sort: 'localStringIndexedSortAttr'
        },
        localNumberIndex: {
            partition: 'namespace',
            sort: 'localNumberIndexedSortAttr'
        },
        globalStringIndex: {
            partition: 'globalIndexedPartitionAttr',
            sort: 'globalStringIndexedSortAttr'
        },
        globalNumberIndex: {
            partition: 'globalIndexedPartitionAttr',
            sort: 'globalNumberIndexedSortAttr'
        }
    }
};

describe('lib/Request', () => {
    const buffer = Buffer.from('hey');

    let now;
    let request;
    let client;

    before(done => {
        client = dynamoDb.client;
        request = dynamoDb.table(tableName, tableSchema);

        request.describe()
            .pipe(
                rxop.catchError(() => request.routeCall('createTable', {
                    TableName: 'tblSpec',
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1
                    },
                    AttributeDefinitions: [{
                        AttributeName: 'namespace',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'id',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'localStringIndexedSortAttr',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'localNumberIndexedSortAttr',
                        AttributeType: 'N'
                    }, {
                        AttributeName: 'globalIndexedPartitionAttr',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'globalStringIndexedSortAttr',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'globalNumberIndexedSortAttr',
                        AttributeType: 'N'
                    }],
                    KeySchema: [{
                        AttributeName: 'namespace',
                        KeyType: 'HASH'
                    }, {
                        AttributeName: 'id',
                        KeyType: 'RANGE'
                    }],
                    LocalSecondaryIndexes: [{
                        IndexName: 'localStringIndex',
                        KeySchema: [{
                            AttributeName: 'namespace',
                            KeyType: 'HASH'
                        }, {
                            AttributeName: 'localStringIndexedSortAttr',
                            KeyType: 'RANGE'
                        }],
                        Projection: {
                            ProjectionType: 'ALL'
                        }
                    }, {
                        IndexName: 'localNumberIndex',
                        KeySchema: [{
                            AttributeName: 'namespace',
                            KeyType: 'HASH'
                        }, {
                            AttributeName: 'localNumberIndexedSortAttr',
                            KeyType: 'RANGE'
                        }],
                        Projection: {
                            ProjectionType: 'ALL'
                        }
                    }],
                    GlobalSecondaryIndexes: [{
                        IndexName: 'globalStringIndex',
                        KeySchema: [{
                            AttributeName: 'globalIndexedPartitionAttr',
                            KeyType: 'HASH'
                        }, {
                            AttributeName: 'globalStringIndexedSortAttr',
                            KeyType: 'RANGE'
                        }],
                        Projection: {
                            ProjectionType: 'ALL'
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1
                        }
                    }, {
                        IndexName: 'globalNumberIndex',
                        KeySchema: [{
                            AttributeName: 'globalIndexedPartitionAttr',
                            KeyType: 'HASH'
                        }, {
                            AttributeName: 'globalNumberIndexedSortAttr',
                            KeyType: 'RANGE'
                        }],
                        Projection: {
                            ProjectionType: 'ALL'
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1
                        }
                    }]
                })),
                rxop.mergeMap(() => {
                    return rx.range(0, 10)
                        .pipe(
                            rxop.mergeMap(n => request.insert({
                                namespace,
                                buffer,
                                id: `id-${n}`,
                                message: `message-${n}`,
                                localStringIndexedSortAttr: `local-indexed-${n}`,
                                localNumberIndexedSortAttr: n,
                                globalIndexedPartitionAttr: `global-indexed-${namespace}`,
                                globalStringIndexedSortAttr: `global-indexed-${n}`,
                                globalNumberIndexedSortAttr: n
                            }, true))
                        );
                })
            )
            .subscribe(null, null, done);
    });

    after(done => {
        request = dynamoDb.table(tableName, tableSchema);
        request.batchWrite = request.batchWrite.bind(request);

        request.query({
                namespace
            })
            .pipe(
                rxop.toArray(),
                rxop.mergeMap(request.batchWrite)
            )
            .subscribe(null, null, done);
    });

    beforeEach(() => {
        now = _.now();
        request = dynamoDb.table(tableName, tableSchema);
        sinon.spy(request, 'routeCall');
    });

    afterEach(() => {
        request.routeCall.restore();
    });

    describe('util', () => {
        it('should return new util dynamoDb', () => {
            const util1 = request.util;
            const util2 = request.util;

            expect(util1).to.be.instanceOf(Util);
            expect(util1 === util2).to.be.false;
        });
    });

    describe('partitionAttr', () => {
        it('should returns primaryKeys.partition', () => {
            expect(request.partitionAttr).to.equal('namespace');
        });
    });

    describe('sortAttr', () => {
        it('should returns primaryKeys.sort', () => {
            expect(request.sortAttr).to.equal('id');
        });
    });

    describe('globalIndexPartitionAttr', () => {
        it('should returns globalIndex.partition', () => {
            request.index('globalStringIndex');

            expect(request.globalIndexPartitionAttr).to.equal('globalIndexedPartitionAttr');
        });

        it('should returns null when wrong globalIndex', () => {
            request.index('globalStringIndex_');

            expect(request.globalIndexPartitionAttr).to.be.null;
        });
    });

    describe('globalIndexSortAttr', () => {
        it('should returns globalIndex.sort', () => {
            request.index('globalStringIndex');

            expect(request.globalIndexSortAttr).to.equal('globalStringIndexedSortAttr');

            request.index('globalNumberIndex');

            expect(request.globalIndexSortAttr).to.equal('globalNumberIndexedSortAttr');
        });

        it('should returns null when wrong globalIndex', () => {
            request.index('globalStringIndex_');

            expect(request.globalIndexSortAttr).to.be.null;
        });
    });

    describe('localIndexSortAttr', () => {
        it('should returns localIndex.sort', () => {
            request.index('localStringIndex');

            expect(request.localIndexSortAttr).to.equal('localStringIndexedSortAttr');
        });

        it('should returns null when wrong localIndex', () => {
            request.index('localStringIndex_');

            expect(request.localIndexSortAttr).to.be.null;
        });

        it('should returns null when is global index', () => {
            request.index('globalStringIndex');

            expect(request.localIndexSortAttr).to.be.null;
        });
    });

    describe('routeCall', () => {
        beforeEach(() => {
            sinon.stub(client, 'getItem');
        });

        afterEach(() => {
            client.getItem.restore();
        });

        it('should return an Observable', () => {
            expect(request.routeCall('getItem', {})).to.be.instanceOf(rx.Observable);
        });

        it('should call method on client', done => {
            setImmediate(() => {
                client.getItem.callArgWith(1, null, {});
            });

            request.routeCall('getItem', {
                    TableName: tableName,
                    Key: {
                        namespace: {
                            S: 'spec'
                        },
                        id: {
                            S: 'id'
                        }
                    }
                })
                .subscribe(() => {
                    expect(client.getItem).to.have.been.calledWith({
                        TableName: tableName,
                        Key: {
                            namespace: {
                                S: 'spec'
                            },
                            id: {
                                S: 'id'
                            }
                        }
                    }, sinon.match.func);
                }, null, done);
        });

        it('should not call method on client when no subscribers', () => {
            request.routeCall('getItem', {
                TableName: tableName,
                Key: {
                    namespace: {
                        S: 'spec'
                    },
                    id: {
                        S: 'id'
                    }
                }
            });

            expect(client.getItem).not.to.have.been.called;
        });
    });

    describe('describe', () => {
        it('should routeCall with describeTable', () => {
            request.describe()
                .subscribe();

            expect(request.routeCall).to.have.been.calledWithExactly('describeTable', {
                TableName: tableName
            });
        });

        it('should return an Observable', () => {
            expect(request.describe()).to.be.instanceOf(rx.Observable);
        });
    });

    describe('table', () => {
        it('should feed table, tableSchema and primaryKeys', () => {
            expect(request.tableName).to.equal(tableName);
            expect(request.primaryKeys).to.deep.equal({
                partition: 'namespace',
                sort: 'id'
            });
        });

        it('should return Request dynamoDb', () => {
            expect(dynamoDb.table(tableName, tableSchema)).to.be.instanceOf(Request);
        });
    });

    describe('return', () => {
        beforeEach(() => {
            request.return(ReturnValues.ALL_NEW);
        });

        it('should feed returnValues', () => {
            expect(request.returnValues).to.equal('ALL_NEW');
        });

        it('should return Request dynamoDb', () => {
            expect(request.return(ReturnValues.ALL_NEW)).to.be.instanceOf(Request);
        });
    });

    describe('consumedCapacity', () => {
        beforeEach(() => {
            request.consumedCapacity(ConsumedCapacity.TOTAL);
        });

        it('should feed returnValues', () => {
            expect(request.returnConsumedCapacity).to.equal('TOTAL');
        });

        it('should return Request dynamoDb', () => {
            expect(request.consumedCapacity(ConsumedCapacity.TOTAL)).to.be.instanceOf(Request);
        });
    });

    describe('addPlaceholderName', () => {
        it('should add placeholders with object', () => {
            request.addPlaceholderName({
                id: 'value'
            });

            expect(request.expressionAttributeNames).to.deep.equal({
                '#id': 'value'
            });
        });

        it('should add placeholders with array', () => {
            request.addPlaceholderName(['id', 'id2']);

            expect(request.expressionAttributeNames).to.deep.equal({
                '#id': 'id',
                '#id2': 'id2'
            });
        });

        it('should add placeholders with string', () => {
            request.addPlaceholderName('id');

            expect(request.expressionAttributeNames).to.deep.equal({
                '#id': 'id'
            });
        });

        it('should return Request dynamoDb', () => {
            expect(request.addPlaceholderName('id')).to.be.instanceOf(Request);
        });
    });

    describe('addPlaceholderValue', () => {
        it('should add placeholders with object and string', () => {
            request.addPlaceholderValue({
                id: 'value'
            });

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    S: 'value'
                }
            });
        });

        it('should add placeholders with object and number', () => {
            request.addPlaceholderValue({
                id: 9
            });

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    N: '9'
                }
            });
        });

        it('should add placeholders with object and boolean', () => {
            request.addPlaceholderValue({
                id: true
            });

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    BOOL: true
                }
            });
        });

        it('should add placeholders with object and null', () => {
            request.addPlaceholderValue({
                id: null
            });

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    NULL: true
                }
            });
        });

        it('should add placeholders with object and array', () => {
            request.addPlaceholderValue({
                id: [1, 2]
            });

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    L: [{
                        N: '1'
                    }, {
                        N: '2'
                    }]
                }
            });
        });

        it('should add placeholders with object and map', () => {
            request.addPlaceholderValue({
                id: {
                    id: 'value'
                }
            });

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    M: {
                        id: {
                            S: 'value'
                        }
                    }
                }
            });
        });

        it('should add placeholders with string array', () => {
            request.addPlaceholderValue(['id', 'id2']);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    S: 'id'
                },
                ':id2': {
                    S: 'id2'
                }
            });
        });

        it('should add placeholders with number array', () => {
            request.addPlaceholderValue([1, 2]);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':1': {
                    N: '1'
                },
                ':2': {
                    N: '2'
                }
            });
        });

        it('should add placeholders with boolean array', () => {
            request.addPlaceholderValue([true, false]);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':true': {
                    BOOL: true
                },
                ':false': {
                    BOOL: false
                }
            });
        });

        it('should add placeholders with null array', () => {
            request.addPlaceholderValue([null]);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':null': {
                    NULL: true
                }
            });
        });

        it('should add placeholders with string', () => {
            request.addPlaceholderValue('id');

            expect(request.expressionAttributeValues).to.deep.equal({
                ':id': {
                    S: 'id'
                }
            });
        });

        it('should add placeholders with number', () => {
            request.addPlaceholderValue(9);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':9': {
                    N: '9'
                }
            });
        });

        it('should add placeholders with boolean', () => {
            request.addPlaceholderValue(true);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':true': {
                    BOOL: true
                }
            });
        });

        it('should add placeholders with null', () => {
            request.addPlaceholderValue(null);

            expect(request.expressionAttributeValues).to.deep.equal({
                ':null': {
                    NULL: true
                }
            });
        });

        it('should return Request dynamoDb', () => {
            expect(request.addPlaceholderValue(null)).to.be.instanceOf(Request);
        });
    });

    describe('select', () => {
        it('should add projectionSelect ALL_ATTRIBUTES', () => {
            request.select(Select.ALL_ATTRIBUTES);

            expect(request.projectionSelect).to.equal('ALL_ATTRIBUTES');
        });

        it('should add projectionSelect ALL_PROJECTED_ATTRIBUTES', () => {
            request.select(Select.ALL_PROJECTED_ATTRIBUTES);

            expect(request.projectionSelect).to.equal('ALL_PROJECTED_ATTRIBUTES');
        });

        it('should add projectionSelect SPECIFIC_ATTRIBUTES', () => {
            request.select(Select.SPECIFIC_ATTRIBUTES);

            expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
        });

        it('should add projectionSelect COUNT', () => {
            request.select(Select.COUNT);

            expect(request.projectionSelect).to.equal('COUNT');
        });

        it('should add projectionSelect, projectionExpression and expressionAttributeNames with custom tokens', () => {
            request.select('id, name, age');

            expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
            expect(request.projectionExpression).to.equal('#id_0,#name_1,#age_2,#namespace_3');
            expect(request.expressionAttributeNames).to.deep.equal({
                '#id_0': 'id',
                '#name_1': 'name',
                '#age_2': 'age',
                '#namespace_3': 'namespace'
            });
        });

        it('should include also primaryKeys and localIndexSortAttr', () => {
            request.index('localStringIndex')
                .select('id, name, age');

            expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
            expect(request.projectionExpression).to.equal('#id_0,#name_1,#age_2,#namespace_3,#localStringIndexedSortAttr_4');
            expect(request.expressionAttributeNames).to.deep.equal({
                '#id_0': 'id',
                '#name_1': 'name',
                '#age_2': 'age',
                '#namespace_3': 'namespace',
                '#localStringIndexedSortAttr_4': 'localStringIndexedSortAttr'
            });
        });

        it('should include also primaryKeys, globalIndexPartitionAttr and globalIndexSortAttr', () => {
            request.index('globalStringIndex')
                .select('name, age');

            expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
            expect(request.projectionExpression).to.equal('#name_0,#age_1,#namespace_2,#id_3,#globalIndexedPartitionAttr_4,#globalStringIndexedSortAttr_5');
            expect(request.expressionAttributeNames).to.deep.equal({
                '#name_0': 'name',
                '#age_1': 'age',
                '#namespace_2': 'namespace',
                '#id_3': 'id',
                '#globalIndexedPartitionAttr_4': 'globalIndexedPartitionAttr',
                '#globalStringIndexedSortAttr_5': 'globalStringIndexedSortAttr'
            });
        });

        it('should return Request dynamoDb', () => {
            expect(request.select('id')).to.be.instanceOf(Request);
        });
    });

    describe('consistent', () => {
        it('should set consistentRead as true', () => {
            request.consistent();

            expect(request.consistentRead).to.be.true;
        });

        it('should return Request dynamoDb', () => {
            expect(request.consistent()).to.be.instanceOf(Request);
        });
    });

    describe('query', () => {
        it('should keyConditionExpression be same of queryData when latter is string', () => {
            request.query('#id = :value');

            expect(request.keyConditionExpression).to.equal('#id = :value');
        });

        it('should build keyConditionExpression when queryData is object, using just schema keys', () => {
            request.query({
                namespace,
                ignoredAttr: 'this attr should be ignored'
            });

            expect(request.keyConditionExpression).to.equal('#namespace = :namespace');
        });

        it('should routeCall with query and relative params and queryLimit + 1', () => {
            request.query({
                namespace,
                ignoredAttr: 'this attr should be ignored'
            });

            expect(request.routeCall).to.have.been.calledOnce;
            expect(request.routeCall).to.have.been.calledWithExactly('query', {
                ConsistentRead: false,
                ExclusiveStartKey: null,
                ExpressionAttributeNames: {
                    '#namespace': 'namespace'
                },
                ExpressionAttributeValues: {
                    ':namespace': {
                        S: 'spec'
                    }
                },
                FilterExpression: null,
                IndexName: null,
                KeyConditionExpression: '#namespace = :namespace',
                Limit: 51,
                ProjectionExpression: undefined,
                ReturnConsumedCapacity: 'TOTAL',
                ScanIndexForward: true,
                Select: 'ALL_ATTRIBUTES',
                TableName: 'tblSpec'
            });
        });

        it('should responds with normalized data', done => {
            request.limit(2)
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[0].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[0].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[0].globalStringIndexedSortAttr,
                        id: 'id-0',
                        message: response[0].message,
                        createdAt: response[0].createdAt,
                        updatedAt: response[0].updatedAt
                    }, {
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[1].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[1].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[1].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[1].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[1].globalStringIndexedSortAttr,
                        id: 'id-1',
                        message: response[1].message,
                        createdAt: response[1].createdAt,
                        updatedAt: response[1].updatedAt
                    }]);
                }, null, done);
        });

        it('should responds with normalized data using Select.COUNT', done => {
            request.limit(2)
                .select(Select.COUNT)
                .query({
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([]);
                }, null, done);
        });

        it('should responds empty using localIndex', done => {
            request.limit(2)
                .index('localStringIndex')
                .query({
                    namespace,
                    localStringIndexedSortAttr: 'local-indexed-3',
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[0].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[0].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[0].globalStringIndexedSortAttr,
                        id: 'id-3',
                        message: response[0].message,
                        createdAt: response[0].createdAt,
                        updatedAt: response[0].updatedAt
                    }]);
                }, null, done);
        });

        it('should responds with normalized data using globalIndex', done => {
            request.limit(2)
                .index('globalStringIndex')
                .query({
                    globalIndexedPartitionAttr: `global-indexed-${namespace}`,
                    globalStringIndexedSortAttr: `global-indexed-3`,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[0].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[0].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[0].globalStringIndexedSortAttr,
                        id: 'id-3',
                        message: response[0].message,
                        createdAt: response[0].createdAt,
                        updatedAt: response[0].updatedAt
                    }]);
                }, null, done);
        });

        it('should responds with resumed data', done => {
            const query = request.limit(3)
                .query({
                    buffer,
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.last()
                );

            const resumedQuery = after => request.limit(2)
                .resume(request.queryStats.after)
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                );

            query.pipe(
                    rxop.mergeMap(() => resumedQuery(request.queryStats.after))
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[0].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[0].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[0].globalStringIndexedSortAttr,
                        id: 'id-3',
                        message: response[0].message,
                        createdAt: response[0].createdAt,
                        updatedAt: response[0].updatedAt
                    }, {
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[1].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[1].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[1].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[1].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[1].globalStringIndexedSortAttr,
                        id: 'id-4',
                        message: response[1].message,
                        createdAt: response[1].createdAt,
                        updatedAt: response[1].updatedAt
                    }]);
                }, null, done);
        });

        it('should responds with resumed data using localIndex', done => {
            const query = request.limit(3)
                .index('localStringIndex')
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.last()
                );

            const resumedQuery = after => request.limit(2)
                .resume(request.queryStats.after)
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                );

            query
                .pipe(
                    rxop.mergeMap(() => resumedQuery(request.queryStats.after))
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[0].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[0].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[0].globalStringIndexedSortAttr,
                        id: 'id-3',
                        message: response[0].message,
                        createdAt: response[0].createdAt,
                        updatedAt: response[0].updatedAt
                    }, {
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[1].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[1].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[1].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[1].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[1].globalStringIndexedSortAttr,
                        id: 'id-4',
                        message: response[1].message,
                        createdAt: response[1].createdAt,
                        updatedAt: response[1].updatedAt
                    }]);
                }, null, done);
        });

        it('should responds with resumed data using globalIndex', done => {
            const query = request.limit(3)
                .index('globalStringIndex')
                .query({
                    globalIndexedPartitionAttr: `global-indexed-${namespace}`,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.last()
                );

            const resumedQuery = after => request.limit(2)
                .resume(request.queryStats.after)
                .query({
                    globalIndexedPartitionAttr: `global-indexed-${namespace}`,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                );

            query
                .pipe(
                    rxop.mergeMap(() => resumedQuery(request.queryStats.after))
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[0].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[0].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[0].globalStringIndexedSortAttr,
                        id: 'id-3',
                        message: response[0].message,
                        createdAt: response[0].createdAt,
                        updatedAt: response[0].updatedAt
                    }, {
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: response[1].localNumberIndexedSortAttr,
                        localStringIndexedSortAttr: response[1].localStringIndexedSortAttr,
                        globalIndexedPartitionAttr: response[1].globalIndexedPartitionAttr,
                        globalNumberIndexedSortAttr: response[1].globalNumberIndexedSortAttr,
                        globalStringIndexedSortAttr: response[1].globalStringIndexedSortAttr,
                        id: 'id-4',
                        message: response[1].message,
                        createdAt: response[1].createdAt,
                        updatedAt: response[1].updatedAt
                    }]);
                }, null, done);
        });

        it('shuld feed queryStats', done => {
            request.limit(2)
                .resume({
                    namespace,
                    id: 'id-2'
                })
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(request.queryStats).to.deep.equal({
                        before: {
                            namespace,
                            id: 'id-3'
                        },
                        after: {
                            namespace,
                            id: 'id-4'
                        },
                        consumedCapacity: 0,
                        count: 2,
                        scannedCount: 2,
                        iteractions: 1
                    });
                }, null, done);
        });

        it('should feed queryStats with localIndex', done => {
            request.index('localStringIndex')
                .limit(2)
                .resume({
                    id: 'id-2',
                    namespace,
                    localStringIndexedSortAttr: 'local-indexed-2'
                })
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(() => {
                    expect(request.queryStats).to.deep.equal({
                        before: {
                            localStringIndexedSortAttr: 'local-indexed-3',
                            namespace,
                            id: 'id-3'
                        },
                        after: {
                            localStringIndexedSortAttr: 'local-indexed-4',
                            namespace,
                            id: 'id-4'
                        },
                        consumedCapacity: 0,
                        count: 2,
                        scannedCount: 2,
                        iteractions: 1
                    });
                }, null, done);
        });

        it('should feed queryStats with globalIndex', done => {
            request.index('globalStringIndex')
                .limit(2)
                .resume({
                    id: 'id-2',
                    namespace,
                    globalIndexedPartitionAttr: 'global-indexed-spec',
                    globalStringIndexedSortAttr: 'global-indexed-2'
                })
                .query({
                    globalIndexedPartitionAttr: `global-indexed-${namespace}`,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(() => {
                    expect(request.queryStats).to.deep.equal({
                        before: {
                            id: 'id-3',
                            namespace,
                            globalIndexedPartitionAttr: 'global-indexed-spec',
                            globalStringIndexedSortAttr: 'global-indexed-3'
                        },
                        after: {
                            id: 'id-4',
                            namespace,
                            globalIndexedPartitionAttr: 'global-indexed-spec',
                            globalStringIndexedSortAttr: 'global-indexed-4'
                        },
                        consumedCapacity: 0,
                        count: 2,
                        scannedCount: 2,
                        iteractions: 1
                    });
                }, null, done);
        });

        it('should not feed queryStats.after when query fetches all', done => {
            request.limit(10)
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(() => {
                    expect(request.queryStats.after).to.be.null;
                }, null, done);
        });

        it('should not feed queryStats.after when resume fetches all', done => {
            request.limit(5)
                .resume({
                    namespace,
                    id: 'id-4'
                })
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(() => {
                    expect(request.queryStats.after).to.be.null;
                }, null, done);
        });

        it('should not feed queryStats.before when not resumed', done => {
            request.limit(2)
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(request.queryStats.before).to.be.null;
                }, null, done);
        });

        it('should not feed queryStats.before when inexistent items', done => {
            request.limit(10)
                .resume({
                    namespace,
                    id: 'id-9'
                })
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(request.queryStats.before).to.be.null;
                }, null, done);
        });

        it('should routeCall with queryLimit * 4 when filterExpression', () => {
            request.addPlaceholderName('message')
                .addPlaceholderValue('message')
                .filter('begins_with(#message, :message)')
                .query({
                    namespace,
                    ignoredAttr: 'this attr should be ignored'
                });

            expect(request.routeCall).to.have.been.calledOnce;
            expect(request.routeCall).to.have.been.calledWithExactly('query', {
                ConsistentRead: false,
                ExclusiveStartKey: null,
                ExpressionAttributeNames: {
                    '#message': 'message',
                    '#namespace': 'namespace'
                },
                ExpressionAttributeValues: {
                    ':message': {
                        S: 'message'
                    },
                    ':namespace': {
                        S: 'spec'
                    }
                },
                FilterExpression: 'begins_with(#message, :message)',
                IndexName: null,
                KeyConditionExpression: '#namespace = :namespace',
                Limit: 200,
                ProjectionExpression: undefined,
                ReturnConsumedCapacity: 'TOTAL',
                ScanIndexForward: true,
                Select: 'ALL_ATTRIBUTES',
                TableName: 'tblSpec'
            });
        });

        it('should return an Observable', () => {
            expect(request.query({})).to.be.instanceOf(rx.Observable);
        });

        describe('multiple operations', () => {
            beforeEach(() => {
                let index = 0;

                request.routeCall.restore();
                sinon.stub(request, 'routeCall')
                    .callsFake(() => rx.of({
                        Items: [{
                            id: {
                                'S': `id-${++index}`
                            },
                            createdAt: {
                                'N': '1462910456975'
                            },
                            namespace: {
                                'S': 'spec'
                            },
                            updatedAt: {
                                'N': '1462910456975'
                            }
                        }, {
                            id: {
                                'S': `id-${++index}`
                            },
                            createdAt: {
                                'N': '1462910456979'
                            },
                            namespace: {
                                'S': 'spec'
                            },
                            updatedAt: {
                                'N': '1462910456979'
                            }
                        }],
                        Count: 2,
                        ScannedCount: 2,
                        LastEvaluatedKey: {
                            id: {
                                'S': `id-${index}`
                            },
                            namespace: {
                                'S': 'spec'
                            }
                        }
                    }));
            });

            it('should run queryOperation 25 times until reach default limit or after be null', done => {
                request.query({
                        namespace,
                        ignoredAttr: 'this attr should be ignored'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        expect(request.queryStats.scannedCount).to.equal(50);
                        expect(request.queryStats.count).to.equal(50);
                        expect(request.queryStats.iteractions).to.equal(25);
                    }, null, done);
            });

            it('should run queryOperation 5 times until reach limit', done => {
                request.limit(10)
                    .query({
                        namespace,
                        ignoredAttr: 'this attr should be ignored'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        expect(request.queryStats.scannedCount).to.equal(10);
                        expect(request.queryStats.count).to.equal(10);
                        expect(request.queryStats.iteractions).to.equal(5);
                    }, null, done);
            });

            it('should return strict the limit even if more items were processed', done => {
                request.limit(9)
                    .query({
                        namespace,
                        ignoredAttr: 'this attr should be ignored'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        expect(request.queryStats.scannedCount).to.equal(10);
                        expect(request.queryStats.count).to.equal(10);
                        expect(request.queryStats.iteractions).to.equal(5);
                        expect(_.size(response)).to.equal(9);
                    }, null, done);
            });

            it('should not feed queryStats.before when not resumed', done => {
                request.limit(10)
                    .query({
                        namespace,
                        ignoredAttr: 'this attr should be ignored'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        expect(request.queryStats.before).to.be.null;
                    }, null, done);
            });

            it('should feed queryStats.before when resumed', done => {
                request.limit(10)
                    .resume({})
                    .query({
                        namespace,
                        ignoredAttr: 'this attr should be ignored'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        expect(request.queryStats.before).to.deep.equal({
                            namespace,
                            id: 'id-1'
                        });
                    }, null, done);
            });
        });
    });

    describe('queryScan', () => {
        it('should run without scan', done => {
            request.queryScan({
                    namespace
                })
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(_.size(response)).to.equal(10);
                }, null, done);
        });

        it('should call scan', done => {
            const scan = sinon.stub()
                .callsFake((items, after, index) => {
                    return rx.of({
                        after,
                        response: items
                    });
                });

            request.queryScan({
                    namespace
                }, scan)
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(scan).to.have.been.calledOnce;
                    expect(_.size(response)).to.equal(10);
                }, null, done);
        });

        it('should call scan 5 times', done => {
            const scan = sinon.stub()
                .callsFake(response => {
                    return rx.of(response);
                });

            request.limit(2)
                .queryScan({
                    namespace
                }, scan)
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(scan).to.have.callCount(5);
                    expect(_.size(response)).to.equal(10);
                }, null, done);
        });

        it('should call until falsy after', done => {
            const scan = sinon.stub()
                .callsFake((response, index) => {
                    if (index >= 1) {
                        return rx.of({
                            after: null,
                            response
                        });
                    }

                    return rx.of(response);
                });

            request.limit(2)
                .queryScan({
                    namespace
                }, scan)
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(scan).to.have.calledTwice;
                    expect(_.size(response)).to.equal(4);
                }, null, done);
        });
    });

    describe('getIndexedAttributes', () => {
        let getIndexedAttributes;

        beforeEach(() => {
            getIndexedAttributes = () => request.getIndexedAttributes({
                namespace: 'namespace',
                id: 'id',
                localStringIndexedSortAttr: 'localStringIndexedSortAttr',
                localNumberIndexedSortAttr: 'localNumberIndexedSortAttr',
                globalIndexedPartitionAttr: 'globalIndexedPartitionAttr',
                globalStringIndexedSortAttr: 'globalStringIndexedSortAttr',
                globalNumberIndexedSortAttr: 'globalNumberIndexedSortAttr'
            });
        });

        it('should pick partitionAttr and sortAttr', () => {
            expect(getIndexedAttributes()).to.deep.equal({
                namespace: 'namespace',
                id: 'id'
            });
        });

        it('should pick partitionAttr, sortAttr and localStringIndexedSortAttr', () => {
            request.index('localStringIndex');

            expect(getIndexedAttributes()).to.deep.equal({
                namespace: 'namespace',
                id: 'id',
                localStringIndexedSortAttr: 'localStringIndexedSortAttr'
            });
        });

        it('should pick partitionAttr, sortAttr and localNumberIndexedSortAttr', () => {
            request.index('localNumberIndex');

            expect(getIndexedAttributes()).to.deep.equal({
                namespace: 'namespace',
                id: 'id',
                localNumberIndexedSortAttr: 'localNumberIndexedSortAttr'
            });
        });

        it('should pick partitionAttr, sortAttr, globalIndexedPartitionAttr, and globalStringIndexedSortAttr', () => {
            request.index('globalStringIndex');

            expect(getIndexedAttributes()).to.deep.equal({
                namespace: 'namespace',
                id: 'id',
                globalIndexedPartitionAttr: 'globalIndexedPartitionAttr',
                globalStringIndexedSortAttr: 'globalStringIndexedSortAttr'
            });
        });

        it('should pick partitionAttr, sortAttr, globalIndexedPartitionAttr, and globalNumberIndexedSortAttr', () => {
            request.index('globalNumberIndex');

            expect(getIndexedAttributes()).to.deep.equal({
                namespace: 'namespace',
                id: 'id',
                globalIndexedPartitionAttr: 'globalIndexedPartitionAttr',
                globalNumberIndexedSortAttr: 'globalNumberIndexedSortAttr'
            });
        });
    });

    describe('get', () => {
        it('should routeCall with get and relative params', () => {
            request.get({
                namespace,
                id: 'id-0',
                ignoredAttr: 'this attr should be ignored'
            });

            expect(request.routeCall).to.have.been.calledOnce;
            expect(request.routeCall).to.have.been.calledWithExactly('getItem', {
                ExpressionAttributeNames: null,
                Key: {
                    id: {
                        S: 'id-0'
                    },
                    namespace: {
                        S: namespace
                    }
                },
                ProjectionExpression: undefined,
                ReturnConsumedCapacity: 'TOTAL',
                TableName: 'tblSpec'
            });
        });

        it('should responds with normalized data', done => {
            request.get({
                    namespace,
                    id: 'id-0',
                    ignoredAttr: 'this attr should be ignored'
                })
                .subscribe(response => {
                    expect(response).to.deep.equal({
                        buffer,
                        namespace,
                        localNumberIndexedSortAttr: 0,
                        localStringIndexedSortAttr: 'local-indexed-0',
                        globalIndexedPartitionAttr: 'global-indexed-spec',
                        globalNumberIndexedSortAttr: 0,
                        globalStringIndexedSortAttr: 'global-indexed-0',
                        id: 'id-0',
                        message: response.message,
                        createdAt: response.createdAt,
                        updatedAt: response.updatedAt
                    });
                }, null, done);
        });

        it('should responds null', done => {
            request.get({
                    namespace,
                    id: 'id-inexistent',
                    ignoredAttr: 'this attr should be ignored'
                })
                .subscribe(response => {
                    expect(response).to.be.null;
                }, null, done);
        });

        it('should return an Observable', () => {
            expect(request.get({})).to.be.instanceOf(rx.Observable);
        });
    });

    describe('index', () => {
        it('should set indexName', () => {
            request.index('someIndex');

            expect(request.indexName).to.equal('someIndex');
        });

        it('should return Request dynamoDb', () => {
            expect(request.index('someIndex')).to.be.instanceOf(Request);
        });
    });

    describe('desc', () => {
        it('should set scanIndexForward to false', () => {
            request.desc();

            expect(request.scanIndexForward).to.be.false;
        });

        it('should return Request dynamoDb', () => {
            expect(request.desc()).to.be.instanceOf(Request);
        });
    });

    describe('limit', () => {
        _.map([25, '25'], limit => {
            it(`should set queryLimit as ${typeof limit}`, () => {
                request.limit(limit);

                expect(request.queryLimit).to.equal(25);
            });
        });

        it('should handle not finite values and defaults to 50', () => {
            request.limit(Infinity);

            expect(request.queryLimit).to.equal(50);
        });

        it('should handle limit = 0 and defaults to 50', () => {
            request.limit(0);

            expect(request.queryLimit).to.equal(50);
        });

        it('should return Request dynamoDb', () => {
            expect(request.limit(25)).to.be.instanceOf(Request);
        });
    });

    describe('filter', () => {
        it('should set filterExpression', () => {
            request.filter('someFilter');

            expect(request.filterExpression).to.equal('someFilter');
        });

        it('should append filterExpression', () => {
            request.filter('someFilter');
            request.filter('someFilter2');

            expect(request.filterExpression).to.equal('someFilter AND someFilter2');
        });

        it('should append filterExpression with OR', () => {
            request.filter('someFilter');
            request.filter('someFilter2', true, 'OR');

            expect(request.filterExpression).to.equal('someFilter OR someFilter2');
        });

        it('should replace filterExpression', () => {
            request.filter('someFilter');
            request.filter('someFilter2', false);

            expect(request.filterExpression).to.equal('someFilter2');
        });

        it('should return Request dynamoDb', () => {
            expect(request.filter('someFilter')).to.be.instanceOf(Request);
        });
    });

    describe('resume', () => {
        it('should set exclusiveStartKey and isResumed', () => {
            request.resume({
                namespace,
                id: 'id-0'
            });

            expect(request.isResumed).to.be.true;
            expect(request.exclusiveStartKey).to.deep.equal({
                namespace: {
                    S: namespace
                },
                id: {
                    S: 'id-0'
                }
            });
        });

        it('should return Request dynamoDb', () => {
            expect(request.resume({
                namespace,
                id: 'id-0'
            })).to.be.instanceOf(Request);
        });
    });

    describe('condition', () => {
        it('should set conditionExpression', () => {
            request.condition('someCondition');

            expect(request.conditionExpression).to.equal('someCondition');
        });

        it('should append conditionExpression', () => {
            request.condition('someCondition');
            request.condition('someCondition2');

            expect(request.conditionExpression).to.equal('someCondition AND someCondition2');
        });

        it('should append conditionExpression with OR', () => {
            request.condition('someCondition');
            request.condition('someCondition2', true, 'OR');

            expect(request.conditionExpression).to.equal('someCondition OR someCondition2');
        });

        it('should replace conditionExpression', () => {
            request.condition('someCondition');
            request.condition('someCondition2', false);

            expect(request.conditionExpression).to.equal('someCondition2');
        });

        it('should return Request dynamoDb', () => {
            expect(request.condition('someCondition')).to.be.instanceOf(Request);
        });
    });

    describe('insert', () => {
        it('should routeCall with putItem and relative params', () => {
            request.insert({
                namespace,
                id: `id-${now}`,
                message: `message-${now}`
            });

            expect(request.routeCall).to.have.been.calledOnce;
            expect(request.routeCall).to.have.been.calledWithExactly('putItem', {
                ConditionExpression: 'attribute_not_exists(#namespace)',
                ExpressionAttributeNames: {
                    '#namespace': 'namespace'
                },
                ExpressionAttributeValues: null,
                Item: {
                    namespace: {
                        S: 'spec'
                    },
                    id: {
                        S: `id-${now}`
                    },
                    message: {
                        S: `message-${now}`
                    },
                    createdAt: {
                        N: sinon.match.string
                    },
                    updatedAt: {
                        N: sinon.match.string
                    }
                },
                ReturnConsumedCapacity: 'TOTAL',
                TableName: 'tblSpec'
            });
        });

        it('should responds with normalized data and createdAt and updatedAt be equals', done => {
            request.insert({
                    buffer,
                    namespace,
                    id: `id-${now}`,
                    message: `message-${now}`,
                    undefined: undefined,
                    null: null,
                    number: 1,
                    boolean: true,
                    object: {
                        data: 'data'
                    },
                    ss: request.util.raw({
                        SS: ['a']
                    }),
                    ns: request.util.raw({
                        NS: ['1']
                    })
                })
                .subscribe(response => {
                    expect(response.createdAt === response.updatedAt).to.be.true;
                    expect(response).to.deep.equal({
                        buffer,
                        namespace,
                        id: `id-${now}`,
                        message: response.message,
                        undefined: undefined,
                        null: null,
                        number: 1,
                        boolean: true,
                        object: {
                            data: 'data'
                        },
                        ss: ['a'],
                        ns: [1],
                        createdAt: response.createdAt,
                        updatedAt: response.updatedAt
                    });
                }, null, done);
        });

        it('should not override createdAt and updatedAt', done => {
            request.insert({
                    namespace,
                    id: `id-${now}`,
                    createdAt: 1,
                    updatedAt: 1
                })
                .subscribe(response => {
                    expect(response.createdAt).not.be.equal(1);
                    expect(response.updatedAt).not.be.equal(1);
                    expect(response.createdAt === response.updatedAt).to.be.true;
                }, null, done);
        });

        it('should override createdAt and updatedAt', done => {
            request.insert({
                    namespace,
                    id: `id-${now}`,
                    createdAt: 1,
                    updatedAt: 1
                }, false, true)
                .subscribe(response => {
                    expect(response.createdAt).be.equal(1);
                    expect(response.updatedAt).be.equal(1);
                    expect(response.createdAt === response.updatedAt).to.be.true;
                }, null, done);
        });

        it('should throw if record exists and replace = false', done => {
            request.insert({
                    namespace,
                    id: 'id-0',
                    message: 'message-0'
                })
                .subscribe(null, err => {
                    expect(request.conditionExpression).to.equal('attribute_not_exists(#namespace)');
                    expect(err.message).to.equal('The conditional request failed');
                    done();
                });
        });

        it('should responds with normalized data if data exists and replace = true', done => {
            request.insert({
                    namespace,
                    id: 'id-0',
                    message: 'message-0'
                }, true)
                .subscribe(response => {
                    expect(request.conditionExpression).to.be.null;
                    expect(response.createdAt === response.updatedAt).to.be.true;
                    expect(response).to.deep.equal({
                        namespace,
                        id: 'id-0',
                        message: response.message,
                        createdAt: response.createdAt,
                        updatedAt: response.updatedAt
                    });
                }, null, done);
        });

        it('should return an Observable', () => {
            expect(request.insert({})).to.be.instanceOf(rx.Observable);
        });
    });

    describe('insertOrReplace', () => {
        beforeEach(() => {
            sinon.spy(request, 'insert');
        });

        afterEach(() => {
            request.insert.restore();
        });

        it('should call insert with replace = true', () => {
            request.insertOrReplace({});

            expect(request.insert).to.have.been.calledWith(sinon.match.object, true);
        });
    });

    describe('insertOrUpdate', () => {
        beforeEach(() => {
            sinon.spy(request, 'update');
        });

        afterEach(() => {
            request.update.restore();
        });

        it('should call update with where = false, insert = true', () => {
            request.insertOrUpdate({});

            expect(request.update).to.have.been.calledWith(sinon.match.object, undefined, true);
        });
    });

    describe('update', () => {
        it('should updateExpression be same of item when latter is string', () => {
            request.update('SET #id = :value', {});

            expect(request.updateExpression).to.equal('SET #id = :value');
        });

        it('should build updateExpression when item is object', () => {
            request.update({
                id: 'value',
                id2: 'value2'
            });

            expect(request.updateExpression).to.equal('SET #id2 = :id2, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
        });

        it('should omit createdAt and updatedAt from updateExpression', () => {
            request.update({
                id: 'value',
                id2: 'value2',
                createdAt: 1,
                updatedAt: 1
            });

            expect(request.updateExpression).to.equal('SET #id2 = :id2, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
        });

        it('should throw when item is string and no where statement is provided', done => {
            request.update('SET #id = :value')
                .subscribe(null, err => {
                    expect(err.message).to.equal('Where statement might be provided');
                    done();
                });
        });

        it('should Keys be composed with just primary keys', () => {
            request.return(ReturnValues.ALL_NEW)
                .update({
                    namespace,
                    id: 'id-0',
                    ignoredAttr: 'this attr should be ignored'
                });

            expect(request.routeCall.lastCall.args[1].Key).to.deep.equal({
                id: {
                    S: 'id-0'
                },
                namespace: {
                    S: 'spec'
                }
            });
        });

        it('should routeCall with updateItem and relative params', () => {
            request.return(ReturnValues.ALL_NEW)
                .update({
                    namespace,
                    id: 'id-0',
                    message: 'message-0'
                });

            expect(request.routeCall).to.have.been.calledOnce;
            expect(request.routeCall).to.have.been.calledWithExactly('updateItem', {
                ConditionExpression: 'attribute_exists(#namespace)',
                ExpressionAttributeNames: {
                    '#createdAt': 'createdAt',
                    '#namespace': 'namespace',
                    '#message': 'message',
                    '#updatedAt': 'updatedAt'
                },
                ExpressionAttributeValues: {
                    ':message': {
                        S: 'message-0'
                    },
                    ':now': {
                        N: sinon.match.string
                    }
                },
                Key: {
                    id: {
                        S: 'id-0'
                    },
                    namespace: {
                        S: 'spec'
                    }
                },
                ReturnConsumedCapacity: 'TOTAL',
                ReturnValues: 'ALL_NEW',
                TableName: 'tblSpec',
                UpdateExpression: 'SET #message = :message, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now'
            });
        });

        it('should responds with normalized data and createdAt and updatedAt be different', done => {
            request.return(ReturnValues.ALL_NEW)
                .update({
                    buffer,
                    namespace,
                    id: 'id-0',
                    message: 'message-0'
                })
                .subscribe(response => {
                    expect(response.createdAt !== response.updatedAt).to.be.true;
                    expect(response).to.deep.equal({
                        buffer,
                        id: 'id-0',
                        createdAt: response.createdAt,
                        namespace,
                        updatedAt: response.updatedAt,
                        message: response.message
                    });
                }, null, done);
        });

        it('should throw if record doesn\'t exists and insert = false', done => {
            request.return(ReturnValues.ALL_NEW)
                .update({
                    namespace,
                    id: `id-${now}`,
                    message: `message-${now}`
                })
                .subscribe(null, err => {
                    expect(request.conditionExpression).to.equal('attribute_exists(#namespace)');
                    expect(err.message).to.equal('The conditional request failed');
                    done();
                });
        });

        it('should responds with normalized data if data exists and insert = true and createdAt and updatedAt be equals', done => {
            request.return(ReturnValues.ALL_NEW)
                .update({
                    buffer,
                    namespace,
                    id: `id-${now}`,
                    message: `message-${now}`
                }, null, true)
                .subscribe(response => {
                    expect(request.conditionExpression).to.be.null;
                    expect(response.createdAt === response.updatedAt).to.be.true;
                    expect(response).to.deep.equal({
                        buffer,
                        namespace,
                        id: response.id,
                        message: response.message,
                        createdAt: response.createdAt,
                        updatedAt: response.updatedAt
                    });
                }, null, done);
        });

        it('should return an Observable', () => {
            expect(request.update({})).to.be.instanceOf(rx.Observable);
        });
    });

    describe('delete', () => {
        it('should Keys be composed with just primary keys', () => {
            request.return(ReturnValues.ALL_OLD)
                .delete({
                    namespace,
                    id: 'id-0',
                    ignoredAttr: 'this attr should be ignored'
                });

            expect(request.routeCall.lastCall.args[1].Key).to.deep.equal({
                id: {
                    S: 'id-0'
                },
                namespace: {
                    S: 'spec'
                }
            });
        });

        it('should routeCall with deleteItem and relative params', () => {
            request.return(ReturnValues.ALL_OLD)
                .delete({
                    namespace,
                    id: 'id-0',
                    message: 'message-0'
                });

            expect(request.routeCall).to.have.been.calledOnce;
            expect(request.routeCall).to.have.been.calledWithExactly('deleteItem', {
                ConditionExpression: null,
                ExpressionAttributeNames: null,
                ExpressionAttributeValues: null,
                Key: {
                    id: {
                        S: 'id-0'
                    },
                    namespace: {
                        S: 'spec'
                    }
                },
                ReturnConsumedCapacity: 'TOTAL',
                ReturnValues: 'ALL_OLD',
                TableName: 'tblSpec'
            });
        });

        it('should responds with normalized data', done => {
            request.return(ReturnValues.ALL_OLD)
                .delete({
                    namespace,
                    id: 'id-0',
                    message: 'message-0'
                })
                .subscribe(response => {
                    expect(response).to.deep.equal({
                        buffer,
                        namespace,
                        id: 'id-0',
                        message: response.message,
                        createdAt: response.createdAt,
                        updatedAt: response.updatedAt
                    });
                }, null, done);
        });

        it('should responds null when record doesn\'t exists', done => {
            request.return(ReturnValues.ALL_OLD)
                .delete({
                    namespace,
                    id: `non-existent-id`
                })
                .subscribe(response => {
                    expect(response).to.be.null;
                }, null, done);
        });
    });

    describe('batchWrite', () => {
        it('should do jobs in steps with 25 operations max', () => {
            request.batchWrite(_.times(76, n => ({
                namespace,
                id: `batchWrite-${n}`,
                message: `message-${n}`
            })));

            expect(request.routeCall).to.have.been.callCount(4);
        });

        describe('toDelete', () => {
            beforeEach(done => {
                request.batchWrite(null, [{
                        namespace,
                        id: 'batchWrite-0',
                        message: 'message-0'
                    }, {
                        namespace,
                        id: 'batchWrite-1',
                        message: 'message-1'
                    }])
                    .subscribe(null, null, () => {
                        request.routeCall.resetHistory();
                        done();
                    });
            });

            it('should routeCall with batchWriteItem and relative params', () => {
                request.batchWrite([{
                    namespace,
                    id: 'batchWrite-0',
                    message: 'message-0'
                }, {
                    namespace,
                    id: 'batchWrite-1',
                    message: 'message-1'
                }]);

                expect(request.routeCall).to.have.been.calledWithExactly('batchWriteItem', {
                    RequestItems: {
                        tblSpec: [{
                            DeleteRequest: {
                                Key: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-0'
                                    }
                                }
                            }
                        }, {
                            DeleteRequest: {
                                Key: {
                                    id: {
                                        S: 'batchWrite-1'
                                    },
                                    namespace: {
                                        S: 'spec'
                                    }
                                }
                            }
                        }]
                    }
                });
            });

            it('should responds with normalized data', done => {
                request.batchWrite([{
                        namespace,
                        id: 'batchWrite-0',
                        message: 'message-0'
                    }, {
                        namespace,
                        id: 'batchWrite-1',
                        message: 'message-1'
                    }])
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            UnprocessedItems: {}
                        });
                    }, null, done);
            });
        });

        describe('toInsert', () => {
            it('should routeCall with batchWriteItem and relative params', () => {
                request.batchWrite(null, [{
                    namespace,
                    id: 'batchWrite-0',
                    message: 'message-0'
                }, {
                    namespace,
                    id: 'batchWrite-1',
                    message: 'message-1'
                }]);

                expect(request.routeCall).to.have.been.calledWithExactly('batchWriteItem', {
                    RequestItems: {
                        tblSpec: [{
                            PutRequest: {
                                Item: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-0'
                                    },
                                    message: {
                                        S: 'message-0'
                                    },
                                    createdAt: {
                                        N: sinon.match.string
                                    },
                                    updatedAt: {
                                        N: sinon.match.string
                                    }
                                }
                            }
                        }, {
                            PutRequest: {
                                Item: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-1'
                                    },
                                    message: {
                                        S: 'message-1'
                                    },
                                    createdAt: {
                                        N: sinon.match.string
                                    },
                                    updatedAt: {
                                        N: sinon.match.string
                                    }
                                }
                            }
                        }]
                    }
                });
            });

            it('should responds with normalized data', done => {
                request.batchWrite(null, [{
                        namespace,
                        id: 'batchWrite-0',
                        message: 'message-0'
                    }, {
                        namespace,
                        id: 'batchWrite-1',
                        message: 'message-1'
                    }])
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            UnprocessedItems: {}
                        });
                    }, null, done);
            });
        });

        describe('toDelete and toInsert', () => {
            it('should routeCall with batchWriteItem and relative params', () => {
                request.batchWrite([{
                    namespace,
                    id: 'batchWrite-0',
                    message: 'message-0'
                }, {
                    namespace,
                    id: 'batchWrite-1',
                    message: 'message-1'
                }], [{
                    namespace,
                    id: 'batchWrite-0',
                    message: 'message-0'
                }, {
                    namespace,
                    id: 'batchWrite-1',
                    message: 'message-1'
                }]);

                expect(request.routeCall).to.have.been.calledWithExactly('batchWriteItem', {
                    RequestItems: {
                        tblSpec: [{
                            DeleteRequest: {
                                Key: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-0'
                                    }
                                }
                            }
                        }, {
                            DeleteRequest: {
                                Key: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-1'
                                    }
                                }
                            }
                        }, {
                            PutRequest: {
                                Item: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-0'
                                    },
                                    message: {
                                        S: 'message-0'
                                    },
                                    createdAt: {
                                        N: sinon.match.string
                                    },
                                    updatedAt: {
                                        N: sinon.match.string
                                    }
                                }
                            }
                        }, {
                            PutRequest: {
                                Item: {
                                    namespace: {
                                        S: 'spec'
                                    },
                                    id: {
                                        S: 'batchWrite-1'
                                    },
                                    message: {
                                        S: 'message-1'
                                    },
                                    createdAt: {
                                        N: sinon.match.string
                                    },
                                    updatedAt: {
                                        N: sinon.match.string
                                    }
                                }
                            }
                        }]
                    }
                });
            });
        });
    });

    describe('batchGet', () => {
        beforeEach(done => {
            rx.range(0, 10)
                .pipe(
                    rxop.mergeMap(n => request.insert({
                        namespace,
                        id: `id-${n}`,
                        message: `message-${n}`,
                        localStringIndexedSortAttr: `local-indexed-${n}`
                    }, true))
                )
                .subscribe(null, null, done);

            request.routeCall.resetHistory();
        });

        it('should do jobs in steps with 100 operations max', () => {
            const items = _.times(176, n => ({
                namespace,
                id: `id-${n}`
            }));

            request.batchGet(items);

            expect(request.routeCall).to.have.been.calledTwice;
        });

        it('should routeCall with batchGet and relative params', () => {
            request.batchGet([{
                namespace,
                id: 'id-0'
            }, {
                namespace,
                id: 'id-1'
            }]);

            expect(request.routeCall).to.have.been.calledWithExactly('batchGetItem', {
                RequestItems: {
                    tblSpec: {
                        Keys: [{
                            namespace: {
                                S: 'spec'
                            },
                            id: {
                                S: 'id-0'
                            }
                        }, {
                            id: {
                                S: 'id-1'
                            },
                            namespace: {
                                S: 'spec'
                            }
                        }],
                        ConsistentRead: false,
                        ExpressionAttributeNames: null,
                        ProjectionExpression: undefined
                    }
                },
                ReturnConsumedCapacity: 'TOTAL'
            });
        });

        it('should responds with normalized data', done => {
            request.batchGet([{
                    namespace,
                    id: 'id-0'
                }, {
                    namespace,
                    id: 'id-1'
                }])
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        namespace,
                        localStringIndexedSortAttr: response[0].localStringIndexedSortAttr,
                        createdAt: response[0].createdAt,
                        message: response[0].message,
                        id: response[0].id,
                        updatedAt: response[0].updatedAt
                    }, {
                        namespace,
                        localStringIndexedSortAttr: response[1].localStringIndexedSortAttr,
                        createdAt: response[1].createdAt,
                        message: response[1].message,
                        id: response[1].id,
                        updatedAt: response[1].updatedAt
                    }]);
                }, null, done);
        });
    });

    describe('batchGetScan', () => {
        beforeEach(done => {
            rx.range(0, 10)
                .pipe(
                    rxop.mergeMap(n => request.insert({
                        namespace,
                        id: `id-${n}`,
                        message: `message-${n}`,
                        localStringIndexedSortAttr: `local-indexed-${n}`
                    }, true))
                )
                .subscribe(null, null, done);

            request.routeCall.resetHistory();
        });

        it('should run without scan', done => {
            const items = _.times(176, n => ({
                namespace,
                id: `id-${n}`
            }));

            request.batchGetScan(items)
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(_.size(response)).to.equal(10);
                }, null, done);
        });

        it('should call scan 2 times', done => {
            const items = _.times(176, n => ({
                namespace,
                id: `id-${n}`
            }));

            const scan = sinon.stub()
                .callsFake(response => {
                    return rx.of(response);
                });

            request.batchGetScan(items, scan)
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(scan).to.have.been.calledTwice;
                    expect(_.size(response)).to.equal(10);
                }, null, done);
        });

        it('should accept concurrency', done => {
            const items = _.times(176, n => ({
                namespace,
                id: `id-${n}`
            }));

            const scan = sinon.stub()
                .callsFake(response => {
                    return rx.of(response);
                });

            request.batchGetScan(items, scan, 100, 2)
                .pipe(
                    rxop.toArray(),
                    rxop.map(_.flatten)
                )
                .subscribe(response => {
                    expect(scan).to.have.been.calledTwice;
                    expect(_.size(response)).to.equal(10);
                }, null, done);
        });
    });
});