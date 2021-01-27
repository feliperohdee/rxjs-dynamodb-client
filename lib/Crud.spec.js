const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');

const rx = require('./rx');
const {
    Request,
    Select,
    Crud
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

describe('lib/Crud', () => {
    let request;
    let crud;

    before(done => {
        request = dynamoDb.table(tableName, tableSchema);
        crud = new Crud(tableName, tableSchema, {
            dynamoDb
        });

        request.describe()
            .pipe(
                rx.catchError(() => request.routeCall('createTable', {
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
                rx.mergeMap(() => {
                    return rx.range(0, 10)
                        .pipe(
                            rx.mergeMap(n => {
                                return request.insert({
                                    namespace,
                                    id: `id-${n}`,
                                    message: `message-${n}`,
                                    localStringIndexedSortAttr: `local-indexed-${n}`,
                                    localNumberIndexedSortAttr: n,
                                    globalIndexedPartitionAttr: `global-indexed-${namespace}`,
                                    globalStringIndexedSortAttr: `global-indexed-${n}`,
                                    globalNumberIndexedSortAttr: n
                                }, true);
                            })
                        );
                })
            )
            .subscribe(null, null, done);
    });

    after(done => {
        request = dynamoDb.table(tableName, tableSchema);

        request.query({
                namespace: 'spec'
            })
            .pipe(
                rx.toArray(),
                rx.mergeMap(response => request.batchWrite(response))
            )
            .subscribe(null, null, done);
    });

    beforeEach(() => {
        request = dynamoDb.table(tableName, tableSchema);
    });

    describe('constructor', () => {
        it('should throw if not deps.dynamoDb', () => {
            expect(() => new Crud()).to.throw('no dynamoDb client provided.');
        });
    });

    describe('request', () => {
        it('should returns and feed lastRequest', () => {
            const req = crud.request;

            expect(req).to.be.instanceOf(Request);
            expect(crud.lastRequest).to.equal(req);
        });
    });

    describe('partitionAttr', () => {
        it('should returns primaryKeys.partition', () => {
            expect(crud.partitionAttr).to.equal('namespace');
        });
    });

    describe('sortAttr', () => {
        it('should returns primaryKeys.sort', () => {
            expect(crud.sortAttr).to.equal('id');
        });
    });

    describe('globalIndexPartitionAttr', () => {
        it('should returns globalIndex.partition', () => {
            expect(crud.globalIndexPartitionAttr('globalStringIndex')).to.equal('globalIndexedPartitionAttr');
        });

        it('should returns null when wrong globalIndex', () => {
            expect(crud.globalIndexPartitionAttr('globalStringIndex_')).to.be.null;
        });
    });

    describe('globalIndexSortAttr', () => {
        it('should returns globalIndex.sort', () => {
            expect(crud.globalIndexSortAttr('globalStringIndex')).to.equal('globalStringIndexedSortAttr');

            expect(crud.globalIndexSortAttr('globalNumberIndex')).to.equal('globalNumberIndexedSortAttr');
        });

        it('should returns null when wrong globalIndex', () => {
            expect(crud.globalIndexSortAttr('globalStringIndex_')).to.be.null;
        });
    });

    describe('localIndexSortAttr', () => {
        it('should returns localIndex.sort', () => {
            expect(crud.localIndexSortAttr('localStringIndex')).to.equal('localStringIndexedSortAttr');

            expect(crud.localIndexSortAttr('localNumberIndex')).to.equal('localNumberIndexedSortAttr');
        });

        it('should returns null when no localIndex', () => {
            expect(crud.localIndexSortAttr()).to.be.null;
        });

        it('should returns null when wrong localIndex', () => {
            expect(crud.localIndexSortAttr('localNumberIndex_')).to.be.null;
        });

        it('should returns null when is global index', () => {
            expect(crud.localIndexSortAttr('globalStringIndex')).to.be.null;
        });
    });

    describe('fetch', () => {
        let items;
        let stats;

        beforeEach(done => {
            crud.fetch({
                    namespace: 'spec'
                })
                .subscribe(response => {
                    items = response.items;
                    stats = _.omit(response, ['items']);
                }, null, done);
        });

        it('should fetch with just namespace', () => {
            expect(items[0].id).to.equal('id-0');
            expect(stats.count).to.equal(10);
        });

        describe('without prefix', () => {
            beforeEach(done => {
                crud.fetch({
                        namespace: 'spec',
                        id: 'id-',
                        prefix: false
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should match exactly', () => {
                expect(stats.count).to.equal(0);
            });
        });

        describe('with Select.COUNT', () => {
            beforeEach(done => {
                crud.fetch({
                        namespace: 'spec',
                        select: Select.COUNT
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should return just stats', () => {
                expect(items).to.deep.equal([]);
                expect(stats).to.deep.equal({
                    before: null,
                    after: null,
                    consumedCapacity: 0,
                    count: 10,
                    scannedCount: 10,
                    iteractions: 1
                });
            });
        });

        describe('with itemSelector', () => {
            beforeEach(done => {
                const itemSelector = items => items.pipe(
                    rx.map(response => _.pick(response, [
                        'id'
                    ])));

                crud.fetch({
                        namespace: 'spec',
                        id: 'id-0'
                    }, null, itemSelector)
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should fetch and apply itemSelector', () => {
                expect(items[0]).to.deep.equal({
                    id: 'id-0'
                });

                expect(stats.count).to.equal(1);
            });
        });

        describe('with reducer', () => {
            beforeEach(done => {
                const reducer = items => items.pipe(
                    rx.toArray()
                );

                crud.fetch({
                        namespace: 'spec',
                        id: 'id-0'
                    }, null, null, reducer)
                    .subscribe(response => {
                        items = response;
                    }, null, done);
            });

            it('should fetch and apply reducer', () => {
                expect(items[0].id).to.equal('id-0');
            });
        });

        describe('without local index', () => {
            beforeEach(done => {
                crud.fetch({
                        namespace: 'spec',
                        id: 'id-3'
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should fetch with namespace and id', () => {
                expect(items[0].id).to.equal('id-3');
                expect(stats.count).to.equal(1);
            });
        });

        describe('with local index', () => {
            it('should fetch with namespace and string local index', done => {
                crud.fetch({
                        indexName: 'localStringIndex',
                        namespace: 'spec',
                        localStringIndexedSortAttr: 'local-indexed-3'
                    })
                    .subscribe(response => {
                        expect(response.items[0].localStringIndexedSortAttr).to.equal('local-indexed-3');
                        expect(response.count).to.equal(1);
                    }, null, done);
            });

            it('should fetch with namespace and number local index', done => {
                crud.fetch({
                        indexName: 'localNumberIndex',
                        namespace: 'spec',
                        localNumberIndexedSortAttr: 3
                    })
                    .subscribe(response => {
                        expect(response.items[0].localNumberIndexedSortAttr).to.equal(3);
                        expect(response.count).to.equal(1);
                    }, null, done);
            });
        });

        describe('with global index', () => {
            it('should fetch with namespace and string global index', done => {
                crud.fetch({
                        indexName: 'globalStringIndex',
                        globalIndexedPartitionAttr: 'global-indexed-spec',
                        globalStringIndexedSortAttr: 'global-indexed-3'
                    })
                    .subscribe(response => {
                        expect(response.items[0].globalStringIndexedSortAttr).to.equal('global-indexed-3');
                        expect(response.count).to.equal(1);
                    }, null, done);
            });

            it('should fetch with namespace and number global index', done => {
                crud.fetch({
                        indexName: 'globalNumberIndex',
                        globalIndexedPartitionAttr: 'global-indexed-spec',
                        globalNumberIndexedSortAttr: 3
                    })
                    .subscribe(response => {
                        expect(response.items[0].globalNumberIndexedSortAttr).to.equal(3);
                        expect(response.count).to.equal(1);
                    }, null, done);
            });
        });

        describe('before', () => {
            let items;
            let stats;

            beforeEach(done => {
                crud.fetch({
                        limit: 2,
                        namespace: 'spec',
                        resume: crud.toBase64(JSON.stringify({
                            namespace,
                            id: 'id-6'
                        }))
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should query before based on before', done => {
                const query = before => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    before
                });

                expect(items[0].id).to.equal('id-7');
                expect(items[1].id).to.equal('id-8');

                query(stats.before)
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-5');
                            expect(response.items[1].id).to.equal('id-6');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-3');
                            expect(response.items[1].id).to.equal('id-4');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-1');
                            expect(response.items[1].id).to.equal('id-2');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-0');
                            expect(response.before).to.be.null;
                            expect(response.count).to.equal(1);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });

            it('should query before desc based on before', done => {
                const query = before => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    before,
                    desc: true
                });

                expect(items[0].id).to.equal('id-7');
                expect(items[1].id).to.equal('id-8');

                query(stats.before)
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-6');
                            expect(response.items[1].id).to.equal('id-5');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-4');
                            expect(response.items[1].id).to.equal('id-3');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-2');
                            expect(response.items[1].id).to.equal('id-1');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-0');
                            expect(response.before).to.be.null;
                            expect(response.count).to.equal(1);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });

            it('should query before "last"', done => {
                const query = before => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    before: before || 'last'
                });

                query()
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-8');
                            expect(response.items[1].id).to.equal('id-9');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-6');
                            expect(response.items[1].id).to.equal('id-7');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-4');
                            expect(response.items[1].id).to.equal('id-5');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-2');
                            expect(response.items[1].id).to.equal('id-3');
                            expect(response.count).to.equal(2);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });

            it('should query desc before "last"', done => {
                const query = before => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    desc: true,
                    before: before || 'last'
                });

                query()
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-9');
                            expect(response.items[1].id).to.equal('id-8');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-7');
                            expect(response.items[1].id).to.equal('id-6');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-5');
                            expect(response.items[1].id).to.equal('id-4');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.before)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-3');
                            expect(response.items[1].id).to.equal('id-2');
                            expect(response.count).to.equal(2);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });
        });

        describe('after', () => {
            let items;
            let stats;

            beforeEach(done => {
                crud.fetch({
                        limit: 2,
                        namespace: 'spec',
                        resume: crud.toBase64(JSON.stringify({
                            namespace,
                            id: 'id-2'
                        }))
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should query after based on after', done => {
                const query = after => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    after
                });

                expect(items[0].id).to.equal('id-3');
                expect(items[1].id).to.equal('id-4');

                query(stats.after)
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-5');
                            expect(response.items[1].id).to.equal('id-6');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-7');
                            expect(response.items[1].id).to.equal('id-8');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-9');
                            expect(response.after).to.be.null;
                            expect(response.count).to.equal(1);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });

            it('should query after desc based on after', done => {
                const query = after => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    after,
                    desc: true
                });

                expect(items[0].id).to.equal('id-3');
                expect(items[1].id).to.equal('id-4');

                query(stats.after)
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-6');
                            expect(response.items[1].id).to.equal('id-5');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-8');
                            expect(response.items[1].id).to.equal('id-7');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-9');
                            expect(response.after).to.be.null;
                            expect(response.count).to.equal(1);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });

            it('should query after "first"', done => {
                const query = after => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    after: after || 'first'
                });

                query()
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-0');
                            expect(response.items[1].id).to.equal('id-1');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-2');
                            expect(response.items[1].id).to.equal('id-3');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-4');
                            expect(response.items[1].id).to.equal('id-5');
                            expect(response.count).to.equal(2);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });

            it('should query desc after "first"', done => {
                const query = after => crud.fetch({
                    limit: 2,
                    namespace: 'spec',
                    desc: true,
                    after: after || 'first'
                });

                query()
                    .pipe(
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-1');
                            expect(response.items[1].id).to.equal('id-0');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-3');
                            expect(response.items[1].id).to.equal('id-2');
                            expect(response.count).to.equal(2);
                        }),
                        rx.mergeMap(response => query(response.after)),
                        rx.tap(response => {
                            expect(response.items[0].id).to.equal('id-5');
                            expect(response.items[1].id).to.equal('id-4');
                            expect(response.count).to.equal(2);
                        })
                    )
                    .subscribe(response => {}, null, done);
            });
        });

        describe('resume', () => {
            let items;
            let stats;

            beforeEach(done => {
                crud.fetch({
                        limit: 5,
                        namespace: 'spec'
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should get correct before and after', () => {
                expect(_.last(items).id).to.equal('id-4');

                expect(JSON.parse(crud.fromBase64(stats.before))).to.be.null;
                expect(JSON.parse(crud.fromBase64(stats.after))).to.deep.equal({
                    namespace: 'spec',
                    id: 'id-4'
                });

                expect(stats.count).to.equal(5);
            });

            it('should resume based on after and feed before', done => {
                crud.fetch({
                        limit: 5,
                        namespace: 'spec',
                        resume: stats.after
                    })
                    .subscribe(response => {
                        expect(response.items[0].id).to.equal('id-5');

                        expect(JSON.parse(crud.fromBase64(response.before))).to.deep.equal({
                            namespace: 'spec',
                            id: 'id-5'
                        });
                        expect(JSON.parse(crud.fromBase64(response.after))).to.be.null;

                        expect(response.count).to.equal(5);
                    }, null, done);
            });

            describe('with local index', () => {
                beforeEach(done => {
                    crud.fetch({
                            limit: 5,
                            indexName: 'localStringIndex',
                            namespace: 'spec',
                            select: 'localStringIndexedSortAttr'
                        })
                        .subscribe(response => {
                            items = response.items;
                            stats = _.omit(response, ['items']);
                        }, null, done);
                });

                it('should get correct after', () => {
                    expect(_.last(items).localStringIndexedSortAttr).to.equal('local-indexed-4');

                    expect(JSON.parse(crud.fromBase64(stats.before))).to.be.null;
                    expect(JSON.parse(crud.fromBase64(stats.after))).to.deep.equal({
                        namespace: 'spec',
                        id: 'id-4',
                        localStringIndexedSortAttr: 'local-indexed-4'
                    });

                    expect(stats.count).to.equal(5);
                });

                it('should resume based on after', done => {
                    crud.fetch({
                            limit: 5,
                            indexName: 'localStringIndex',
                            namespace: 'spec',
                            select: 'localStringIndexedSortAttr',
                            resume: stats.after
                        })
                        .subscribe(response => {
                            expect(response.items[0].localStringIndexedSortAttr).to.equal('local-indexed-5');

                            expect(JSON.parse(crud.fromBase64(response.before))).to.deep.equal({
                                namespace: 'spec',
                                id: 'id-5',
                                localStringIndexedSortAttr: 'local-indexed-5'
                            });
                            expect(JSON.parse(crud.fromBase64(response.after))).to.be.null;

                            expect(response.count).to.equal(5);
                        }, null, done);
                });
            });

            describe('with global index', () => {
                beforeEach(done => {
                    crud.fetch({
                            limit: 5,
                            indexName: 'globalStringIndex',
                            globalIndexedPartitionAttr: 'global-indexed-spec',
                            select: 'globalIndexedPartitionAttr'
                        })
                        .subscribe(response => {
                            items = response.items;
                            stats = _.omit(response, ['items']);
                        }, null, done);
                });

                it('should get correct after', () => {
                    expect(items[0].globalStringIndexedSortAttr).to.equal('global-indexed-0');

                    expect(JSON.parse(crud.fromBase64(stats.before))).to.be.null;
                    expect(JSON.parse(crud.fromBase64(stats.after))).to.deep.equal({
                        namespace: 'spec',
                        id: 'id-4',
                        globalStringIndexedSortAttr: 'global-indexed-4',
                        globalIndexedPartitionAttr: 'global-indexed-spec'
                    });

                    expect(stats.count).to.equal(5);
                });

                it('should resume based on after', done => {
                    crud.fetch({
                            limit: 5,
                            indexName: 'globalStringIndex',
                            globalIndexedPartitionAttr: 'global-indexed-spec',
                            select: 'globalIndexedPartitionAttr',
                            resume: stats.after
                        })
                        .subscribe(response => {
                            expect(response.items[0].globalStringIndexedSortAttr).to.equal('global-indexed-5');
                            expect(JSON.parse(crud.fromBase64(response.before))).to.deep.equal({
                                namespace: 'spec',
                                id: 'id-5',
                                globalStringIndexedSortAttr: 'global-indexed-5',
                                globalIndexedPartitionAttr: 'global-indexed-spec'
                            });
                            expect(JSON.parse(crud.fromBase64(response.after))).to.be.null;
                            expect(response.count).to.equal(5);
                        }, null, done);
                });
            });
        });

        describe('desc', () => {
            beforeEach(done => {
                crud.fetch({
                        desc: true,
                        namespace: 'spec'
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should fetch desc', () => {
                expect(items[0].id).to.equal('id-9');
                expect(stats.count).to.equal(10);
            });
        });

        describe('select', () => {
            beforeEach(done => {
                crud.fetch({
                        select: 'id',
                        namespace: 'spec'
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should fetch just namespace and id', () => {
                expect(items[0]).to.deep.equal({
                    namespace: 'spec',
                    id: 'id-0'
                });

                expect(stats.count).to.equal(10);
            });
        });

        describe('limit', () => {
            beforeEach(done => {
                crud.fetch({
                        limit: 1,
                        namespace: 'spec'
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            it('should fetch one', () => {
                expect(stats.count).to.equal(1);
            });
        });

        describe('consistent', () => {
            beforeEach(done => {
                sinon.spy(Request.prototype, 'consistent');

                crud.fetch({
                        consistent: true,
                        namespace: 'spec'
                    })
                    .subscribe(response => {
                        items = response.items;
                        stats = _.omit(response, ['items']);
                    }, null, done);
            });

            afterEach(() => {
                Request.prototype.consistent.restore();
            });

            it('should fetch one', () => {
                expect(Request.prototype.consistent).to.have.been.calledOnce;
            });
        });

        describe('hook', () => {
            let callback;
            let query;

            beforeEach(() => {
                callback = sinon.stub();

                crud.fetch({
                    namespace: 'spec'
                }, ({
                    expression,
                    request
                }) => {
                    callback(expression);

                    query = sinon.spy(request, 'query');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                query.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('#partition = :partition');
            });

            it('should query be called with hookArgs', () => {
                expect(query).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('get', () => {
        let item;

        beforeEach(done => {
            crud.get({
                    namespace: 'spec',
                    id: 'id-3'
                })
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        it('should get', () => {
            expect(item.id).to.equal('id-3');
        });

        describe('consistent', () => {
            beforeEach(done => {
                sinon.spy(Request.prototype, 'consistent');

                crud.get({
                        consistent: true,
                        namespace: 'spec',
                        id: 'id-0'
                    })
                    .subscribe(null, null, done);
            });

            afterEach(() => {
                Request.prototype.consistent.restore();
            });

            it('should fetch one', () => {
                expect(Request.prototype.consistent).to.have.been.calledOnce;
            });
        });

        describe('select', () => {
            beforeEach(done => {
                crud.get({
                        select: 'id',
                        namespace: 'spec',
                        id: 'id-0'
                    })
                    .subscribe(response => {
                        item = response;
                    }, null, done);
            });

            it('should get', () => {
                expect(item).to.deep.equal({
                    namespace: 'spec',
                    id: 'id-0'
                });
            });
        });

        describe('hook', () => {
            let callback;
            let get;

            beforeEach(() => {
                callback = sinon.stub();

                crud.get({
                    namespace: 'spec',
                    id: 'id-3'
                }, ({
                    partition,
                    sort,
                    request
                }) => {
                    callback({
                        partition,
                        sort
                    });

                    get = sinon.spy(request, 'get');

                    return ['hooked'];
                });
            });

            afterEach(() => {
                get.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    partition: 'spec',
                    sort: 'id-3'
                });
            });

            it('should query be called with hookArgs', () => {
                expect(get).to.have.been.calledWith('hooked');
            });
        });
    });

    describe('insert', () => {
        let item;

        beforeEach(done => {
            crud.insert({
                    namespace: 'spec',
                    id: 'id-10'
                })
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        afterEach(done => {
            crud.delete({
                    namespace: 'spec',
                    id: 'id-10'
                })
                .subscribe(null, null, done);
        });

        it('should return inserted item', () => {
            expect(item.id).to.equal('id-10');
            expect(item).to.have.all.keys([
                'namespace',
                'id',
                'createdAt',
                'updatedAt'
            ]);
        });

        describe('hook', () => {
            let callback;
            let insert;

            beforeEach(() => {
                callback = sinon.stub();

                crud.insert({
                    namespace: 'spec',
                    id: 'id-10',
                    title: 'title'
                }, ({
                    partition,
                    sort,
                    args,
                    request
                }) => {
                    callback({
                        partition,
                        sort,
                        args
                    });

                    insert = sinon.spy(request, 'insert');

                    return [{
                        hooked: true
                    }];
                });
            });

            afterEach(() => {
                insert.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    partition: 'spec',
                    sort: 'id-10',
                    args: {
                        namespace: 'spec',
                        id: 'id-10',
                        title: 'title'
                    }
                });
            });

            it('should query be called with hookArgs', () => {
                expect(insert).to.have.been.calledWith({
                    hooked: true
                });
            });
        });
    });

    describe('insertOrReplace', () => {
        let item;

        beforeEach(done => {
            crud.insertOrReplace({
                    namespace: 'spec',
                    id: 'id-0'
                })
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        afterEach(done => {
            crud.delete({
                    namespace: 'spec',
                    id: 'id-10'
                })
                .subscribe(null, null, done);
        });

        it('should return inserted or replaced item', () => {
            expect(item.id).to.equal('id-0');
            expect(item.createdAt).to.equal(item.updatedAt);
            expect(item).to.have.all.keys([
                'namespace',
                'id',
                'createdAt',
                'updatedAt'
            ]);
        });

        describe('hook', () => {
            let callback;
            let insertOrReplace;

            beforeEach(() => {
                callback = sinon.stub();

                crud.insertOrReplace({
                    namespace: 'spec',
                    id: 'id-0',
                    title: 'title'
                }, ({
                    partition,
                    sort,
                    args,
                    request
                }) => {
                    callback({
                        partition,
                        sort,
                        args
                    });

                    insertOrReplace = sinon.spy(request, 'insertOrReplace');

                    return [{
                        hooked: true
                    }];
                });
            });

            afterEach(() => {
                insertOrReplace.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    partition: 'spec',
                    sort: 'id-0',
                    args: {
                        namespace: 'spec',
                        id: 'id-0',
                        title: 'title'
                    }
                });
            });

            it('should query be called with hookArgs', () => {
                expect(insertOrReplace).to.have.been.calledWith({
                    hooked: true
                });
            });
        });
    });

    describe('insertOrUpdate', () => {
        let item;

        beforeEach(done => {
            crud.insertOrUpdate({
                    namespace: 'spec',
                    id: 'id-1',
                    title: 'title'
                })
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        it('should return inserted or updated item', () => {
            expect(item.id).to.equal('id-1');
            expect(item.createdAt)
                .not.to.equal(item.updatedAt);
            expect(item.createdAt).to.be.below(item.updatedAt);
            expect(item).to.have.all.keys([
                'globalIndexedPartitionAttr',
                'globalNumberIndexedSortAttr',
                'globalStringIndexedSortAttr',
                'localNumberIndexedSortAttr',
                'localStringIndexedSortAttr',
                'namespace',
                'id',
                'title',
                'message',
                'createdAt',
                'updatedAt'
            ]);
        });

        it('should return old item', done => {
            crud.insertOrUpdate({
                    namespace: 'spec',
                    id: 'id-1',
                    title: 'title-1'
                }, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.id).to.equal('id-1');
                    expect(response.title).to.equal('title');
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let insertOrUpdate;

            beforeEach(() => {
                callback = sinon.stub();

                crud.insertOrUpdate({
                    namespace: 'spec',
                    id: 'id-1',
                    title: 'title'
                }, 'ALL_NEW', ({
                    partition,
                    sort,
                    args,
                    request
                }) => {
                    callback({
                        partition,
                        sort,
                        args
                    });

                    insertOrUpdate = sinon.spy(request, 'insertOrUpdate');

                    return [{
                        hooked: true
                    }];
                });
            });

            afterEach(() => {
                insertOrUpdate.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    partition: 'spec',
                    sort: 'id-1',
                    args: {
                        namespace: 'spec',
                        id: 'id-1',
                        title: 'title'
                    }
                });
            });

            it('should query be called with hookArgs', () => {
                expect(insertOrUpdate).to.have.been.calledWith({
                    hooked: true
                });
            });
        });
    });

    describe('update', () => {
        let item;

        beforeEach(done => {
            crud.update({
                    namespace: 'spec',
                    id: 'id-2',
                    title: 'title'
                })
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        it('should return updated item', () => {
            expect(item.id).to.equal('id-2');
            expect(item.createdAt)
                .not.to.equal(item.updatedAt);
            expect(item.createdAt).to.be.below(item.updatedAt);
            expect(item).to.have.all.keys([
                'globalIndexedPartitionAttr',
                'globalNumberIndexedSortAttr',
                'globalStringIndexedSortAttr',
                'localNumberIndexedSortAttr',
                'localStringIndexedSortAttr',
                'namespace',
                'id',
                'title',
                'message',
                'createdAt',
                'updatedAt'
            ]);
        });

        it('should return old item', done => {
            crud.update({
                    namespace: 'spec',
                    id: 'id-2',
                    title: 'title-1'
                }, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.id).to.equal('id-2');
                    expect(response.title).to.equal('title');
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.update({
                    namespace: 'spec',
                    id: 'id-2',
                    title: 'title'
                }, 'ALL_NEW', ({
                    partition,
                    sort,
                    args,
                    request
                }) => {
                    callback({
                        partition,
                        sort,
                        args
                    });

                    update = sinon.spy(request, 'update');

                    return [{
                        hooked: true
                    }];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    partition: 'spec',
                    sort: 'id-2',
                    args: {
                        namespace: 'spec',
                        id: 'id-2',
                        title: 'title'
                    }
                });
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith({
                    hooked: true
                });
            });
        });
    });

    describe('updatePrimaryKeys', () => {
        let oldResult;
        let result;

        beforeEach(done => {
            sinon.spy(crud, 'delete');
            sinon.spy(Request.prototype, 'insert');

            crud.get({
                    namespace,
                    id: 'id-0'
                })
                .pipe(
                    rx.tap(response => oldResult = response),
                    rx.mergeMap(() => crud.updatePrimaryKeys({
                        namespace,
                        id: 'id-0'
                    }, {
                        id: 'id-00',
                        forbidden: 'forbidden'
                    }))
                )
                .subscribe(response => {
                    result = response;
                }, null, done);
        });

        afterEach(done => {
            crud.delete.restore();
            Request.prototype.insert.restore();

            crud.updatePrimaryKeys({
                    namespace,
                    id: 'id-00'
                }, {
                    id: 'id-0'
                })
                .subscribe(null, null, done);
        });

        it('should call crud.delete', () => {
            expect(crud.delete).to.have.been.calledWith({
                id: 'id-0',
                namespace
            });
        });

        it('should call crud.insert', () => {
            expect(Request.prototype.insert).to.have.been.calledWith({
                ..._.omit(oldResult, ['updatedAt']),
                id: 'id-00'
            }, false, true);
        });

        it('should return with same createdAt', () => {
            expect(oldResult.updatedAt < result.updatedAt).to.be.true;
            expect(result).to.deep.equal({
                ...oldResult,
                id: 'id-00',
                updatedAt: result.updatedAt
            });
        });
    });

    describe('delete', () => {
        let item;

        beforeEach(done => {
            crud.insertOrReplace({
                    namespace: 'spec',
                    id: 'id-3'
                })
                .pipe(
                    rx.mergeMap(() => crud.delete({
                        namespace: 'spec',
                        id: 'id-3'
                    }))
                )
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        it('should return deleted item', () => {
            expect(item.id).to.equal('id-3');
            expect(item).to.have.all.keys([
                'namespace',
                'id',
                'createdAt',
                'updatedAt'
            ]);
        });

        describe('hook', () => {
            let callback;
            let del;

            beforeEach(() => {
                callback = sinon.stub();

                crud.delete({
                    namespace: 'spec',
                    id: 'id-3'
                }, ({
                    partition,
                    sort,
                    request
                }) => {
                    callback({
                        partition,
                        sort
                    });

                    del = sinon.spy(request, 'delete');

                    return [{
                        hooked: true
                    }];
                });
            });

            afterEach(() => {
                del.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    partition: 'spec',
                    sort: 'id-3'
                });
            });

            it('should query be called with hookArgs', () => {
                expect(del).to.have.been.calledWith({
                    hooked: true
                });
            });
        });
    });

    describe('appendToList', () => {
        let item;

        beforeEach(done => {
            crud.insertOrReplace({
                    deep: {},
                    namespace: 'spec',
                    id: 'id-0'
                })
                .pipe(
                    rx.mergeMap(() => crud.appendToList({
                        namespace,
                        id: 'id-0',
                        list: [{
                            a: 1
                        }, {
                            b: 2
                        }],
                        'deep.list': [{
                            a: 1
                        }, {
                            b: 2
                        }]
                    }))
                )
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        it('should not create item', done => {
            crud.appendToList({
                    namespace,
                    id: 'inexistentAppendList',
                    list: [{
                        a: 1
                    }, {
                        b: 2
                    }]
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('The conditional request failed');
                    done();
                });
        });

        it('should create item if create=true', done => {
            crud.appendToList({
                    namespace,
                    id: 'inexistentAppendList',
                    list: [{
                        a: 1
                    }, {
                        b: 2
                    }]
                }, true)
                .subscribe(response => {
                    expect(response.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }]);
                }, null, done);
        });

        it('should create a list', () => {
            expect(item.deep.list).to.deep.equal([{
                a: 1
            }, {
                b: 2
            }]);

            expect(item.list).to.deep.equal([{
                a: 1
            }, {
                b: 2
            }]);
        });

        it('should append array', done => {
            crud.appendToList({
                    'deep.list': [{
                        c: 3
                    }, {
                        d: 4
                    }],
                    namespace,
                    id: 'id-0',
                    list: [{
                        c: 3
                    }, {
                        d: 4
                    }]
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }, {
                        c: 3
                    }, {
                        d: 4
                    }]);

                    expect(response.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }, {
                        c: 3
                    }, {
                        d: 4
                    }]);
                }, null, done);
        });

        it('should append non array', done => {
            crud.appendToList({
                    'deep.list': {
                        c: 3
                    },
                    namespace,
                    id: 'id-0',
                    list: {
                        c: 3
                    }
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }, {
                        c: 3
                    }]);

                    expect(response.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }, {
                        c: 3
                    }]);
                }, null, done);
        });

        it('should return old item', done => {
            crud.appendToList({
                    namespace,
                    id: 'id-0',
                    list: [{
                        c: 3
                    }, {
                        d: 4
                    }]
                }, false, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }]);

                    expect(response.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }]);
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.appendToList({
                    namespace,
                    id: 'id-0',
                    list: [{
                        e: 5
                    }, {
                        f: 6
                    }]
                }, false, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression.replace(/:appendList_\w*/g, ':appendList_{cuid}'));

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('SET #list = list_append(if_not_exists(#list, :emptyList), :appendList_{cuid}), #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('prependToList', () => {
        let item;

        beforeEach(done => {
            crud.insertOrReplace({
                    namespace: 'spec',
                    id: 'id-0'
                })
                .pipe(
                    rx.mergeMap(() => crud.prependToList({
                        namespace,
                        id: 'id-0',
                        list: [{
                            a: 1
                        }, {
                            b: 2
                        }]
                    }))
                )
                .subscribe(response => {
                    item = response;
                }, null, done);
        });

        it('should not create item', done => {
            crud.prependToList({
                    namespace,
                    id: 'inexistentPrependList',
                    list: [{
                        a: 1
                    }, {
                        b: 2
                    }]
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('The conditional request failed');
                    done();
                });
        });

        it('should create item if create=true', done => {
            crud.prependToList({
                    namespace,
                    id: 'inexistentPrependList',
                    list: [{
                        a: 1
                    }, {
                        b: 2
                    }]
                }, true)
                .subscribe(response => {
                    expect(item.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }]);
                }, null, done);
        });

        it('should create a list', () => {
            expect(item.list).to.deep.equal([{
                a: 1
            }, {
                b: 2
            }]);
        });

        it('should prepend array', done => {
            crud.prependToList({
                    namespace,
                    id: 'id-0',
                    list: [{
                        c: 3
                    }, {
                        d: 4
                    }]
                })
                .subscribe(response => {
                    expect(response.list).to.deep.equal([{
                        c: 3
                    }, {
                        d: 4
                    }, {
                        a: 1
                    }, {
                        b: 2
                    }]);
                }, null, done);
        });

        it('should prepend non array', done => {
            crud.prependToList({
                    namespace,
                    id: 'id-0',
                    list: {
                        c: 3
                    }
                })
                .subscribe(response => {
                    expect(response.list).to.deep.equal([{
                        c: 3
                    }, {
                        a: 1
                    }, {
                        b: 2
                    }]);
                }, null, done);
        });

        it('should return old item', done => {
            crud.prependToList({
                    namespace,
                    id: 'id-0',
                    list: [{
                        c: 3
                    }, {
                        d: 4
                    }]
                }, false, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.list).to.deep.equal([{
                        a: 1
                    }, {
                        b: 2
                    }]);
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.prependToList({
                    namespace,
                    id: 'id-0',
                    list: [{
                        e: 5
                    }, {
                        f: 6
                    }]
                }, false, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression.replace(/:appendList_\w*/g, ':appendList_{cuid}'));

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('SET #list = list_append(:appendList_{cuid}, if_not_exists(#list, :emptyList)), #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('removeFromList', () => {
        beforeEach(done => {
            crud.insertOrReplace({
                    deep: {
                        list: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                    },
                    namespace: 'spec',
                    id: 'id-0',
                    list: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                })
                .subscribe(null, null, done);
        });

        it('should pull with array', done => {
            crud.removeFromList({
                    'deep.list': [0, 1],
                    namespace,
                    id: 'id-0',
                    list: [0, 1]
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([2, 3, 4, 5, 6, 7, 8, 9]);
                    expect(response.list).to.deep.equal([2, 3, 4, 5, 6, 7, 8, 9]);
                }, null, done);
        });

        it('should pull with single value', done => {
            crud.removeFromList({
                    'deep.list': 0,
                    namespace,
                    id: 'id-0',
                    list: 0
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    expect(response.list).to.deep.equal([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                }, null, done);
        });

        it('should not pull one empty value', done => {
            crud.removeFromList({
                    'deep.list': [],
                    namespace,
                    id: 'id-0',
                    list: [0]
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    expect(response.list).to.deep.equal([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                }, null, done);
        });

        it('should not pull all empty values', done => {
            crud.removeFromList({
                    'deep.list': [],
                    namespace,
                    id: 'id-0',
                    list: []
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    expect(response.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                }, null, done);
        });

        it('should return old item', done => {
            crud.removeFromList({
                    'deep.list': 0,
                    namespace,
                    id: 'id-0',
                    list: 0
                }, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    expect(response.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.removeFromList({
                    namespace,
                    id: 'id-0',
                    list: [2, 3]
                }, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression);

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('REMOVE #list[2],#list[3] SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('updateAtList', () => {
        beforeEach(done => {
            crud.insertOrReplace({
                    deep: {
                        list: [0, 1, 2, 3, 4, 5, 6, 7, {
                            a: 1
                        }]
                    },
                    namespace: 'spec',
                    id: 'id-0',
                    list: [0, 1, 2, 3, 4, 5, 6, 7, {
                        a: 1
                    }]
                })
                .subscribe(null, null, done);
        });

        it('should update with primaries', done => {
            crud.updateAtList({
                    'deep.list': {
                        0: 'updated 0',
                        2: 'updated 2'
                    },
                    namespace,
                    id: 'id-0',
                    list: {
                        0: 'updated 0',
                        2: 'updated 2'
                    }
                })
                .subscribe(response => {
                    expect(response.list).to.deep.equal(['updated 0', 1, 'updated 2', 3, 4, 5, 6, 7, {
                        a: 1
                    }]);
                }, null, done);
        });

        it('should update with object path', done => {
            crud.updateAtList({
                    'deep.list': {
                        8: {
                            a: 2,
                            b: 3
                        }
                    },
                    namespace,
                    id: 'id-0',
                    list: {
                        8: {
                            a: 2,
                            b: 3
                        }
                    }
                })
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, {
                        a: 2,
                        b: 3
                    }]);
                    expect(response.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, {
                        a: 2,
                        b: 3
                    }]);
                }, null, done);
        });

        it('should return old item', done => {
            crud.updateAtList({
                    'deep.list': {
                        8: {
                            a: 2
                        }
                    },
                    namespace,
                    id: 'id-0',
                    list: {
                        8: {
                            a: 2
                        }
                    }
                }, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.deep.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, {
                        a: 1
                    }]);
                    expect(response.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, {
                        a: 1
                    }]);
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.updateAtList({
                    namespace,
                    id: 'id-0',
                    list: {
                        5: 'updated 6'
                    }
                }, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression.replace(/:\w{7,8}/g, ':{cuid}'));

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('SET #list[5] = :{cuid}, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('addToSet', () => {
        beforeEach(done => {
            crud.insertOrReplace({
                    namespace: 'spec',
                    id: 'id-0'
                })
                .subscribe(null, null, done);
        });

        it('should not create item', done => {
            crud.addToSet({
                    namespace,
                    id: 'inexistentSet',
                    set: ['a', 'b']
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('The conditional request failed');
                    done();
                });
        });

        it('should create item if create=true', done => {
            crud.addToSet({
                    namespace,
                    id: 'inexistentSet',
                    set: ['a', 'b']
                }, true)
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a', 'b']);
                }, null, done);
        });

        it('should create a string set with array', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b']
                })
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a', 'b']);
                }, null, done);
        });

        it('should create a number set with array', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: [1, 2]
                })
                .subscribe(response => {
                    expect(response.set).to.deep.equal([1, 2]);
                }, null, done);
        });

        it('should create a string set with single value', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: 'a'
                })
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a']);
                }, null, done);
        });

        it('should create a number set with single value', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: 1
                })
                .subscribe(response => {
                    expect(response.set).to.deep.equal([1]);
                }, null, done);
        });

        it('should not duplicate', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b']
                })
                .pipe(
                    rx.mergeMap(() => crud.addToSet({
                        namespace,
                        id: 'id-0',
                        set: ['a', 'b']
                    }))
                )
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a', 'b']);
                }, null, done);
        });

        it('should not create a set', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: [1, 'b']
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('Invalid UpdateExpression: Syntax error; token: "SET", near: "ADD  SET #createdAt"');

                    done();
                });
        });

        it('should return old item', done => {
            crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b']
                })
                .pipe(
                    rx.mergeMap(() => crud.addToSet({
                        namespace,
                        id: 'id-0',
                        set: 'c'
                    }, false, 'ALL_OLD'))
                )
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a', 'b']);
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.addToSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b']
                }, false, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression);

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('ADD #set :set SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('removeFromSet', () => {
        beforeEach(done => {
            crud.insertOrReplace({
                    namespace: 'spec',
                    id: 'id-0'
                })
                .pipe(
                    rx.mergeMap(() => crud.addToSet({
                        namespace,
                        id: 'id-0',
                        set: ['a', 'b', 'c', 'd']
                    }))
                )
                .subscribe(null, null, done);
        });

        it('should pull', done => {
            crud.removeFromSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b']
                })
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['c', 'd']);
                }, null, done);
        });

        it('should remove attribute is set is empty', done => {
            crud.removeFromSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b', 'c', 'd']
                })
                .subscribe(response => {
                    expect(response.set).to.be.undefined;
                }, null, done);
        });

        it('should do nothing if inexistent', done => {
            crud.removeFromSet({
                    namespace,
                    id: 'id-0',
                    set: ['e', 'f']
                })
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a', 'b', 'c', 'd']);
                }, null, done);
        });

        it('should return old item', done => {
            crud.removeFromSet({
                    namespace,
                    id: 'id-0',
                    set: 'd'
                }, 'ALL_OLD')
                .subscribe(response => {
                    expect(response.set).to.deep.equal(['a', 'b', 'c', 'd']);
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.removeFromSet({
                    namespace,
                    id: 'id-0',
                    set: ['a', 'b']
                }, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression);

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('DELETE #set :set SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('removeAttributes', () => {
        beforeEach(done => {
            crud.insertOrReplace({
                    deep: {
                        a: {
                            b: 'b'
                        }
                    },
                    deep2: {
                        a: {
                            b: 'b'
                        }
                    },
                    namespace: 'spec',
                    id: 'id-0',
                    title: 'title'
                })
                .subscribe(null, null, done);
        });

        it('should remove attribute', done => {
            crud.removeAttributes({
                    'deep.a': true,
                    'deep2.a.b': true,
                    namespace: 'spec',
                    id: 'id-0',
                    title: true
                })
                .subscribe(response => {
                    expect(response).to.deep.equal({
                        deep: {},
                        deep2: {
                            a: {}
                        },
                        namespace: 'spec',
                        id: 'id-0',
                        updatedAt: response.updatedAt,
                        createdAt: response.createdAt
                    });
                }, null, done);
        });

        it('should return old item', done => {
            crud.removeAttributes({
                    'deep.a': true,
                    'deep2.a.b': true,
                    namespace: 'spec',
                    id: 'id-0',
                    title: true
                }, 'ALL_OLD')
                .subscribe(response => {
                    expect(response).to.deep.equal({
                        deep: {
                            a: {
                                b: 'b'
                            }
                        },
                        deep2: {
                            a: {
                                b: 'b'
                            }
                        },
                        namespace: 'spec',
                        id: 'id-0',
                        title: 'title',
                        updatedAt: response.updatedAt,
                        createdAt: response.createdAt
                    });
                }, null, done);
        });

        describe('hook', () => {
            let callback;
            let update;

            beforeEach(() => {
                callback = sinon.stub();

                crud.removeAttributes({
                    namespace: 'spec',
                    id: 'id-0',
                    title: 'title'
                }, 'ALL_NEW', ({
                    request,
                    expression
                }) => {
                    callback(expression);

                    update = sinon.spy(request, 'update');

                    return ['hooked expression'];
                });
            });

            afterEach(() => {
                update.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith('REMOVE #title SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
            });

            it('should query be called with hookArgs', () => {
                expect(update).to.have.been.calledWith('hooked expression');
            });
        });
    });

    describe('multiGet', () => {
        it('should get multiple items', done => {
            crud.multiGet({
                    items: [{
                        namespace,
                        id: 'id-8',
                        useless: 'useless'
                    }, {
                        namespace,
                        id: 'id-9',
                        useless: 'useless'
                    }, {
                        namespace,
                        id: 'id-90',
                        useless: 'useless'
                    }]
                })
                .pipe(
                    rx.toArray()
                )
                .subscribe(response => {
                    const firstIndex = _.findIndex(response, {
                        id: 'id-8'
                    });

                    expect(response).to.deep.contain({
                        createdAt: response[firstIndex].createdAt,
                        globalIndexedPartitionAttr: 'global-indexed-spec',
                        globalNumberIndexedSortAttr: 8,
                        localNumberIndexedSortAttr: 8,
                        namespace: 'spec',
                        id: 'id-8',
                        globalStringIndexedSortAttr: 'global-indexed-8',
                        message: 'message-8',
                        localStringIndexedSortAttr: 'local-indexed-8',
                        updatedAt: response[firstIndex].updatedAt
                    });

                    expect(response).to.deep.contain({
                        createdAt: response[firstIndex === 0 ? 1 : 0].createdAt,
                        globalIndexedPartitionAttr: 'global-indexed-spec',
                        globalNumberIndexedSortAttr: 9,
                        localNumberIndexedSortAttr: 9,
                        namespace: 'spec',
                        id: 'id-9',
                        globalStringIndexedSortAttr: 'global-indexed-9',
                        message: 'message-9',
                        localStringIndexedSortAttr: 'local-indexed-9',
                        updatedAt: response[firstIndex === 0 ? 1 : 0].updatedAt
                    });
                }, null, done);
        });

        describe('select', () => {
            it('should get multiple items', done => {
                crud.multiGet({
                        select: 'id',
                        items: [{
                            namespace,
                            id: 'id-8',
                            useless: 'useless'
                        }, {
                            namespace,
                            id: 'id-9',
                            useless: 'useless'
                        }, {
                            namespace,
                            id: 'id-90',
                            useless: 'useless'
                        }]
                    })
                    .pipe(
                        rx.toArray()
                    )
                    .subscribe(response => {
                        expect(response).to.deep.contain({
                            namespace: 'spec',
                            id: 'id-8'
                        });

                        expect(response).to.deep.contain({
                            namespace: 'spec',
                            id: 'id-9'
                        });
                    }, null, done);
            });
        });

        describe('hook', () => {
            let callback;
            let batchGet;
            let items = [{
                namespace,
                id: 'id-8',
                useless: 'useless'
            }, {
                namespace,
                id: 'id-9',
                useless: 'useless'
            }, {
                namespace,
                id: 'id-90',
                useless: 'useless'
            }];

            beforeEach(() => {
                callback = sinon.stub();

                crud.multiGet({
                    items
                }, ({
                    request,
                    items
                }) => {
                    callback({
                        items
                    });

                    batchGet = sinon.spy(request, 'batchGet');

                    return ['hooked'];
                });
            });

            afterEach(() => {
                batchGet.restore();
            });

            it('should callback be called with hookArgs', () => {
                expect(callback).to.have.been.calledWith({
                    items: _.map(items, item => _.pick(item, ['namespace', 'id']))
                });
            });

            it('should query be called with hookArgs', () => {
                expect(batchGet).to.have.been.calledWith('hooked');
            });
        });
    });

    describe('clear', done => {
        it('should clear table', () => {
            crud.clear({
                    namespace: 'spec'
                })
                .pipe(
                    rx.mergeMap(() => crud.fetch({
                        namespace
                    }))
                )
                .subscribe(response => {
                    expect(_.size(response.items)).to.equal(0);
                }, null, done);
        });
    });

    describe('toBase64', () => {
        it('should encode base64', () => {
            expect(crud.toBase64('aaaaa')).to.equal('YWFhYWE=');
        });

        it('should return null if empty', () => {
            expect(crud.toBase64()).to.be.null;
        });
    });

    describe('fromBase64', () => {
        it('should decode base64', () => {
            expect(crud.fromBase64('YWFhYWE=')).to.equal('aaaaa');
        });

        it('should return null if empty', () => {
            expect(crud.fromBase64()).to.be.null;
        });
    });
});