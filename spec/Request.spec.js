import _ from 'lodash';
import {
	Observable,
	Scheduler
} from 'rxjs';
import {
	Util,
	Request,
	ReturnValues,
	Select,
	ConsumedCapacity
} from 'src';

import {
	instance
} from 'testingEnv';

const namespace = 'spec';
const tableName = 'tblSpec';
const tableSchema = {
	primaryKeys: {
		partition: 'namespace',
		sort: 'key'
	},
	indexes: {
		localIndexedSpec: {
			partition: 'namespace',
			sort: 'localIndexedAttr'
		},
		globalIndexedSpec: {
			partition: 'globalIndexedPartitionAttr',
			sort: 'globalIndexedSortAttr'
		}
	}
};

describe('src/Request', () => {
	let now;
	let request;
	let client;
	let routeCall;

	before(done => {
		client = instance.client;
		request = instance.table(tableName, tableSchema);

		request.describe()
			.catch(() => request.routeCall('createTable', {
				TableName: 'tblSpec',
				ProvisionedThroughput: {
					ReadCapacityUnits: 1,
					WriteCapacityUnits: 1
				},
				AttributeDefinitions: [{
					AttributeName: 'namespace',
					AttributeType: 'S'
				}, {
					AttributeName: 'key',
					AttributeType: 'S'
				}, {
					AttributeName: 'localIndexedAttr',
					AttributeType: 'S'
				}, {
					AttributeName: 'globalIndexedPartitionAttr',
					AttributeType: 'S'
				}, {
					AttributeName: 'globalIndexedSortAttr',
					AttributeType: 'S'
				}],
				KeySchema: [{
					AttributeName: 'namespace',
					KeyType: 'HASH'
				}, {
					AttributeName: 'key',
					KeyType: 'RANGE'
				}],
				LocalSecondaryIndexes: [{
					IndexName: 'localIndexedSpec',
					KeySchema: [{
						AttributeName: 'namespace',
						KeyType: 'HASH'
					}, {
						AttributeName: 'localIndexedAttr',
						KeyType: 'RANGE'
					}],
					Projection: {
						ProjectionType: 'ALL'
					}
				}],
				GlobalSecondaryIndexes: [{
					IndexName: 'globalIndexedSpec',
					KeySchema: [{
						AttributeName: 'globalIndexedPartitionAttr',
						KeyType: 'HASH'
					}, {
						AttributeName: 'globalIndexedSortAttr',
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
			}))
			.mergeMap(() => Observable.range(0, 10)
				.mergeMap(n => request.insert({
					namespace,
					key: `key-${n}`,
					message: `message-${n}`,
					localIndexedAttr: `local-indexed-${n}`,
					globalIndexedPartitionAttr: `global-indexed-${namespace}`,
					globalIndexedSortAttr: `global-indexed-${n}`,
				}, true)))
			.subscribe(null, null, done);
	});

	after(done => {
		request = instance.table(tableName, tableSchema);
		request.query({
				namespace: 'spec'
			})
			.toArray()
			.mergeMap(::request.batchWrite)
			.subscribe(null, null, done);
	});

	beforeEach(() => {
		now = _.now();
		request = instance.table(tableName, tableSchema);
		routeCall = spy(request, 'routeCall');
	});

	afterEach(() => {
		routeCall.restore();
	});

	describe('util', () => {
		it('should return new util instance', () => {
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

		it('should returns globalIndex.partition', () => {
			request
				.index('globalIndexedSpec');

			expect(request.partitionAttr).to.equal('globalIndexedPartitionAttr');
		});

		it('should returns null when wrong globalIndex', () => {
			request
				.index('globalIndexedSpec_');

			expect(request.partitionAttr).to.be.null;
		});
	});

	describe('sortAttr', () => {
		it('should returns primaryKeys.sort', () => {
			expect(request.sortAttr).to.equal('key');
		});

		it('should returns globalIndex.sort', () => {
			request
				.index('globalIndexedSpec');

			expect(request.sortAttr).to.equal('globalIndexedSortAttr');
		});

		it('should returns null when wrong globalIndex', () => {
			request
				.index('globalIndexedSpec_');

			expect(request.sortAttr).to.be.null;
		});
	});

	describe('localIndexAttr', () => {
		it('should returns localIndex.sort', () => {
			request
				.index('localIndexedSpec');

			expect(request.localIndexAttr).to.equal('localIndexedAttr');
		});

		it('should returns null when wrong globalIndex', () => {
			request
				.index('localIndexedSpec_');

			expect(request.localIndexAttr).to.be.null;
		});

		it('should returns null when partition does not belongs to primary keys', () => {
			request
				.index('globalIndexedSpec');

			expect(request.localIndexAttr).to.be.null;
		});
	});

	describe('routeCall', () => {
		let getItem;

		beforeEach(() => {
			getItem = spy(client, 'getItem');
		});

		afterEach(() => {
			getItem.restore();
		});

		it('should return an Observable', () => {
			expect(request.routeCall('getItem', {})).to.be.instanceOf(Observable);
		});

		it('should call method on client', done => {
			request.routeCall('getItem', {
				TableName: tableName,
				Key: {
					namespace: {
						S: 'spec'
					},
					key: {
						S: 'key'
					}
				}
			}).subscribe(() => {
				expect(getItem).to.have.been.calledWith({
					TableName: tableName,
					Key: {
						namespace: {
							S: 'spec'
						},
						key: {
							S: 'key'
						}
					}
				}, match.func);
			}, null, done);
		});

		it('should not call method on client when no subscribers', () => {
			request.routeCall('getItem', {
				TableName: tableName,
				Key: {
					namespace: {
						S: 'spec'
					},
					key: {
						S: 'key'
					}
				}
			});

			expect(getItem).not.to.have.been.called;
		});
	});

	describe('describe', () => {
		it('should routeCall with describeTable', () => {
			request.describe().subscribe();

			expect(routeCall).to.have.been.calledWithExactly('describeTable', {
				TableName: tableName
			});
		});

		it('should return an Observable', () => {
			expect(request.describe()).to.be.instanceOf(Observable);
		});
	});

	describe('table', () => {
		it('should feed table, tableSchema and primaryKeys', () => {
			expect(request.tableName).to.equal(tableName);
			expect(request.primaryKeys).to.deep.equal({
				partition: 'namespace',
				sort: 'key'
			});
		});

		it('should return Request instance', () => {
			expect(instance.table(tableName, tableSchema)).to.be.instanceOf(Request);
		});
	});

	describe('return', () => {
		beforeEach(() => {
			request.return(ReturnValues.ALL_NEW);
		});

		it('should feed returnValues', () => {
			expect(request.returnValues).to.equal('ALL_NEW');
		});

		it('should return Request instance', () => {
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

		it('should return Request instance', () => {
			expect(request.consumedCapacity(ConsumedCapacity.TOTAL)).to.be.instanceOf(Request);
		});
	});

	describe('addPlaceholderName', () => {
		it('should add placeholders with object', () => {
			request.addPlaceholderName({
				key: 'value'
			});

			expect(request.expressionAttributeNames).to.deep.equal({
				'#key': 'value'
			});
		});

		it('should add placeholders with array', () => {
			request.addPlaceholderName(['key', 'key2']);

			expect(request.expressionAttributeNames).to.deep.equal({
				'#key': 'key',
				'#key2': 'key2'
			});
		});

		it('should add placeholders with string', () => {
			request.addPlaceholderName('key');

			expect(request.expressionAttributeNames).to.deep.equal({
				'#key': 'key'
			});
		});

		it('should return Request instance', () => {
			expect(request.addPlaceholderName('key')).to.be.instanceOf(Request);
		});
	});

	describe('addPlaceholderValue', () => {
		it('should add placeholders with object and string', () => {
			request.addPlaceholderValue({
				key: 'value'
			});

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					S: 'value'
				}
			});
		});

		it('should add placeholders with object and number', () => {
			request.addPlaceholderValue({
				key: 9
			});

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					N: '9'
				}
			});
		});

		it('should add placeholders with object and boolean', () => {
			request.addPlaceholderValue({
				key: true
			});

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					BOOL: true
				}
			});
		});

		it('should add placeholders with object and null', () => {
			request.addPlaceholderValue({
				key: null
			});

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					NULL: true
				}
			});
		});

		it('should add placeholders with object and array', () => {
			request.addPlaceholderValue({
				key: [1, 2]
			});

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
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
				key: {
					key: 'value'
				}
			});

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					M: {
						key: {
							S: 'value'
						}
					}
				}
			});
		});

		it('should add placeholders with string array', () => {
			request.addPlaceholderValue(['key', 'key2']);

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					S: 'key'
				},
				':key2': {
					S: 'key2'
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
			request.addPlaceholderValue('key');

			expect(request.expressionAttributeValues).to.deep.equal({
				':key': {
					S: 'key'
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

		it('should return Request instance', () => {
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
			request.select('key, name, age');

			expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
			expect(request.projectionExpression).to.equal('#key_0,#name_1,#age_2,#namespace_3');
			expect(request.expressionAttributeNames).to.deep.equal({
				'#key_0': 'key',
				'#name_1': 'name',
				'#age_2': 'age',
				'#namespace_3': 'namespace',
			});
		});

		it('should include localIndexAttr', () => {
			request
				.index('localIndexedSpec')
				.select('key, name, age');

			expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
			expect(request.projectionExpression).to.equal('#key_0,#name_1,#age_2,#namespace_3,#localIndexedAttr_4');
			expect(request.expressionAttributeNames).to.deep.equal({
				'#key_0': 'key',
				'#name_1': 'name',
				'#age_2': 'age',
				'#namespace_3': 'namespace',
				'#localIndexedAttr_4': 'localIndexedAttr'
			});
		});

		it('should include globalIndex attrs', () => {
			request
				.index('globalIndexedSpec')
				.select('key, name, age');

			expect(request.projectionSelect).to.equal('SPECIFIC_ATTRIBUTES');
			expect(request.projectionExpression).to.equal('#key_0,#name_1,#age_2,#globalIndexedPartitionAttr_3,#globalIndexedSortAttr_4');
			expect(request.expressionAttributeNames).to.deep.equal({
				'#key_0': 'key',
				'#name_1': 'name',
				'#age_2': 'age',
				'#globalIndexedPartitionAttr_3': 'globalIndexedPartitionAttr',
				'#globalIndexedSortAttr_4': 'globalIndexedSortAttr'
			});
		});

		it('should return Request instance', () => {
			expect(request.select('key')).to.be.instanceOf(Request);
		});
	});

	describe('consistent', () => {
		it('should set consistentRead as true', () => {
			request.consistent();

			expect(request.consistentRead).to.be.true;
		});

		it('should return Request instance', () => {
			expect(request.consistent()).to.be.instanceOf(Request);
		});
	});

	describe('query', () => {
		it('should keyConditionExpression be same of queryData when latter is string', () => {
			request.query('#key = :value');

			expect(request.keyConditionExpression).to.equal('#key = :value');
		});

		it('should build keyConditionExpression when queryData is object, using just schema keys', () => {
			request.query({
				namespace,
				ignoredKey: 'this key should be ignored'
			});

			expect(request.keyConditionExpression).to.equal('#namespace = :namespace');
		});

		it('should routeCall with query and relative params', () => {
			request.query({
				namespace,
				ignoredKey: 'this key should be ignored'
			});

			expect(routeCall).to.have.been.calledOnce;
			expect(routeCall).to.have.been.calledWithExactly('query', {
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
				Limit: 50,
				ProjectionExpression: undefined,
				ReturnConsumedCapacity: 'TOTAL',
				ScanIndexForward: true,
				Select: 'ALL_ATTRIBUTES',
				TableName: 'tblSpec'
			});
		});

		it('should responds with normalized data', done => {
			request
				.limit(2)
				.query({
					namespace,
					ignoredKey: 'this key should be ignored'
				})
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([{
						namespace: 'spec',
						localIndexedAttr: response[0].localIndexedAttr,
						globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
						globalIndexedSortAttr: response[0].globalIndexedSortAttr,
						key: 'key-0',
						message: response[0].message,
						createdAt: response[0].createdAt,
						updatedAt: response[0].updatedAt
					}, {
						namespace: 'spec',
						localIndexedAttr: response[1].localIndexedAttr,
						globalIndexedPartitionAttr: response[1].globalIndexedPartitionAttr,
						globalIndexedSortAttr: response[1].globalIndexedSortAttr,
						key: 'key-1',
						message: response[1].message,
						createdAt: response[1].createdAt,
						updatedAt: response[1].updatedAt
					}]);
				}, null, done);
		});

		it('should responds with normalized data using localIndex', done => {
			request
				.limit(2)
				.index('localIndexedSpec')
				.query({
					namespace,
					localIndexedAttr: 'local-indexed-3',
					ignoredKey: 'this key should be ignored'
				})
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([{
						namespace: 'spec',
						localIndexedAttr: response[0].localIndexedAttr,
						globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
						globalIndexedSortAttr: response[0].globalIndexedSortAttr,
						key: 'key-3',
						message: response[0].message,
						createdAt: response[0].createdAt,
						updatedAt: response[0].updatedAt
					}]);
				}, null, done);
		});

		it('should responds with normalized data using globalIndex', done => {
			request
				.limit(2)
				.index('globalIndexedSpec')
				.query({
					globalIndexedPartitionAttr: `global-indexed-${namespace}`,
					globalIndexedSortAttr: `global-indexed-3`,
					ignoredKey: 'this key should be ignored'
				})
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([{
						namespace: 'spec',
						localIndexedAttr: response[0].localIndexedAttr,
						globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
						globalIndexedSortAttr: response[0].globalIndexedSortAttr,
						key: 'key-3',
						message: response[0].message,
						createdAt: response[0].createdAt,
						updatedAt: response[0].updatedAt
					}]);
				}, null, done);
		});

		it('should responds with resumed data', done => {
			request
				.limit(2)
				.resume({
					namespace,
					key: 'key-2'
				})
				.query({
					namespace,
					ignoredignoredKey: 'this key should be ignored'
				})
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([{
						namespace: 'spec',
						localIndexedAttr: response[0].localIndexedAttr,
						globalIndexedPartitionAttr: response[0].globalIndexedPartitionAttr,
						globalIndexedSortAttr: response[0].globalIndexedSortAttr,
						key: 'key-3',
						message: response[0].message,
						createdAt: response[0].createdAt,
						updatedAt: response[0].updatedAt
					}, {
						namespace: 'spec',
						localIndexedAttr: response[1].localIndexedAttr,
						globalIndexedPartitionAttr: response[1].globalIndexedPartitionAttr,
						globalIndexedSortAttr: response[1].globalIndexedSortAttr,
						key: 'key-4',
						message: response[1].message,
						createdAt: response[1].createdAt,
						updatedAt: response[1].updatedAt
					}]);
				}, null, done);
		});

		it('should return an Observable', () => {
			expect(request.query({})).to.be.instanceOf(Observable);
		});

		describe('with filterExpression', () => {
			it('should routeCall with limit = null', () => {
				request
					.addPlaceholderName('message')
					.addPlaceholderValue('message')
					.filter('begins_with(#message, :message)')
					.query({
						namespace,
						ignoredKey: 'this key should be ignored'
					});

				expect(routeCall).to.have.been.calledOnce;
				expect(routeCall).to.have.been.calledWithExactly('query', {
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
					Limit: null,
					ProjectionExpression: undefined,
					ReturnConsumedCapacity: 'TOTAL',
					ScanIndexForward: true,
					Select: 'ALL_ATTRIBUTES',
					TableName: 'tblSpec'
				});
			});

			it('should emulate lastKey when limit', done => {
				request
					.addPlaceholderName('message')
					.addPlaceholderValue('message')
					.filter('begins_with(#message, :message)')
					.limit(5)
					.query({
						namespace,
						ignoredKey: 'this key should be ignored'
					})
					.toArray()
					.subscribe(() => {
						expect(request.queryStats.count).to.equal(5);
						expect(request.queryStats.lastKey).to.deep.equal({
							namespace: 'spec',
							key: 'key-4'
						});
					}, null, done);
			});

			it('should emulate lastKey when limit and localIndex', done => {
				request
					.addPlaceholderName('message')
					.addPlaceholderValue('message')
					.filter('begins_with(#message, :message)')
					.index('localIndexedSpec')
					.limit(5)
					.query({
						namespace,
						ignoredKey: 'this key should be ignored'
					})
					.toArray()
					.subscribe(() => {
						expect(request.queryStats.count).to.equal(5);
						expect(request.queryStats.lastKey).to.deep.equal({
							localIndexedAttr: 'local-indexed-4',
							namespace: 'spec',
							key: 'key-4'
						});
					}, null, done);
			});

			it('should emulate lastKey when limit and globalIndex', done => {
				request
					.addPlaceholderName('message')
					.addPlaceholderValue('message')
					.filter('begins_with(#message, :message)')
					.index('globalIndexedSpec')
					.limit(5)
					.query({
						globalIndexedPartitionAttr: `global-indexed-${namespace}`,
						ignoredKey: 'this key should be ignored'
					})
					.toArray()
					.subscribe(() => {
						expect(request.queryStats.count).to.equal(5);
						expect(request.queryStats.lastKey).to.deep.equal({
							globalIndexedPartitionAttr: 'global-indexed-spec',
							globalIndexedSortAttr: 'global-indexed-4'
						});
					}, null, done);
			});
		});

		describe('multiple operations', () => {
			beforeEach(() => {
				routeCall.restore();
				routeCall = stub(request, 'routeCall')
					.callsFake(() => Observable.of({
						Items: [{
							key: {
								'S': 'key-0'
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
							key: {
								'S': 'key-1'
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
							key: {
								'S': 'key-1'
							},
							namespace: {
								'S': 'spec'
							}
						}
					}).observeOn(Scheduler.asap));
			});

			it('should run queryOperation 25 times until reach default limit or lastKey be null', done => {
				request
					.query({
						namespace,
						ignoredKey: 'this key should be ignored'
					})
					.toArray()
					.subscribe(response => {
						expect(request.queryStats.scannedCount).to.equal(50);
						expect(request.queryStats.count).to.equal(50);
						expect(request.queryStats.iteractions).to.equal(25);
					}, null, done);
			});

			it('should run queryOperation 5 times until reach limit', done => {
				request
					.limit(10)
					.query({
						namespace,
						ignoredKey: 'this key should be ignored'
					})
					.toArray()
					.subscribe(response => {
						expect(request.queryStats.scannedCount).to.equal(10);
						expect(request.queryStats.count).to.equal(10);
						expect(request.queryStats.iteractions).to.equal(5);
					}, null, done);
			});

			it('should return strict the limit even if more items were processed', done => {
				request
					.limit(9)
					.query({
						namespace,
						ignoredKey: 'this key should be ignored'
					})
					.toArray()
					.subscribe(response => {
						expect(request.queryStats.scannedCount).to.equal(10);
						expect(request.queryStats.count).to.equal(10);
						expect(request.queryStats.iteractions).to.equal(5);
						expect(_.size(response)).to.equal(9);
					}, null, done);
			});
		});
	});

	describe('get', () => {
		it('should routeCall with get and relative params', () => {
			request
				.get({
					namespace,
					key: 'key-0',
					ignoredKey: 'this key should be ignored'
				});

			expect(routeCall).to.have.been.calledOnce;
			expect(routeCall).to.have.been.calledWithExactly('getItem', {
				ExpressionAttributeNames: null,
				Key: {
					key: {
						S: 'key-0'
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
			request
				.get({
					namespace,
					key: 'key-0',
					ignoredKey: 'this key should be ignored'
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						namespace: 'spec',
						localIndexedAttr: 'local-indexed-0',
						globalIndexedPartitionAttr: 'global-indexed-spec',
						globalIndexedSortAttr: 'global-indexed-0',
						key: 'key-0',
						message: response.message,
						createdAt: response.createdAt,
						updatedAt: response.updatedAt
					});
				}, null, done);
		});

		it('should responds empty', done => {
			request
				.get({
					namespace,
					key: 'key-inexistent',
					ignoredKey: 'this key should be ignored'
				})
				.subscribe(response => {
					expect(response).to.deep.equal({});
				}, null, done);
		});

		it('should return an Observable', () => {
			expect(request.get({})).to.be.instanceOf(Observable);
		});
	});

	describe('index', () => {
		it('should set indexName', () => {
			request.index('someIndex');

			expect(request.indexName).to.equal('someIndex');
		});

		it('should return Request instance', () => {
			expect(request.index('someIndex')).to.be.instanceOf(Request);
		});
	});

	describe('desc', () => {
		it('should set scanIndexForward to false', () => {
			request.desc();

			expect(request.scanIndexForward).to.be.false;
		});

		it('should return Request instance', () => {
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

		it('should handle not finite values and defaults to 0', () => {
			request.limit(Infinity);

			expect(request.queryLimit).to.equal(0);
		});

		it('should return Request instance', () => {
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

		it('should return Request instance', () => {
			expect(request.filter('someFilter')).to.be.instanceOf(Request);
		});
	});

	describe('resume', () => {
		it('should set exclusiveStartKey', () => {
			request.resume({
				namespace,
				key: 'key-0'
			});

			expect(request.exclusiveStartKey).to.deep.equal({
				namespace: {
					S: namespace
				},
				key: {
					S: 'key-0'
				}
			});
		});

		it('should return Request instance', () => {
			expect(request.resume({
				namespace,
				key: 'key-0'
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

		it('should return Request instance', () => {
			expect(request.condition('someCondition')).to.be.instanceOf(Request);
		});
	});

	describe('insert', () => {
		it('should routeCall with putItem and relative params', () => {
			request
				.insert({
					namespace,
					key: `key-${now}`,
					message: `message-${now}`
				});

			expect(routeCall).to.have.been.calledOnce;
			expect(routeCall).to.have.been.calledWithExactly('putItem', {
				ConditionExpression: 'attribute_not_exists(#namespace)',
				ExpressionAttributeNames: {
					'#namespace': 'namespace'
				},
				ExpressionAttributeValues: null,
				Item: {
					namespace: {
						S: 'spec'
					},
					key: {
						S: `key-${now}`
					},
					message: {
						S: `message-${now}`
					},
					createdAt: {
						N: match.string
					},
					updatedAt: {
						N: match.string
					}
				},
				ReturnConsumedCapacity: 'TOTAL',
				TableName: 'tblSpec'
			});
		});

		it('should responds with normalized data and createdAt and updatedAt be equals', done => {
			request
				.insert({
					namespace,
					key: `key-${now}`,
					message: `message-${now}`
				})
				.subscribe(response => {
					expect(response.createdAt === response.updatedAt).to.be.true;
					expect(response).to.deep.equal({
						namespace: 'spec',
						key: `key-${now}`,
						message: response.message,
						createdAt: response.createdAt,
						updatedAt: response.updatedAt
					});
				}, null, done);
		});

		it('should throw if record exists and replace = false', done => {
			request
				.insert({
					namespace,
					key: 'key-0',
					message: 'message-0'
				})
				.subscribe(null, err => {
					expect(request.conditionExpression).to.equal('attribute_not_exists(#namespace)');
					expect(err.message).to.equal('The conditional request failed');
					done();
				});
		});

		it('should responds with normalized data if data exists and replace = true', done => {
			request
				.insert({
					namespace,
					key: 'key-0',
					message: 'message-0'
				}, true)
				.subscribe(response => {
					expect(request.conditionExpression).to.be.null;
					expect(response.createdAt === response.updatedAt).to.be.true;
					expect(response).to.deep.equal({
						namespace: 'spec',
						key: 'key-0',
						message: response.message,
						createdAt: response.createdAt,
						updatedAt: response.updatedAt
					});
				}, null, done);
		});

		it('should return an Observable', () => {
			expect(request.insert({})).to.be.instanceOf(Observable);
		});
	});

	describe('insertOrReplace', () => {
		let insert;

		beforeEach(() => {
			insert = spy(request, 'insert');
		});

		afterEach(() => {
			insert.restore();
		});

		it('should call insert with replace = true', () => {
			request.insertOrReplace({});

			expect(insert).to.have.been.calledWith(match.object, true);
		});
	});

	describe('insertOrUpdate', () => {
		let update;

		beforeEach(() => {
			update = spy(request, 'update');
		});

		afterEach(() => {
			update.restore();
		});

		it('should call update with where = false, insert = true', () => {
			request.insertOrUpdate({});

			expect(update).to.have.been.calledWith(match.object, undefined, true);
		});
	});

	describe('update', () => {
		it('should updateExpression be same of item when latter is string', () => {
			request.update('SET #key = :value', {});

			expect(request.updateExpression).to.equal('SET #key = :value');
		});

		it('should build updateExpression when item is object', () => {
			request.update({
				key: 'value',
				key2: 'value2'
			});

			expect(request.updateExpression).to.equal('SET #key2 = :key2, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
		});

		it('should throw when item is string and no where statement is provided', done => {
			request.update('SET #key = :value')
				.subscribe(null, err => {
					expect(err.message).to.equal('Where statement might be provided');
					done();
				});
		});

		it('should Keys be composed with just primary keys', () => {
			request
				.return(ReturnValues.ALL_NEW)
				.update({
					namespace,
					key: 'key-0',
					ignoredKey: 'this key should be ignored'
				});

			expect(routeCall.lastCall.args[1].Key).to.deep.equal({
				key: {
					S: 'key-0'
				},
				namespace: {
					S: 'spec'
				}
			});
		});

		it('should routeCall with updateItem and relative params', () => {
			request
				.return(ReturnValues.ALL_NEW)
				.update({
					namespace,
					key: 'key-0',
					message: 'message-0'
				});

			expect(routeCall).to.have.been.calledOnce;
			expect(routeCall).to.have.been.calledWithExactly('updateItem', {
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
						N: match.string
					}
				},
				Key: {
					key: {
						S: 'key-0'
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
			request
				.return(ReturnValues.ALL_NEW)
				.update({
					namespace,
					key: 'key-0',
					message: 'message-0'
				})
				.subscribe(response => {
					expect(response.createdAt !== response.updatedAt).to.be.true;
					expect(response).to.deep.equal({
						key: 'key-0',
						createdAt: response.createdAt,
						namespace: 'spec',
						updatedAt: response.updatedAt,
						message: response.message
					});
				}, null, done);
		});

		it('should throw if record doesn\'t exists and insert = false', done => {
			request
				.return(ReturnValues.ALL_NEW)
				.update({
					namespace,
					key: `key-${now}`,
					message: `message-${now}`
				})
				.subscribe(null, err => {
					expect(request.conditionExpression).to.equal('attribute_exists(#namespace)');
					expect(err.message).to.equal('The conditional request failed');
					done();
				});
		});

		it('should responds with normalized data if data exists and insert = true and createdAt and updatedAt be equals', done => {
			request
				.return(ReturnValues.ALL_NEW)
				.update({
					namespace,
					key: `key-${now}`,
					message: `message-${now}`
				}, null, true)
				.subscribe(response => {
					expect(request.conditionExpression).to.be.null;
					expect(response.createdAt === response.updatedAt).to.be.true;
					expect(response).to.deep.equal({
						namespace: 'spec',
						key: response.key,
						message: response.message,
						createdAt: response.createdAt,
						updatedAt: response.updatedAt
					});
				}, null, done);
		});

		it('should return an Observable', () => {
			expect(request.update({})).to.be.instanceOf(Observable);
		});
	});

	describe('delete', () => {
		it('should Keys be composed with just primary keys', () => {
			request
				.return(ReturnValues.ALL_OLD)
				.delete({
					namespace,
					key: 'key-0',
					ignoredKey: 'this key should be ignored'
				});

			expect(routeCall.lastCall.args[1].Key).to.deep.equal({
				key: {
					S: 'key-0'
				},
				namespace: {
					S: 'spec'
				}
			});
		});

		it('should routeCall with deleteItem and relative params', () => {
			request
				.return(ReturnValues.ALL_OLD)
				.delete({
					namespace,
					key: 'key-0',
					message: 'message-0'
				});

			expect(routeCall).to.have.been.calledOnce;
			expect(routeCall).to.have.been.calledWithExactly('deleteItem', {
				ConditionExpression: null,
				ExpressionAttributeNames: null,
				ExpressionAttributeValues: null,
				Key: {
					key: {
						S: 'key-0'
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
			request
				.return(ReturnValues.ALL_OLD)
				.delete({
					namespace,
					key: 'key-0',
					message: 'message-0'
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						namespace: 'spec',
						key: 'key-0',
						message: response.message,
						createdAt: response.createdAt,
						updatedAt: response.updatedAt
					});
				}, null, done);
		});

		it('should responds empty when record doesn\'t exists', done => {
			request
				.return(ReturnValues.ALL_OLD)
				.delete({
					namespace,
					key: `non-existent-key`
				})
				.subscribe(response => {
					expect(response).to.deep.equal({});
				}, null, done);
		});
	});

	describe('batchWrite', () => {
		it('should do jobs in steps with 25 operations max', () => {
			request.batchWrite(_.times(76, n => ({
				namespace,
				key: `batchWrite-${n}`,
				message: `message-${n}`
			})));

			expect(routeCall).to.have.been.callCount(4);
		});

		describe('toDelete', () => {
			beforeEach(done => {
				request.batchWrite(null, [{
					namespace,
					key: 'batchWrite-0',
					message: 'message-0'
				}, {
					namespace,
					key: 'batchWrite-1',
					message: 'message-1'
				}]).subscribe(null, null, () => {
					routeCall.reset();
					done();
				});
			});

			it('should routeCall with batchWriteItem and relative params', () => {
				request.batchWrite([{
					namespace,
					key: 'batchWrite-0',
					message: 'message-0'
				}, {
					namespace,
					key: 'batchWrite-1',
					message: 'message-1'
				}]);

				expect(routeCall).to.have.been.calledWithExactly('batchWriteItem', {
					RequestItems: {
						tblSpec: [{
							DeleteRequest: {
								Key: {
									namespace: {
										S: 'spec'
									},
									key: {
										S: 'batchWrite-0'
									}
								}
							}
						}, {
							DeleteRequest: {
								Key: {
									key: {
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
				request
					.batchWrite([{
						namespace,
						key: 'batchWrite-0',
						message: 'message-0'
					}, {
						namespace,
						key: 'batchWrite-1',
						message: 'message-1'
					}]).subscribe(response => {
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
					key: 'batchWrite-0',
					message: 'message-0'
				}, {
					namespace,
					key: 'batchWrite-1',
					message: 'message-1'
				}]);

				expect(routeCall).to.have.been.calledWithExactly('batchWriteItem', {
					RequestItems: {
						tblSpec: [{
							PutRequest: {
								Item: {
									namespace: {
										S: 'spec'
									},
									key: {
										S: 'batchWrite-0'
									},
									message: {
										S: 'message-0'
									},
									createdAt: {
										N: match.string
									},
									updatedAt: {
										N: match.string
									}
								}
							}
						}, {
							PutRequest: {
								Item: {
									namespace: {
										S: 'spec'
									},
									key: {
										S: 'batchWrite-1'
									},
									message: {
										S: 'message-1'
									},
									createdAt: {
										N: match.string
									},
									updatedAt: {
										N: match.string
									}
								}
							}
						}]
					}
				});
			});

			it('should responds with normalized data', done => {
				request
					.batchWrite(null, [{
						namespace,
						key: 'batchWrite-0',
						message: 'message-0'
					}, {
						namespace,
						key: 'batchWrite-1',
						message: 'message-1'
					}]).subscribe(response => {
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
					key: 'batchWrite-0',
					message: 'message-0'
				}, {
					namespace,
					key: 'batchWrite-1',
					message: 'message-1'
				}], [{
					namespace,
					key: 'batchWrite-0',
					message: 'message-0'
				}, {
					namespace,
					key: 'batchWrite-1',
					message: 'message-1'
				}]);

				expect(routeCall).to.have.been.calledWithExactly('batchWriteItem', {
					RequestItems: {
						tblSpec: [{
							DeleteRequest: {
								Key: {
									namespace: {
										S: 'spec'
									},
									key: {
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
									key: {
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
									key: {
										S: 'batchWrite-0'
									},
									message: {
										S: 'message-0'
									},
									createdAt: {
										N: match.string
									},
									updatedAt: {
										N: match.string
									}
								}
							}
						}, {
							PutRequest: {
								Item: {
									namespace: {
										S: 'spec'
									},
									key: {
										S: 'batchWrite-1'
									},
									message: {
										S: 'message-1'
									},
									createdAt: {
										N: match.string
									},
									updatedAt: {
										N: match.string
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
			Observable.range(0, 10)
				.mergeMap(n => request.insert({
					namespace,
					key: `key-${n}`,
					message: `message-${n}`,
					localIndexedAttr: `local-indexed-${n}`
				}, true))
				.subscribe(null, null, done);

			routeCall.reset();
		});

		it('should do jobs in steps with 100 operations max', () => {
			request.batchGet(_.times(176, n => ({
				namespace,
				key: `key-${n}`
			})));

			expect(routeCall).to.have.been.calledTwice;
		});

		it('should routeCall with batchGet and relative params', () => {
			request.batchGet([{
				namespace,
				key: 'key-0'
			}, {
				namespace,
				key: 'key-1'
			}]);

			expect(routeCall).to.have.been.calledWithExactly('batchGetItem', {
				RequestItems: {
					tblSpec: {
						Keys: [{
							namespace: {
								S: 'spec'
							},
							key: {
								S: 'key-0'
							}
						}, {
							key: {
								S: 'key-1'
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
					key: 'key-0'
				}, {
					namespace,
					key: 'key-1'
				}])
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([{
						namespace: 'spec',
						localIndexedAttr: response[0].localIndexedAttr,
						createdAt: response[0].createdAt,
						message: response[0].message,
						key: response[0].key,
						updatedAt: response[0].updatedAt
					}, {
						namespace: 'spec',
						localIndexedAttr: response[1].localIndexedAttr,
						createdAt: response[1].createdAt,
						message: response[1].message,
						key: response[1].key,
						updatedAt: response[1].updatedAt
					}]);
				}, null, done);
		});
	});
});
