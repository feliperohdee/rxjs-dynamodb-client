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
	ConsumedCapacity,
	Crud
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

describe('src/Crud', () => {
	let now;
	let request;
	let client;
	let crud;

	before(done => {
		client = instance.client;
		request = instance.table(tableName, tableSchema);
		crud = new Crud(tableName, tableSchema, {
			dynamoDb: instance
		});

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
	});

	describe('constructor', () => {
		it('should throw if not deps.dynamoDb', () => {
			expect(() => new Crud()).to.throw('no dynamoDb client provided.');
		});
	});

	describe('partitionAttr', () => {
		it('should returns primaryKeys.partition', () => {
			expect(crud.partitionAttr()).to.equal('namespace');
		});

		it('should returns globalIndex.partition', () => {
			expect(crud.partitionAttr('globalIndexedSpec')).to.equal('globalIndexedPartitionAttr');
		});

		it('should returns null when wrong globalIndex', () => {
			expect(crud.partitionAttr('globalIndexedSpec_')).to.be.null;
		});
	});

	describe('sortAttr', () => {
		it('should returns primaryKeys.sort', () => {
			expect(crud.sortAttr()).to.equal('key');
		});

		it('should returns globalIndex.sort', () => {
			expect(crud.sortAttr('globalIndexedSpec')).to.equal('globalIndexedSortAttr');
		});

		it('should returns null when wrong globalIndex', () => {
			expect(crud.sortAttr('globalIndexedSpec_')).to.be.null;
		});
	});

	describe('localIndexAttr', () => {
		it('should returns localIndex.sort', () => {
			expect(crud.localIndexAttr('localIndexedSpec')).to.equal('localIndexedAttr');
		});

		it('should returns null when wrong localIndex', () => {
			expect(crud.localIndexAttr()).to.be.null;
		});

		it('should returns null when is global index', () => {
			expect(crud.localIndexAttr('globalIndexedSpec')).to.be.null;
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
					stats = response.stats;
				}, null, done);
		});

		it('should fetch with just namespace', () => {
			expect(items[0].key).to.equal('key-0');
			expect(stats.count).to.equal(10);
		});

		describe('with itemSelector', () => {
			beforeEach(done => {
				const itemSelector = items => items
					.map(response => _.pick(response, [
						'key'
					]));

				crud.fetch({
						namespace: 'spec',
						key: 'key-0'
					}, null, itemSelector)
					.subscribe(response => {
						items = response.items;
						stats = response.stats;
					}, null, done);
			});

			it('should fetch and apply itemSelector', () => {
				expect(items[0]).to.deep.equal({
					key: 'key-0'
				});

				expect(stats.count).to.equal(1);
			});
		});

		describe('with customReducer', () => {
			beforeEach(done => {
				const customReducer = items => items
					.toArray();

				crud.fetch({
						namespace: 'spec',
						key: 'key-0'
					}, null, null, customReducer)
					.subscribe(response => {
						items = response;
					}, null, done);
			});

			it('should fetch and apply customReducer', () => {
				expect(items[0].key).to.equal('key-0');
			});
		});

		describe('with local index', () => {
			beforeEach(done => {
				crud.fetch({
						namespace: 'spec',
						key: 'key-3'
					})
					.subscribe(response => {
						items = response.items;
						stats = response.stats;
					}, null, done);
			});

			it('should fetch with namespace and key', () => {
				expect(items[0].key).to.equal('key-3');
				expect(stats.count).to.equal(1);
			});
		});

		describe('with local index', () => {
			beforeEach(done => {
				crud.fetch({
						indexName: 'localIndexedSpec',
						namespace: 'spec',
						localIndexedAttr: 'local-indexed-3'
					})
					.subscribe(response => {
						items = response.items;
						stats = response.stats;
					}, null, done);
			});

			it('should fetch with namespace and local index', () => {
				expect(items[0].localIndexedAttr).to.equal('local-indexed-3');
				expect(stats.count).to.equal(1);
			});
		});

		describe('with global index', () => {
			beforeEach(done => {
				crud.fetch({
						indexName: 'globalIndexedSpec',
						globalIndexedPartitionAttr: 'global-indexed-spec',
						globalIndexedSortAttr: 'global-indexed-3'
					})
					.subscribe(response => null, null, done);
			});

			it('should fetch with namespace and global index', done => {
				crud.fetch({
						indexName: 'globalIndexedSpec',
						globalIndexedPartitionAttr: 'global-indexed-spec',
						globalIndexedSortAttr: 'global-indexed-3'
					})
					.subscribe(response => {
						expect(response.items[0].globalIndexedSortAttr).to.equal('global-indexed-3');
						expect(response.stats.count).to.equal(1);
					}, null, done);
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
						stats = response.stats;
					}, null, done);
			});

			it('should get correct lastKey', () => {
				expect(_.last(items).key).to.equal('key-4');
				expect(JSON.parse(crud.fromBase64(stats.lastKey))).to.deep.equal({
					namespace: 'spec',
					key: 'key-4'
				});

				expect(stats.count).to.equal(5);
			});

			it('should resume based on lastKey', done => {
				crud.fetch({
						limit: 5,
						namespace: 'spec',
						resume: stats.lastKey
					})
					.subscribe(response => {
						expect(response.items[0].key).to.equal('key-5');
						expect(stats.count).to.equal(5);
					}, null, done);
			});

			describe('with local index', () => {
				beforeEach(done => {
					crud.fetch({
							limit: 5,
							indexName: 'localIndexedSpec',
							namespace: 'spec',
							select: 'localIndexedAttr'
						})
						.subscribe(response => {
							items = response.items;
							stats = response.stats;
						}, null, done);
				});

				it('should get correct lastKey', () => {
					expect(_.last(items).localIndexedAttr).to.equal('local-indexed-4');
					expect(JSON.parse(crud.fromBase64(stats.lastKey))).to.deep.equal({
						namespace: 'spec',
						key: 'key-4',
						localIndexedAttr: 'local-indexed-4',
					});

					expect(stats.count).to.equal(5);
				});

				it('should resume based on lastKey', done => {
					crud.fetch({
							limit: 5,
							indexName: 'localIndexedSpec',
							namespace: 'spec',
							select: 'localIndexedAttr',
							resume: stats.lastKey
						})
						.subscribe(response => {
							expect(response.items[0].localIndexedAttr).to.equal('local-indexed-5');
							expect(stats.count).to.equal(5);
						}, null, done);
				});
			});

			describe('with global index', () => {
				beforeEach(done => {
					crud.fetch({
							limit: 5,
							indexName: 'globalIndexedSpec',
							globalIndexedPartitionAttr: 'global-indexed-spec',
							select: 'globalIndexedPartitionAttr'
						})
						.subscribe(response => {
							items = response.items;
							stats = response.stats;
						}, null, done);
				});

				it('should get correct lastKey', () => {
					expect(items[0].globalIndexedSortAttr).to.equal('global-indexed-0');
					expect(JSON.parse(crud.fromBase64(stats.lastKey))).to.deep.equal({
						namespace: 'spec',
						key: 'key-4',
						globalIndexedSortAttr: 'global-indexed-4',
						globalIndexedPartitionAttr: 'global-indexed-spec',
					});

					expect(stats.count).to.equal(5);
				});

				it('should resume based on lastKey', done => {
					crud.fetch({
							limit: 5,
							indexName: 'globalIndexedSpec',
							globalIndexedPartitionAttr: 'global-indexed-spec',
							select: 'globalIndexedPartitionAttr',
							resume: stats.lastKey
						})
						.subscribe(response => {
							expect(response.items[0].globalIndexedSortAttr).to.equal('global-indexed-5');
							expect(stats.count).to.equal(5);
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
						stats = response.stats;
					}, null, done);
			});

			it('should fetch desc', () => {
				expect(items[0].key).to.equal('key-9');
				expect(stats.count).to.equal(10);
			});
		});

		describe('select', () => {
			beforeEach(done => {
				crud.fetch({
						select: 'key',
						namespace: 'spec'
					})
					.subscribe(response => {
						items = response.items;
						stats = response.stats;
					}, null, done);
			});

			it('should fetch just namespace and key', () => {
				expect(items[0]).to.deep.equal({
					namespace: 'spec',
					key: 'key-0'
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
						stats = response.stats;
					}, null, done);
			});

			it('should fetch one', () => {
				expect(stats.count).to.equal(1);
			});
		});

		describe('consistent', () => {
			beforeEach(done => {
				spy(Request.prototype, 'consistent');

				crud.fetch({
						consistent: true,
						namespace: 'spec',
					})
					.subscribe(response => {
						items = response.items;
						stats = response.stats;
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
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.fetch({
					namespace: 'spec'
				}, ({
					expression,
					request
				}) => {
					callback(expression);
					_request = spy(request, 'query');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('#partition = :partition');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('get', () => {
		let item;

		beforeEach(done => {
			crud.get({
					namespace: 'spec',
					key: 'key-3'
				})
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should get', () => {
			expect(item.key).to.equal('key-3');
		});

		describe('select', () => {
			beforeEach(done => {
				crud.get({
						select: 'key',
						namespace: 'spec',
						key: 'key-0'
					})
					.subscribe(response => {
						item = response;
					}, null, done);
			});

			it('should get', () => {
				expect(item).to.deep.equal({
					namespace: 'spec',
					key: 'key-0'
				});
			});
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.get({
					namespace: 'spec',
					key: 'key-3'
				}, ({
					partition,
					sort,
					request
				}) => {
					callback({
						partition,
						sort
					});

					_request = spy(request, 'get');

					return ['hooked'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith({
					partition: 'spec',
					sort: 'key-3'
				});
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked');
			});
		});
	});

	describe('insert', () => {
		let item;

		beforeEach(done => {
			crud.insert({
					namespace: 'spec',
					key: 'key-10'
				})
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		afterEach(done => {
			crud.delete({
					namespace: 'spec',
					key: 'key-10'
				})
				.subscribe(null, null, done);
		});

		it('should return inserted item', () => {
			expect(item.key).to.equal('key-10');
			expect(item).to.have.all.keys([
				'namespace',
				'key',
				'createdAt',
				'updatedAt',
			]);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.insert({
					namespace: 'spec',
					key: 'key-10',
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

					_request = spy(request, 'insert');

					return [{
						hooked: true
					}];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith({
					partition: 'spec',
					sort: 'key-10',
					args: {
						namespace: 'spec',
						key: 'key-10',
						title: 'title'
					}
				});
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith({
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
					key: 'key-0'
				})
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		afterEach(done => {
			crud.delete({
					namespace: 'spec',
					key: 'key-10'
				})
				.subscribe(null, null, done);
		});

		it('should return inserted or replaced item', () => {
			expect(item.key).to.equal('key-0');
			expect(item.createdAt).to.equal(item.updatedAt);
			expect(item).to.have.all.keys([
				'namespace',
				'key',
				'createdAt',
				'updatedAt'
			]);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0',
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

					_request = spy(request, 'insertOrReplace');

					return [{
						hooked: true
					}];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith({
					partition: 'spec',
					sort: 'key-0',
					args: {
						namespace: 'spec',
						key: 'key-0',
						title: 'title'
					}
				});
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith({
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
					key: 'key-1',
					title: 'title'
				})
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should return inserted or updated item', () => {
			expect(item.key).to.equal('key-1');
			expect(item.createdAt).not.to.equal(item.updatedAt);
			expect(item.createdAt).to.be.below(item.updatedAt);
			expect(item).to.have.all.keys([
				'globalIndexedPartitionAttr',
				'globalIndexedSortAttr',
				'localIndexedAttr',
				'namespace',
				'key',
				'title',
				'message',
				'createdAt',
				'updatedAt',
			]);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.insertOrUpdate({
					namespace: 'spec',
					key: 'key-1',
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

					_request = spy(request, 'insertOrUpdate');

					return [{
						hooked: true
					}];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith({
					partition: 'spec',
					sort: 'key-1',
					args: {
						namespace: 'spec',
						key: 'key-1',
						title: 'title'
					}
				});
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith({
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
					key: 'key-2',
					title: 'title'
				})
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should return updated item', () => {
			expect(item.key).to.equal('key-2');
			expect(item.createdAt).not.to.equal(item.updatedAt);
			expect(item.createdAt).to.be.below(item.updatedAt);
			expect(item).to.have.all.keys([
				'globalIndexedPartitionAttr',
				'globalIndexedSortAttr',
				'localIndexedAttr',
				'namespace',
				'key',
				'title',
				'message',
				'createdAt',
				'updatedAt',
			]);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.update({
					namespace: 'spec',
					key: 'key-2',
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

					_request = spy(request, 'update');

					return [{
						hooked: true
					}];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith({
					partition: 'spec',
					sort: 'key-2',
					args: {
						namespace: 'spec',
						key: 'key-2',
						title: 'title'
					}
				});
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith({
					hooked: true
				});
			});
		});
	});

	describe('delete', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-3'
				}).mergeMap(() => crud.delete({
					namespace: 'spec',
					key: 'key-3'
				}))
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should return deleted item', () => {
			expect(item.key).to.equal('key-3');
			expect(item).to.have.all.keys([
				'namespace',
				'key',
				'createdAt',
				'updatedAt',
			]);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.delete({
					namespace: 'spec',
					key: 'key-3'
				}, ({
					partition,
					sort,
					request
				}) => {
					callback({
						partition,
						sort
					});

					_request = spy(request, 'delete');

					return [{
						hooked: true
					}];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith({
					partition: 'spec',
					sort: 'key-3'
				});
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith({
					hooked: true
				});
			});
		});
	});

	describe('appendToList', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0'
				})
				.mergeMap(() => crud.appendToList({
					namespace,
					key: 'key-0',
					list: [{
						a: 1
					}, {
						b: 2
					}]
				}))
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should create a list', () => {
			expect(item.list).to.deep.equal([{
				a: 1
			}, {
				b: 2
			}]);
		});

		it('should append', done => {
			crud.appendToList({
					namespace,
					key: 'key-0',
					list: [{
						c: 3
					}, {
						d: 4
					}]
				})
				.subscribe(response => {
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

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.appendToList({
					namespace,
					key: 'key-0',
					list: [{
						e: 5
					}, {
						f: 6
					}]
				}, null, ({
					request,
					expression
				}) => {
					callback(expression.replace(/:appendList_\w*/g, ':appendList_{cuid}'));
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('SET #list = list_append(if_not_exists(#list, :emptyList), :appendList_{cuid}), #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('prependToList', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0'
				})
				.mergeMap(() => crud.prependToList({
					namespace,
					key: 'key-0',
					list: [{
						a: 1
					}, {
						b: 2
					}]
				}))
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should create a list', () => {
			expect(item.list).to.deep.equal([{
				a: 1
			}, {
				b: 2
			}]);
		});

		it('should prepend', done => {
			crud.prependToList({
					namespace,
					key: 'key-0',
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

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.prependToList({
					namespace,
					key: 'key-0',
					list: [{
						e: 5
					}, {
						f: 6
					}]
				}, ({
					request,
					expression
				}) => {
					callback(expression.replace(/:appendList_\w*/g, ':appendList_{cuid}'));
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('SET #list = list_append(:appendList_{cuid}, if_not_exists(#list, :emptyList)), #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('removeFromList', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0'
				})
				.mergeMap(() => crud.appendToList({
					namespace,
					key: 'key-0',
					list: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
				}))
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should pull with array', done => {
			crud.removeFromList({
					namespace,
					key: 'key-0',
					list: [0, 1]
				})
				.subscribe(response => {
					expect(response.list).to.deep.equal([2, 3, 4, 5, 6, 7, 8, 9]);
				}, null, done);
		});

		it('should pull with single value', done => {
			crud.removeFromList({
					namespace,
					key: 'key-0',
					list: 0
				})
				.subscribe(response => {
					expect(response.list).to.deep.equal([1, 2, 3, 4, 5, 6, 7, 8, 9]);
				}, null, done);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.removeFromList({
					namespace,
					key: 'key-0',
					list: [2, 3]
				}, ({
					request,
					expression
				}) => {
					callback(expression);
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('REMOVE #list[2], #list[3] SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('updateAtList', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0'
				})
				.mergeMap(() => crud.appendToList({
					namespace,
					key: 'key-0',
					list: [0, 1, 2, 3, 4, 5, 6, 7, {
						a: 1
					}]
				}))
				.subscribe(response => {
					item = response;
				}, null, done);
		});

		it('should update with primaries', done => {
			crud.updateAtList({
					namespace,
					key: 'key-0',
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
					namespace,
					key: 'key-0',
					list: {
						8: {
							a: 2
						}
					}
				})
				.subscribe(response => {
					expect(response.list).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, {
						a: 2
					}]);
				}, null, done);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.updateAtList({
					namespace,
					key: 'key-0',
					list: {
						5: 'updated 6'
					}
				}, ({
					request,
					expression
				}) => {
					callback(expression.replace(/:\w{7,8}/g, ':{cuid}'));
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('SET #list[5] = :{cuid}, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('addToSet', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0'
				})
				.subscribe(null, null, done);
		});

		it('should create a string set with array', done => {
			crud.addToSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b']
				})
				.subscribe(response => {
					expect(response.set).to.deep.equal(['a', 'b']);
				}, null, done);
		});

		it('should create a number set with array', done => {
			crud.addToSet({
					namespace,
					key: 'key-0',
					set: [1, 2]
				})
				.subscribe(response => {
					expect(response.set).to.deep.equal([1, 2]);
				}, null, done);
		});

		it('should create a string set with single value', done => {
			crud.addToSet({
					namespace,
					key: 'key-0',
					set: 'a'
				})
				.subscribe(response => {
					expect(response.set).to.deep.equal(['a']);
				}, null, done);
		});

		it('should create a number set with single value', done => {
			crud.addToSet({
					namespace,
					key: 'key-0',
					set: 1
				})
				.subscribe(response => {
					expect(response.set).to.deep.equal([1]);
				}, null, done);
		});

		it('should not duplicate', done => {
			crud.addToSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b']
				})
				.mergeMap(() => crud.addToSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b']
				}))
				.subscribe(response => {
					expect(response.set).to.deep.equal(['a', 'b']);
				}, null, done);
		});


		it('should not create a set', done => {
			crud.addToSet({
					namespace,
					key: 'key-0',
					set: [1, 'b']
				})
				.subscribe(null, err => {
					expect(err.message).to.equal('Invalid UpdateExpression: Syntax error; token: "SET", near: "ADD  SET #createdAt"');

					done();
				});
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.addToSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b']
				}, ({
					request,
					expression
				}) => {
					callback(expression);
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('ADD #set :set SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('removeFromSet', () => {
		let item;

		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0'
				})
				.mergeMap(() => crud.addToSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b', 'c', 'd']
				}))
				.subscribe(null, null, done);
		});

		it('should pull', done => {
			crud.removeFromSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b']
				})
				.subscribe(response => {
					expect(response.set).to.deep.equal(['c', 'd']);
				}, null, done);
		});

		it('should remove attribute is set is empty', done => {
			crud.removeFromSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b', 'c', 'd']
				})
				.subscribe(response => {
					expect(response.set).to.be.undefined;
				}, null, done);
		});

		it('should do nothing if inexistent', done => {
			crud.removeFromSet({
					namespace,
					key: 'key-0',
					set: ['e', 'f']
				})
				.subscribe(response => {
					expect(response.set).to.deep.equal(['a', 'b', 'c', 'd']);
				}, null, done);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.removeFromSet({
					namespace,
					key: 'key-0',
					set: ['a', 'b']
				}, ({
					request,
					expression
				}) => {
					callback(expression);
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('DELETE #set :set SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('remove attribute', () => {
		beforeEach(done => {
			crud.insertOrReplace({
					namespace: 'spec',
					key: 'key-0',
					title: 'title'
				})
				.subscribe(null, null, done);
		});

		it('should remove attribute', done => {
			crud.removeAttributes({
					namespace: 'spec',
					key: 'key-0',
					title: 'title'
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						namespace: 'spec',
						key: 'key-0',
						updatedAt: response.updatedAt,
						createdAt: response.createdAt
					});
				}, null, done);
		});

		describe('hook', () => {
			let callback;
			let _request;

			beforeEach(() => {
				callback = stub();

				crud.removeAttributes({
					namespace: 'spec',
					key: 'key-0',
					title: 'title'
				}, ({
					request,
					expression
				}) => {
					callback(expression);
					_request = spy(request, 'update');

					return ['hooked expression'];
				});
			});

			afterEach(() => {
				_request.restore();
			});

			it('should callback be called with hookArgs', () => {
				expect(callback).to.have.been.calledWith('REMOVE #title SET #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
			});

			it('should query be called with hookArgs', () => {
				expect(_request).to.have.been.calledWith('hooked expression');
			});
		});
	});

	describe('clear', done => {
		it('should clear table', () => {
			crud.clear({
					namespace: 'spec'
				})
				.mergeMap(() => crud.fetch({
					namespace
				}))
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
