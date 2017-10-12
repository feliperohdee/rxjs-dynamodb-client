const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const _ = require('lodash');
const {
	Observable
} = require('rxjs');
const {
	DynamoDB: AWSDynamoDB
} = require('aws-sdk');

const {
	DynamoDB,
	Util,
	Request
} = require('../');
const {
	dynamoDb
} = require('../testing/');

chai.use(sinonChai);

const expect = chai.expect;

describe('src/index', () => {
	describe('constructor', () => {
		it('should throw if not deps.client', () => {
			expect(() => new DynamoDB()).to.throw('no dynamoDb client provided.');
		});

		it('client be dynamoDb of AWSDynamoDB', () => {
			expect(dynamoDb.client).to.be.instanceOf(AWSDynamoDB);
		});
	});

	describe('onRetryableError', () => {
		let request;
		let callback;
		let err;

		beforeEach(() => {
			request = dynamoDb.request;
			callback = sinon.stub();
			err = new Error();
			err.code = 'LimitExceededException';
			err.statusCode = 400;
			err.retryable = true;

			sinon.stub(request, 'insert')
				.callsFake(() => {
					let index = 0;

					return Observable.create(subscriber => {
						err.retryDelay = index++;
						callback();

						subscriber.error(err);
					});
				});
		});

		afterEach(() => {
			request.insert.restore();
		});

		it('should retry 1x by default', done => {
			request.insert()
				.onRetryableError()
				.subscribe(null, err => {
					expect(callback).to.have.been.callCount(2);
					expect(err.retryDelay).to.equal(1);

					done();
				});
		});

		it('should retry 2x', done => {
			request.insert()
				.onRetryableError(() => 2)
				.subscribe(null, err => {
					expect(callback).to.have.been.callCount(3);
					expect(err.retryDelay).to.equal(2);

					done();
				});
		});

		it('should retry 3x', done => {
			request.insert()
				.onRetryableError(3)
				.subscribe(null, err => {
					expect(callback).to.have.been.callCount(4);
					expect(err.retryDelay).to.equal(3);

					done();
				});
		});

		it('should retry 3x by 50ms (150ms total)', done => {
			request.insert()
				.onRetryableError({
					max: 3,
					delay: 50
				})
				.subscribe(null, err => {
					expect(callback).to.have.been.callCount(4);
					expect(err.retryDelay).to.equal(3);

					done();
				});
		});

		it('should retry 4x', done => {
			request.insert()
				.onRetryableError((err, index) => ({
					max: 10,
					retryable: index >= 4 ? false : err.retryable
				}))
				.subscribe(null, err => {
					expect(callback).to.have.been.callCount(5);
					expect(err.retryDelay).to.equal(4);

					done();
				});
		});

		describe('not retryable', () => {
			beforeEach(() => {
				err.retryable = false;
			});

			it('should not retry', done => {
				request.insert()
					.onRetryableError()
					.subscribe(null, err => {
						expect(callback).to.have.been.callCount(1);
						expect(err.retryDelay).to.equal(0);

						done();
					});
			});
		});
	});

	describe('request', () => {
		it('should request be unique', () => {
			const request = dynamoDb.request;
			const request2 = dynamoDb.request;

			expect(request === request2).to.be.false;
			expect(request).to.be.instanceOf(Request);
		});
	});

	describe('util', () => {
		it('should util be unique', () => {
			const util = dynamoDb.util;
			const util2 = dynamoDb.util;

			expect(util === util2).to.be.false;
			expect(util).to.be.instanceOf(Util);
		});
	});

	describe('table', () => {
		it('should return a request dynamoDb', () => {
			const request = dynamoDb.table('someTable', {
				partition: 'partition',
				sort: 'sort'
			});

			expect(request).to.be.instanceOf(Request);
		});
	});

	describe('call', () => {
		beforeEach(() => {
			sinon.stub(Request.prototype, 'routeCall');
		});

		afterEach(() => {
			Request.prototype.routeCall.restore();
		});

		it('should call Request.prototype.routeCall', () => {
			dynamoDb.call('someMethod', {});

			expect(Request.prototype.routeCall).to.have.been.calledOnce;
		});
	});

	describe('S', () => {
		it('should return Util dynamoDb with S type data', () => {
			const S = dynamoDb.S('string');

			expect(S).to.be.instanceOf(Util);
			expect(S.data).to.deep.equal({
				S: 'string'
			});
		});
	});

	describe('N', () => {
		it('should return Util dynamoDb with N type data', () => {
			const N = dynamoDb.N(9);

			expect(N).to.be.instanceOf(Util);
			expect(N.data).to.deep.equal({
				N: '9'
			});
		});
	});

	describe('SS', () => {
		it('should return Util dynamoDb with SS type data', () => {
			const SS = dynamoDb.SS(['a', 'b', 'c']);

			expect(SS).to.be.instanceOf(Util);
			expect(SS.data).to.deep.equal({
				SS: ['a', 'b', 'c']
			});
		});
	});

	describe('NS', () => {
		it('should return Util dynamoDb with NS type data', () => {
			const NS = dynamoDb.NS([1, 2, 3]);

			expect(NS).to.be.instanceOf(Util);
			expect(NS.data).to.deep.equal({
				NS: ['1', '2', '3']
			});
		});
	});

	describe('L', () => {
		it('should return Util dynamoDb with L type data', () => {
			const L = dynamoDb.L(['a', 1, {
				key: 'value'
			}]);

			expect(L).to.be.instanceOf(Util);
			expect(L.data).to.deep.equal({
				L: [{
					S: 'a'
				}, {
					N: '1'
				}, {
					M: {
						key: {
							S: 'value'
						}
					}
				}]
			});
		});
	});
});
