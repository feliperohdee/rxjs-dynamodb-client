const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const {
	DynamoDB: AWSDynamoDB
} = require('aws-sdk');

const {
	DynamoDB,
	Util,
	Request
} = require('./');
const {
	dynamodb
} = require('./testing/');

chai.use(sinonChai);

const expect = chai.expect;

describe('index', () => {
	describe('constructor', () => {
		it('should throw if not deps.client', () => {
			expect(() => new DynamoDB()).to.throw('no dynamodb client provided.');
		});

		it('client be dynamodb of AWSDynamoDB', () => {
			expect(dynamodb.client).to.be.instanceOf(AWSDynamoDB);
		});
	});

	describe('request', () => {
		it('should request be unique', () => {
			const request = dynamodb.request;
			const request2 = dynamodb.request;

			expect(request === request2).to.be.false;
			expect(request).to.be.instanceOf(Request);
		});
	});

	describe('util', () => {
		it('should util be unique', () => {
			const util = dynamodb.util;
			const util2 = dynamodb.util;

			expect(util === util2).to.be.false;
			expect(util).to.be.instanceOf(Util);
		});
	});

	describe('table', () => {
		it('should return a request dynamodb', () => {
			const request = dynamodb.table('someTable', {
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
			dynamodb.call('someMethod', {});

			expect(Request.prototype.routeCall).to.have.been.calledOnce;
		});
	});

	describe('S', () => {
		it('should return Util dynamodb with S type data', () => {
			const S = dynamodb.S('string');

			expect(S).to.be.instanceOf(Util);
			expect(S.data).to.deep.equal({
				S: 'string'
			});
		});
	});

	describe('N', () => {
		it('should return Util dynamodb with N type data', () => {
			const N = dynamodb.N(9);

			expect(N).to.be.instanceOf(Util);
			expect(N.data).to.deep.equal({
				N: '9'
			});
		});
	});

	describe('SS', () => {
		it('should return Util dynamodb with SS type data', () => {
			const SS = dynamodb.SS(['a', 'b', 'c']);

			expect(SS).to.be.instanceOf(Util);
			expect(SS.data).to.deep.equal({
				SS: ['a', 'b', 'c']
			});
		});
	});

	describe('NS', () => {
		it('should return Util dynamodb with NS type data', () => {
			const NS = dynamodb.NS([1, 2, 3]);

			expect(NS).to.be.instanceOf(Util);
			expect(NS.data).to.deep.equal({
				NS: ['1', '2', '3']
			});
		});
	});

	describe('L', () => {
		it('should return Util dynamodb with L type data', () => {
			const L = dynamodb.L(['a', 1, {
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
