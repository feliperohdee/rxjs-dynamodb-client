import _ from 'lodash';
import {
	DynamoDB as AWSDynamoDB
} from 'aws-sdk';
import {
	DynamoDB,
	Util,
	Request
} from 'src';

import {
	instance
} from 'testingEnv';

describe('src/index', () => {
	describe('constructor', () => {
		it('should throw if not deps.client', () => {			
			expect(() => new DynamoDB()).to.throw('no dynamoDb client provided.');
		});

		it('client be instance of AWSDynamoDB', () => {			
			expect(instance.client).to.be.instanceOf(AWSDynamoDB);
		});
	});

	describe('request', () => {
		it('should request be unique', () => {
			const request = instance.request;
			const request2 = instance.request;

			expect(request === request2).to.be.false;
			expect(request).to.be.instanceOf(Request);
		});
	});

	describe('util', () => {
		it('should util be unique', () => {
			const util = instance.util;
			const util2 = instance.util;

			expect(util === util2).to.be.false;
			expect(util).to.be.instanceOf(Util);
		});
	});

	describe('table', () => {
		it('should return a request instance', () => {
			const request = instance.table('someTable', {
				partition: 'partition',
				sort: 'sort'
			});

			expect(request).to.be.instanceOf(Request);
		});
	});

	describe('call', () => {
		let routeCall;

		beforeEach(() => {
			routeCall = stub(Request.prototype, 'routeCall');
		});

		afterEach(() => {
			routeCall.restore();
		});

		it('should call routeCall', () => {
			instance.call('someMethod', {});

			expect(routeCall).to.have.been.calledOnce;
		});
	});

	describe('S', () => {
		it('should return Util instance with S type data', () => {
			const S = instance.S('string');

			expect(S).to.be.instanceOf(Util);
			expect(S.data).to.deep.equal({
				S: 'string'
			});
		});
	});

	describe('N', () => {
		it('should return Util instance with N type data', () => {
			const N = instance.N(9);

			expect(N).to.be.instanceOf(Util);
			expect(N.data).to.deep.equal({
				N: '9'
			});
		});
	});

	describe('SS', () => {
		it('should return Util instance with SS type data', () => {
			const SS = instance.SS(['a', 'b', 'c']);

			expect(SS).to.be.instanceOf(Util);
			expect(SS.data).to.deep.equal({
				SS: ['a', 'b', 'c']
			});
		});
	});

	describe('NS', () => {
		it('should return Util instance with NS type data', () => {
			const NS = instance.NS([1, 2, 3]);

			expect(NS).to.be.instanceOf(Util);
			expect(NS.data).to.deep.equal({
				NS: ['1', '2', '3']
			});
		});
	});

	describe('L', () => {
		it('should return Util instance with L type data', () => {
			const L = instance.L(['a', 1, {
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
