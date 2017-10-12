const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const _ = require('lodash');

const {
	Select,
	ReturnValues,
	ConsumedCapacity
} = require('../../');

chai.use(sinonChai);

const expect = chai.expect;

describe('lib/constants', () => {
	it('should Select to have [ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT]', () => {
		expect(Select).to.deep.equal({
			ALL_ATTRIBUTES: 0,
			ALL_PROJECTED_ATTRIBUTES: 1,
			SPECIFIC_ATTRIBUTES: 2,
			COUNT: 3
		});
	});

	it('should Select to have [NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW]', () => {
		expect(ReturnValues).to.deep.equal({
			NONE: 'NONE',
			ALL_OLD: 'ALL_OLD',
			UPDATED_OLD: 'UPDATED_OLD',
			ALL_NEW: 'ALL_NEW',
			UPDATED_NEW: 'UPDATED_NEW'
		});
	});

	it('should ConsumedCapacity to have [NONE, TOTAL, INDEXES]', () => {
		expect(ConsumedCapacity).to.deep.equal({
			NONE: 'NONE',
			TOTAL: 'TOTAL',
			INDEXES: 'INDEXES'
		});
	});
});
