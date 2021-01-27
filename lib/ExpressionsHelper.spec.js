const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const cuid = require('cuid');

const _ = require('./lodash');
const {
	ExpressionsHelper
} = require('../');
const {
	dynamoDb
} = require('../testing');

const tableName = 'tblSpec';
const tableSchema = {
	primaryKeys: {
		partition: 'namespace',
		sort: 'id'
	}
};

chai.use(sinonChai);

const expect = chai.expect;

describe('lib/ExpressionsHelper', () => {
	let expressionsHelper;
	let request;
	let cuidIndex = 0;

	beforeEach(() => {
		sinon.stub(cuid, 'slug').callsFake(() => {
			return `{cuid_${cuidIndex++}}`;
		});

		request = dynamoDb.table(tableName, tableSchema);
		expressionsHelper = new ExpressionsHelper(request);
	});

	afterEach(() => {
		cuidIndex = 0;
		cuid.slug.restore();
	});

	describe('contructor', () => {
		it('should set a request', () => {
			expect(expressionsHelper.request).to.equal(request);
		});
	});

	describe('getTokens', () => {
		it('should extract tokens', () => {
			expect(expressionsHelper.getTokens('aaa.bbb.ccc')).to.deep.equal(['aaa', 'bbb', 'ccc']);
		});
	});

	describe('attrNotExists', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.attrNotExists('namespace');

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith('namespace');
		});

		it('should make expression', () => {
			const result = expressionsHelper.attrNotExists('namespace');

			expect(result).to.equal('attribute_not_exists(#namespace)');
		});
	});

	describe('attrExists', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.attrExists('namespace');

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith('namespace');
		});

		it('should make expression', () => {
			const result = expressionsHelper.attrExists('namespace');

			expect(result).to.equal('attribute_exists(#namespace)');
		});
	});

	describe('prependList', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.prependList('list', 123);

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith('list');
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.prependList('list', 123);

			expect(request.addPlaceholderValue).to.have.been.calledTwice;
			expect(request.addPlaceholderValue.firstCall).to.have.been.calledWith({
				'appendList_{cuid_0}': [123]
			});
			expect(request.addPlaceholderValue.secondCall).to.have.been.calledWith({
				emptyList: []
			});
		});

		it('should make expression', () => {
			const result = expressionsHelper.prependList('list', 123);

			expect(result).to.equal('#list = list_append(:appendList_{cuid_0}, if_not_exists\(#list, :emptyList))');
		});

		it('should make expression with value as array', () => {
			const result = expressionsHelper.prependList('list', [123]);

			expect(result).to.equal('#list = list_append(:appendList_{cuid_0}, if_not_exists\(#list, :emptyList))');
		});
	});

	describe('appendList', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.appendList('list', 123);

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith('list');
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.appendList('list', 123);

			expect(request.addPlaceholderValue).to.have.been.calledTwice;
			expect(request.addPlaceholderValue.firstCall).to.have.been.calledWith({
				'appendList_{cuid_0}': [123]
			});
			expect(request.addPlaceholderValue.secondCall).to.have.been.calledWith({
				emptyList: []
			});
		});

		it('should make expression', () => {
			const result = expressionsHelper.appendList('list', 123);

			expect(result).to.equal('#list = list_append(if_not_exists(#list, :emptyList), :appendList_{cuid_0})');
		});

		it('should make expression with value as array', () => {
			const result = expressionsHelper.appendList('list', [123]);

			expect(result).to.equal('#list = list_append(if_not_exists(#list, :emptyList), :appendList_{cuid_0})');
		});
	});

	describe('ifNotExists', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.ifNotExists('list', 123);

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith('list');
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.ifNotExists('list', 123);

			expect(request.addPlaceholderValue).to.have.been.calledOnce;
		});

		it('should make expression', () => {
			const result = expressionsHelper.ifNotExists('list', 123);

			expect(result).to.equal('#list = if_not_exists(#list, :ifNotExists_{cuid_0})');
		});
	});

	describe('contains', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should make expression without array', () => {
			const result = expressionsHelper.contains('list', 123);

			expect(result).to.equal('(contains(#list, :cFilter_{cuid_0}))');
		});

		it('should returns undefined if empty', () => {
			expect(expressionsHelper.contains('list')).to.be.undefined;
		});

		describe('OR', () => {
			it('should call addPlaceholderName', () => {
				expressionsHelper.contains('list', [123, 124, 125]);

				expect(request.addPlaceholderName).to.have.been.calledOnce;
				expect(request.addPlaceholderName).to.have.been.calledWith('list');
			});

			it('should call addPlaceholderValue', () => {
				expressionsHelper.contains('list', [123, 124, 125]);

				expect(request.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.contains('list', [123, 124, 125]);

				expect(result).to.equal('(contains(#list, :cFilter_{cuid_0}) OR contains(#list, :cFilter_{cuid_1}) OR contains(#list, :cFilter_{cuid_2}))');
			});
		});

		describe('AND', () => {
			it('should call addPlaceholderName', () => {
				expressionsHelper.contains('list', [123, 124, 125], 'AND');

				expect(request.addPlaceholderName).to.have.been.calledOnce;
				expect(request.addPlaceholderName).to.have.been.calledWith('list');
			});

			it('should call addPlaceholderValue', () => {
				expressionsHelper.contains('list', [123, 124, 125], 'AND');

				expect(request.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.contains('list', [123, 124, 125], 'AND');

				expect(result).to.equal('(contains(#list, :cFilter_{cuid_0}) AND contains(#list, :cFilter_{cuid_1}) AND contains(#list, :cFilter_{cuid_2}))');
			});
		});
	});

	describe('between', () => {
		beforeEach(() => {
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.between('value', 123);

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith('value');
		});

		describe('min only', () => {
			it('should call addPlaceholderValue', () => {
				expressionsHelper.between('value', 123);

				expect(request.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.between('value', 123);

				expect(result).to.equal('#value >= :min');
			});
		});

		describe('max only', () => {
			it('should call addPlaceholderValue', () => {
				expressionsHelper.between('value', null, 123);

				expect(request.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.between('value', null, 123);

				expect(result).to.equal('#value <= :max');
			});
		});

		describe('min and max', () => {
			it('should call addPlaceholderValue', () => {
				expressionsHelper.between('value', 122, 123);

				expect(request.addPlaceholderValue).to.have.been.calledTwice;
			});

			it('should make expression', () => {
				const result = expressionsHelper.between('value', 122, 123);

				expect(result).to.equal('#value BETWEEN :min AND :max');
			});
		});
	});

	describe('update', () => {
		beforeEach(() => {
			sinon.stub(_, 'now').returns(1);
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_.now.restore();
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.update({
				namespace: 'namespace',
				id: 'id',
				name: 'name',
				age: 'age',
				createdAt: 1,
				updatedAt: 1
			});

			expect(request.addPlaceholderName).to.have.been.calledThrice;
			expect(request.addPlaceholderName).to.have.been.calledWith('name');
			expect(request.addPlaceholderName).to.have.been.calledWith('age');
			expect(request.addPlaceholderName).to.have.been.calledWith(['createdAt', 'updatedAt']);
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.update({
				namespace: 'namespace',
				id: 'id',
				name: 'name',
				age: 'age',
				createdAt: 1,
				updatedAt: 1
			});

			expect(request.addPlaceholderValue).to.have.been.calledThrice;
			expect(request.addPlaceholderValue).to.have.been.calledWith({
				name: 'name'
			});
			expect(request.addPlaceholderValue).to.have.been.calledWith({
				age: 'age'
			});
			expect(request.addPlaceholderValue).to.have.been.calledWith({
				now: 1
			});
		});

		it('should make expression ignoring undefined', () => {
			expect(expressionsHelper.update({
				createdAt: 1,
				updatedAt: 1,
				string: 'string',
				undefined: undefined,
				nulled: null,
				zero: 0,
				one: 1,
				truthy: true,
				falsy: false
			})).to.equal('#falsy = :falsy, #truthy = :truthy, #one = :one, #zero = :zero, #nulled = :nulled, #string = :string, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
		});

		it('should make expression without timestamp', () => {
			expect(expressionsHelper.update({
				createdAt: 1,
				updatedAt: 1,
				string: 'string',
				undefined: undefined,
				nulled: null,
				zero: 0,
				one: 1,
				truthy: true,
				falsy: false
			}, false)).to.equal('#falsy = :falsy, #truthy = :truthy, #one = :one, #zero = :zero, #nulled = :nulled, #string = :string');
		});

		it('should make expression with condition.ifNotExists option', () => {
			expect(expressionsHelper.update({
				createdAt: 1,
				updatedAt: 1,
				string: 'string',
				string2: {
					value: 'string',
					condition: 'ifNotExists'
				},
				undefined: undefined,
				undefined2: {
					value: undefined,
					condition: 'ifNotExists'
				},
				nulled: null,
				nulled2: {
					value: null,
					condition: 'ifNotExists'
				},
				zero: 0,
				zero2: {
					value: 0,
					condition: 'ifNotExists'
				},
				one: 1,
				one2: {
					value: 1,
					condition: 'ifNotExists'
				},
				truthy: true,
				truthy2: {
					value: true,
					condition: 'ifNotExists'
				},
				falsy: false,
				falsy2: {
					value: false,
					condition: 'ifNotExists'
				}
			}, false)).to.equal('#falsy2 = if_not_exists(#falsy2, :falsy2), #falsy = :falsy, #truthy2 = if_not_exists(#truthy2, :truthy2), #truthy = :truthy, #one2 = if_not_exists(#one2, :one2), #one = :one, #zero2 = if_not_exists(#zero2, :zero2), #zero = :zero, #nulled2 = if_not_exists(#nulled2, :nulled2), #nulled = :nulled, #string2 = if_not_exists(#string2, :string2), #string = :string');
		});
	});

	describe('timestamp', () => {
		beforeEach(() => {
			sinon.stub(_, 'now').returns(1);
			sinon.spy(request, 'addPlaceholderName');
			sinon.spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_.now.restore();
			request.addPlaceholderName.restore();
			request.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.timestamp();

			expect(request.addPlaceholderName).to.have.been.calledOnce;
			expect(request.addPlaceholderName).to.have.been.calledWith(['createdAt', 'updatedAt']);
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.timestamp();

			expect(request.addPlaceholderValue).to.have.been.calledOnce;
			expect(request.addPlaceholderValue).to.have.been.calledWith({
				now: 1
			});
		});

		it('should make expression', () => {
			expect(expressionsHelper.timestamp()).to.equal('#createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
		});
	});
});
