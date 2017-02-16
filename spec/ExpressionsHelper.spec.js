import _ from 'lodash';
import cuid from 'cuid';
import {
	ExpressionsHelper
} from 'src';

import {
	dynamoDb
} from 'testingEnv';

const _global = {};
const tableName = 'tblSpec';
const tableSchema = {
	primaryKeys: {
		partition: 'namespace',
		sort: 'id'
	}
};

describe('src/ExpressionsHelper', () => {
	let expressionsHelper;
	let request;
	let cuidIndex = 0;

	beforeEach(() => {
		_global.slug = stub(cuid, 'slug').callsFake(() => {
			return `{cuid_${cuidIndex++}}`
		});

		request = dynamoDb.table(tableName, tableSchema);
		expressionsHelper = new ExpressionsHelper(request);
	});

	afterEach(() => {
		cuidIndex = 0;
		_global.slug.restore();
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
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.attrNotExists('namespace');

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith('namespace');
		});

		it('should make expression', () => {
			const result = expressionsHelper.attrNotExists('namespace');

			expect(result).to.equal('attribute_not_exists(#namespace)');
		});
	});

	describe('attrExists', () => {
		beforeEach(() => {
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.attrExists('namespace');

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith('namespace');
		});

		it('should make expression', () => {
			const result = expressionsHelper.attrExists('namespace');

			expect(result).to.equal('attribute_exists(#namespace)');
		});
	});

	describe('prependList', () => {
		beforeEach(() => {
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.prependList('list', 123);

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith('list');
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.prependList('list', 123);

			expect(_global.addPlaceholderValue).to.have.been.calledTwice;
			expect(_global.addPlaceholderValue.firstCall).to.have.been.calledWith({
				'appendList_{cuid_0}': [123]
			});
			expect(_global.addPlaceholderValue.secondCall).to.have.been.calledWith({
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
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.appendList('list', 123);

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith('list');
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.appendList('list', 123);

			expect(_global.addPlaceholderValue).to.have.been.calledTwice;
			expect(_global.addPlaceholderValue.firstCall).to.have.been.calledWith({
				'appendList_{cuid_0}': [123]
			});
			expect(_global.addPlaceholderValue.secondCall).to.have.been.calledWith({
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
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.ifNotExists('list', 123);

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith('list');
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.ifNotExists('list', 123);

			expect(_global.addPlaceholderValue).to.have.been.calledOnce;
		});

		it('should make expression', () => {
			const result = expressionsHelper.ifNotExists('list', 123);

			expect(result).to.equal('#list = if_not_exists(#list, :ifNotExists_{cuid_0})');
		});
	});

	describe('contains', () => {
		beforeEach(() => {
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
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

				expect(_global.addPlaceholderName).to.have.been.calledOnce;
				expect(_global.addPlaceholderName).to.have.been.calledWith('list');
			});

			it('should call addPlaceholderValue', () => {
				expressionsHelper.contains('list', [123, 124, 125]);

				expect(_global.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.contains('list', [123, 124, 125]);

				expect(result).to.equal('(contains(#list, :cFilter_{cuid_0}) OR contains(#list, :cFilter_{cuid_1}) OR contains(#list, :cFilter_{cuid_2}))');
			});
		});

		describe('AND', () => {
			it('should call addPlaceholderName', () => {
				expressionsHelper.contains('list', [123, 124, 125], 'AND');

				expect(_global.addPlaceholderName).to.have.been.calledOnce;
				expect(_global.addPlaceholderName).to.have.been.calledWith('list');
			});

			it('should call addPlaceholderValue', () => {
				expressionsHelper.contains('list', [123, 124, 125], 'AND');

				expect(_global.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.contains('list', [123, 124, 125], 'AND');

				expect(result).to.equal('(contains(#list, :cFilter_{cuid_0}) AND contains(#list, :cFilter_{cuid_1}) AND contains(#list, :cFilter_{cuid_2}))');
			});
		});
	});

	describe('between', () => {
		beforeEach(() => {
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.between('value', 123);

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith('value');
		});

		describe('min only', () => {
			it('should call addPlaceholderValue', () => {
				expressionsHelper.between('value', 123);

				expect(_global.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.between('value', 123);

				expect(result).to.equal('#value >= :min');
			});
		});

		describe('max only', () => {
			it('should call addPlaceholderValue', () => {
				expressionsHelper.between('value', null, 123);

				expect(_global.addPlaceholderValue).to.have.been.calledOnce;
			});

			it('should make expression', () => {
				const result = expressionsHelper.between('value', null, 123);

				expect(result).to.equal('#value <= :max');
			});
		});

		describe('min and max', () => {
			it('should call addPlaceholderValue', () => {
				expressionsHelper.between('value', 122, 123);

				expect(_global.addPlaceholderValue).to.have.been.calledTwice;
			});

			it('should make expression', () => {
				const result = expressionsHelper.between('value', 122, 123);

				expect(result).to.equal('#value BETWEEN :min AND :max');
			});
		});
	});

	describe('update', () => {
		beforeEach(() => {
			_global.now = stub(_, 'now').returns(1);
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.now.restore();
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
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

			expect(_global.addPlaceholderName).to.have.been.calledThrice;
			expect(_global.addPlaceholderName).to.have.been.calledWith('name');
			expect(_global.addPlaceholderName).to.have.been.calledWith('age');
			expect(_global.addPlaceholderName).to.have.been.calledWith(['createdAt', 'updatedAt']);
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

			expect(_global.addPlaceholderValue).to.have.been.calledThrice;
			expect(_global.addPlaceholderValue).to.have.been.calledWith({
				name: 'name'
			});
			expect(_global.addPlaceholderValue).to.have.been.calledWith({
				age: 'age'
			});
			expect(_global.addPlaceholderValue).to.have.been.calledWith({
				now: 1
			});
		});

		it('should make expression', () => {
			expect(expressionsHelper.update({
				namespace: 'namespace',
				id: 'id',
				name: 'name',
				age: 'age',
				createdAt: 1,
				updatedAt: 1
			})).to.equal('#age = :age, #name = :name, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
		});

		it('should make expression without timestamp', () => {
			expect(expressionsHelper.update({
				namespace: 'namespace',
				id: 'id',
				name: 'name',
				age: 'age',
				createdAt: 1,
				updatedAt: 1
			}, false)).to.equal('#age = :age, #name = :name');
		});
	});

	describe('timestamp', () => {
		beforeEach(() => {
			_global.now = stub(_, 'now').returns(1);
			_global.addPlaceholderName = spy(request, 'addPlaceholderName');
			_global.addPlaceholderValue = spy(request, 'addPlaceholderValue');
		});

		afterEach(() => {
			_global.now.restore();
			_global.addPlaceholderName.restore();
			_global.addPlaceholderValue.restore();
		});

		it('should call addPlaceholderName', () => {
			expressionsHelper.timestamp();

			expect(_global.addPlaceholderName).to.have.been.calledOnce;
			expect(_global.addPlaceholderName).to.have.been.calledWith(['createdAt', 'updatedAt']);
		});

		it('should call addPlaceholderValue', () => {
			expressionsHelper.timestamp();

			expect(_global.addPlaceholderValue).to.have.been.calledOnce;
			expect(_global.addPlaceholderValue).to.have.been.calledWith({
				now: 1
			});
		});

		it('should make expression', () => {
			expect(expressionsHelper.timestamp()).to.equal('#createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now');
		});
	});
});
