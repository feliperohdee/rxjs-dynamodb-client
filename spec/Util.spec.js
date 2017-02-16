import {
	Util
} from 'src';

describe('src/Util', () => {
	let util;
	let normalItem;
	let anormalItem;

	beforeEach(() => {
		util = new Util();
		normalItem = {
			booleanTruthy: true,
			booleanFalsy: false,
			string: 'someString',
			emptyString: '',
			null: null,
			negativeInteger: -9,
			integerZero: 0,
			integer: 9,
			float: 0.5,
			negativeFloat: -0.5,
			numberArray: [1, 2, 3],
			stringArray: ['a', 'b'],
			array: [1, 'a'],
			map: {
				key: 'value',
				deepMap: {
					key: 9
				}
			}
		}

		anormalItem = {
			booleanTruthy: {
				BOOL: true
			},
			booleanFalsy: {
				BOOL: false
			},
			string: {
				S: 'someString'
			},
			emptyString: {
				S: ':empty'
			},
			null: {
				NULL: true
			},
			negativeInteger: {
				N: '-9'
			},
			integerZero: {
				N: '0'
			},
			integer: {
				N: '9'
			},
			float: {
				N: '0.5'
			},
			negativeFloat: {
				N: '-0.5'
			},
			numberArray: {
				L: [{
					N: '1'
				}, {
					N: '2'
				}, {
					N: '3'
				}]
			},
			stringArray: {
				L: [{
					S: 'a'
				}, {
					S: 'b'
				}]
			},
			array: {
				L: [{
					N: '1'
				}, {
					S: 'a'
				}]
			},
			map: {
				M: {
					key: {
						S: 'value'
					},
					deepMap: {
						M: {
							key: {
								N: '9'
							}
						}
					}
				}
			}
		}
	});

	describe('raw', () => {
		it('should wrap data at Util object', () => {
			util.raw(normalItem);

			expect(util.data).not.to.be.undefined;
		});

		it('should return util instance', () => {
			const raw = util.raw(normalItem);

			expect(raw).to.be.instanceOf(Util);
		});
	});

	describe('anormalizeValue', () => {
		it('should convert boolean truthy to BOOL', () => {
			expect(util.anormalizeValue(normalItem.booleanTruthy)).to.deep.equal(anormalItem.booleanTruthy);
		});

		it('should convert boolean falsy to BOOL', () => {
			expect(util.anormalizeValue(normalItem.booleanFalsy)).to.deep.equal(anormalItem.booleanFalsy);
		});

		it('should convert positive integer to N', () => {
			expect(util.anormalizeValue(normalItem.integer)).to.deep.equal(anormalItem.integer);
		});

		it('should convert integer zero to N', () => {
			expect(util.anormalizeValue(normalItem.integerZero)).to.deep.equal(anormalItem.integerZero);
		});

		it('should convert negative integer to N', () => {
			expect(util.anormalizeValue(normalItem.negativeInteger)).to.deep.equal(anormalItem.negativeInteger);
		});

		it('should convert float to N', () => {
			expect(util.anormalizeValue(normalItem.float)).to.deep.equal(anormalItem.float);
		});

		it('should convert negative float to N', () => {
			expect(util.anormalizeValue(normalItem.negativeFloat)).to.deep.equal(anormalItem.negativeFloat);
		});

		it('should convert string to S', () => {
			expect(util.anormalizeValue(normalItem.string)).to.deep.equal(anormalItem.string);
		});

		it('should convert null to NULL', () => {
			expect(util.anormalizeValue(normalItem.null)).to.deep.equal(anormalItem.null);
		});

		it('should handle raw', () => {
			expect(util.anormalizeValue(util.raw({
				NS: [1, 2, 3]
			}))).to.deep.equal({
				NS: [1, 2, 3]
			});
		});

		it('should convert number array to L', () => {
			expect(util.anormalizeValue(normalItem.numberArray)).to.deep.equal(anormalItem.numberArray);
		});

		it('should convert string array to L', () => {
			expect(util.anormalizeValue(normalItem.stringArray)).to.deep.equal(anormalItem.stringArray);
		});

		it('should convert combined array to L', () => {
			expect(util.anormalizeValue(normalItem.array)).to.deep.equal(anormalItem.array);
		});

		it('should convert map to M', () => {
			expect(util.anormalizeValue(normalItem.map)).to.deep.equal(anormalItem.map);
		});
	});

	describe('anormalizeItem', () => {
		it('should anormalize a normalItem', () => {
			expect(util.anormalizeItem(normalItem)).to.deep.equal(anormalItem);
		});
	});

	describe('anormalizeList', () => {
		it('should anormalize a normalItem list', () => {
			expect(util.anormalizeList([normalItem])).to.deep.equal([anormalItem]);
		});
	});

	describe('anormalizeType', () => {
		it('should return BOOL for booleans', () => {
			expect(util.anormalizeType(true)).to.equal('BOOL');
		});

		it('should return N for numbers', () => {
			expect(util.anormalizeType(9)).to.equal('N');
		});

		it('should return S for strinsg', () => {
			expect(util.anormalizeType('string')).to.equal('S');
		});

		it('should return NULL for nulls', () => {
			expect(util.anormalizeType(null)).to.equal('NULL');
		});

		it('should return L for arrays', () => {
			expect(util.anormalizeType([])).to.equal('L');
		});

		it('should return M for objects', () => {
			expect(util.anormalizeType({})).to.equal('M');
		});
	});

	describe('normalizeValue', () => {
		it('should convert boolean truthy to BOOL', () => {
			expect(util.normalizeValue(anormalItem.booleanTruthy)).to.deep.equal(normalItem.booleanTruthy);
		});

		it('should convert boolean falsy to BOOL', () => {
			expect(util.normalizeValue(anormalItem.booleanFalsy)).to.deep.equal(normalItem.booleanFalsy);
		});

		it('should convert positive integer to N', () => {
			expect(util.normalizeValue(anormalItem.integer)).to.deep.equal(normalItem.integer);
		});

		it('should convert integer zero to N', () => {
			expect(util.normalizeValue(anormalItem.integerZero)).to.deep.equal(normalItem.integerZero);
		});

		it('should convert negative integer to N', () => {
			expect(util.normalizeValue(anormalItem.negativeInteger)).to.deep.equal(normalItem.negativeInteger);
		});

		it('should convert float to N', () => {
			expect(util.normalizeValue(anormalItem.float)).to.deep.equal(normalItem.float);
		});

		it('should convert negative float to N', () => {
			expect(util.normalizeValue(anormalItem.negativeFloat)).to.deep.equal(normalItem.negativeFloat);
		});

		it('should convert string to S', () => {
			expect(util.normalizeValue(anormalItem.string)).to.deep.equal(normalItem.string);
		});

		it('should convert null to NULL', () => {
			expect(util.normalizeValue(anormalItem.null)).to.deep.equal(normalItem.null);
		});

		it('should convert number array to L', () => {
			expect(util.normalizeValue(anormalItem.numberArray)).to.deep.equal(normalItem.numberArray);
		});

		it('should convert string array to L', () => {
			expect(util.normalizeValue(anormalItem.stringArray)).to.deep.equal(normalItem.stringArray);
		});

		it('should convert combined array to L', () => {
			expect(util.normalizeValue(anormalItem.array)).to.deep.equal(normalItem.array);
		});

		it('should convert map to M', () => {
			expect(util.normalizeValue(anormalItem.map)).to.deep.equal(normalItem.map);
		});

		it('should convert number set into number array', () => {
			expect(util.normalizeValue({
				NS: [1, 2, 3]
			})).to.deep.equal([1, 2, 3]);
		});

		it('should convert string set into string array', () => {
			expect(util.normalizeValue({
				SS: ['a', 'b', 'c']
			})).to.deep.equal(['a', 'b', 'c']);
		});
	});

	describe('normalizeItem', () => {
		it('should normalize an anormalItem', () => {
			expect(util.normalizeItem(anormalItem)).to.deep.equal(normalItem);
		});

		it('should return null if empty', () => {
			expect(util.normalizeItem()).to.be.null;
		});
	});

	describe('normalizeList', () => {
		it('should normalize an anormalItem list', () => {
			expect(util.normalizeList([anormalItem])).to.deep.equal([normalItem]);
		});
	});
});
