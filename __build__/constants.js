'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});
const Select = exports.Select = {
	ALL_ATTRIBUTES: 0,
	ALL_PROJECTED_ATTRIBUTES: 1,
	SPECIFIC_ATTRIBUTES: 2,
	COUNT: 3
};

const ReturnValues = exports.ReturnValues = {
	NONE: 'NONE',
	ALL_OLD: 'ALL_OLD',
	UPDATED_OLD: 'UPDATED_OLD',
	ALL_NEW: 'ALL_NEW',
	UPDATED_NEW: 'UPDATED_NEW'
};

const ConsumedCapacity = exports.ConsumedCapacity = {
	NONE: 'NONE',
	TOTAL: 'TOTAL',
	INDEXES: 'INDEXES'
};