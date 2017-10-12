const _ = require('lodash');
const {
	Observable
} = require('rxjs');

const Crud = require('./lib/Crud');
const Util = require('./lib/Util');
const Request = require('./lib/Request');
const ExpressionsHelper = require('./lib/ExpressionsHelper');
const {
	Select,
	ReturnValues,
	ConsumedCapacity
} = require('./lib/constants');

Observable.prototype.onRetryableError = function(callback = {}) {
	const source = this;

	return source.retryWhen(err => err.mergeMap((err, index) => {
		let error = _.isFunction(callback) ? callback(err, index) : callback;

		if(_.isNumber(error)) {
			error = {
				max: error
			};
		}

		error = _.defaults({}, error, {
			retryable: !_.isUndefined(err.retryable) ? err.retryable : false,
			delay: !_.isUndefined(err.retryDelay) ? err.retryDelay : 1000,
			max: 1
		});

		if (error && error.retryable && index < error.max) {
			return Observable.of(err)
				.delay(error.delay);
		}

		return Observable.throw(err);
	}));
};

class DynamoDB {
	constructor(deps = {}) {
		if (!deps.client) {
			throw new Error('no dynamoDb client provided.');
		}

		this.client = deps.client;
	}

	get request() {
		return new Request(this.client);
	}

	get util() {
		return new Util();
	}

	table(name, schema) {
		return this.request.table(name, schema);
	}

	call(method, args) {
		return this.request.routeCall(method, args);
	}

	S(data) {
		return this.util.raw({
			S: data
		});
	}

	N(data) {
		if (_.isNumber(data)) {
			data = data.toString();
		}

		return this.util.raw({
			N: data
		});
	}

	SS(data) {
		return this.util.raw({
			SS: data
		});
	}

	NS(data) {
		return this.util.raw({
			NS: _.map(data, n => n.toString())
		});
	}

	L(data) {
		return this.util.raw({
			L: _.reduce(data, (result, value) => {
				result.push(this.util.anormalizeValue(value));

				return result;
			}, [])
		});
	}
}

module.exports = {
	ConsumedCapacity,
	Crud,
	DynamoDB,
	ExpressionsHelper,
	Request,
	ReturnValues,
	Select,
	Util
}
