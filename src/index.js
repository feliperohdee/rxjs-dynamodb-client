import _ from 'lodash';
import {
	Observable,
	Subject
} from 'rxjs';

import {
	Util
} from './Util';
import {
	Request
} from './Request';
import {
	ExpressionsHelper
} from './ExpressionsHelper';
import {
	Select,
	ReturnValues,
	ConsumedCapacity
} from './constants';

export class DynamoDB {
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

export {
	ExpressionsHelper,
	Request,
	Util,
	Select,
	ReturnValues,
	ConsumedCapacity
}
