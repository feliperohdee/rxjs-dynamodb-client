const _ = require('./lib/lodash');
const Crud = require('./lib/Crud');
const Util = require('./lib/Util');
const Request = require('./lib/Request');
const ExpressionsHelper = require('./lib/ExpressionsHelper');
const {
    Select,
    ReturnValues,
    ConsumedCapacity
} = require('./lib/constants');

class DynamoDB {
    constructor(deps = {}) {
        if (!deps.client) {
            throw new Error('no dynamodb client provided.');
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
            L: _.reduce(data, (reduction, value) => {
                return reduction.concat(this.util.anormalizeValue(value));
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
};