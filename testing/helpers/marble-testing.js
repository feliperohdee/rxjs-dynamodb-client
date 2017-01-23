'use strict';

const _ = require('lodash');
const Rx = require('rxjs');
const sinon = require('sinon');

const Observable = Rx.Observable;

module.exports = {
    hot,
    cold,
    expectObservable,
    expectSubscriptions,
    time,
    spyObservable
}

function hot(marbles, values, error) {
    if (!global.rxTestScheduler) {
        throw 'tried to use hot() in async test';
    }

    return global.rxTestScheduler.createHotObservable.apply(global.rxTestScheduler, arguments);
}

function cold(marbles, values, error) {
    if (!global.rxTestScheduler) {
        throw 'tried to use cold() in async test';
    }

    return global.rxTestScheduler.createColdObservable.apply(global.rxTestScheduler, arguments);
}

function expectObservable(observableunsubscriptionMarbles) {
    if (_.isUndefined(observableunsubscriptionMarbles)) {
        observableunsubscriptionMarbles = null;
    }

    if (!global.rxTestScheduler) {
        throw 'tried to use expectObservable() in async test';
    }

    return global.rxTestScheduler.expectObservable.apply(global.rxTestScheduler, arguments);
}

function expectSubscriptions(actualSubscriptionLogs) {
    if (!global.rxTestScheduler) {
        throw 'tried to use expectSubscriptions() in async test';
    }

    return global.rxTestScheduler.expectSubscriptions.apply(global.rxTestScheduler, arguments);
}

function time(marbles) {
    if (!global.rxTestScheduler) {
        throw 'tried to use time() in async test';
    }

    return global.rxTestScheduler.createTime.apply(global.rxTestScheduler, arguments);
}

function spyObservable(base, name, ...customArgs) {
    let original = _.get(base, name);
    let testScheduler = this.testScheduler;

    return sinon.stub(base, name).callsFake(function(...args) {
        if (_.size(customArgs)) {
            args = customArgs;
        }

        args.push(testScheduler);

        return original.apply(this, args);
    });
}
