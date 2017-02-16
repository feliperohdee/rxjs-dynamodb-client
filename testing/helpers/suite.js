'use strict';

const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const Rx = require('rxjs');
const sinonChai = require('sinon-chai');
const Mocha = require('mocha');
const Suite = require('mocha/lib/suite');
const Test = require('mocha/lib/test');
const escapeRe = require('escape-string-regexp');
const commonInterface = require('mocha/lib/interfaces/common');
const marble = require('./marble-testing');
const observableMatcher = require('./observableMatcher');

const TestScheduler = Rx.TestScheduler;

chai.use(sinonChai);

module.exports = Mocha.interfaces['ui'] = suite => {
	const suites = [suite];

	suite.on('pre-require', (context, file, mocha) => {
		const common = commonInterface(suites, context);

		context.expect = chai.expect;
		context.spy = sinon.spy;
		context.stub = sinon.stub;
		context.match = sinon.match;

		context.before = common.before;
		context.after = common.after;
		context.beforeEach = common.beforeEach;
		context.afterEach = common.afterEach;
		context.run = mocha.options.delay && common.runWithSuite(suite);

		//setting up per-context test scheduler
		context.rxTestScheduler = null;

		//setting up assertion, helper for marble testing
		context.hot = marble.hot;
		context.cold = marble.cold;
		context.expectObservable = marble.expectObservable;
		context.expectSubscriptions = marble.expectSubscriptions;
		context.time = marble.time;
		context.spyObservable = marble.spyObservable;

		/**
		 * Describe a "suite" with the given `title`
		 * and callback `fn` containing nested suites
		 * and/or tests.
		 */
		context.describe = (title, fn) => {
			const suite = Suite.create(suites[0], title);
			suite.file = file;
			suites.unshift(suite);
			fn.call(suite);
			suites.shift();

			return suite;
		};

		/**
		 * Pending describe.
		 */

		context.describe.skip = (title, fn) => {
			const suite = Suite.create(suites[0], title);

			suite.pending = true;
			suites.unshift(suite);
			fn.call(suite);
			suites.shift();
		};

		/**
		 * Exclusive suite.
		 */

		context.describe.only = (title, fn) => {
			const suite = context.describe(title, fn);
			mocha.grep(suite.fullTitle());

			return suite;
		};

		/**
		 * Describe a test case to test type definition
		 * sanity on build time. Recommended only for
		 * exceptional type definition won't be used in test cases.
		 */

		context.type = (title, fn) => {
			//intentionally does not execute to avoid unexpected side effect occurs by subscription,
			//or infinite source. Suffecient to check build time only.
		};

		/**
		 * Describe a specification or test-case
		 * with the given `title` and callback `fn`
		 * acting as a thunk.
		 */
		context.it = (title, fn) => {
			context.rxTestScheduler = null;

			let modified = fn;

			if (_.startsWith(title, '(withObservable)') && fn && fn.length === 0) {
				modified = done => {
					context.rxTestScheduler = new TestScheduler(observableMatcher);

					let error = null;

					try {
						fn();
						context.rxTestScheduler.flush();
					} catch (e) {
						error = e instanceof Error ? e : new Error(e);
					} finally {
						context.rxTestScheduler = null;
						error ? done(error) : done();
					}
				};
			}

			const suite = suites[0];

			if (suite.pending) {
				modified = null;
			}

			const test = new Test(title, modified);

			test.file = file;
			suite.addTest(test);

			return test;
		};

		/**
		 * Exclusive test-case.
		 */

		context.it.only = (title, fn) => {
			const test = context.it(title, fn);
			const reString = `^${escapeRe(test.fullTitle())}$`;
			mocha.grep(new RegExp(reString));

			return test;
		};

		/**
		 * Pending test case.
		 */

		context.it.skip = title => {
			context.it(title);
		};

		/**
		 * Number of attempts to retry.
		 */
		context.it.retries = n => {
			context.retries(n);
		};
	});
}

//overrides JSON.toStringfy to serialize error object
Object.defineProperty(Error.prototype, 'toJSON', {
	value() {
		const alt = {};

		Object.getOwnPropertyNames(this).forEach(key => {
			if (key !== 'stack') {
				alt[key] = this[key];
			}
		}, this);
		return alt;
	},
	configurable: true
});
