'use strict';

const _ = require('lodash');
const chai = require('chai');

module.exports = observableMatcher;

function stringify(x) {
    return JSON.stringify(x, (key, value) => {
            if (_.isArray(value)) {
                return `[${value.map(i => `\n\t${stringify(i)}`)}\n]`;
            }

            return value;
        })
        .replace(/\\"/g, '"')
        .replace(/\\t/g, '\t')
        .replace(/\\n/g, '\n');
}

function deleteErrorNotificationStack(marble) {
    const {
        notification
    } = marble;
    if (notification) {

        const {
            kind,
            error
        } = notification;

        if (kind === 'E' && error instanceof Error) {
            notification.error = {
                name: error.name,
                message: error.message
            };
        }
    }

    return marble;
}

function observableMatcher(actual, expected) {
    if (_.isArray(actual) && _.isArray(expected)) {
        actual = actual.map(deleteErrorNotificationStack);
        expected = expected.map(deleteErrorNotificationStack);

        const passed = _.isEqual(actual, expected);

        if (passed) {
            return;
        }

        let message = '\nExpected \n';
        actual.forEach((x) => message += `\t${stringify(x)}\n`);

        message += '\t\nto deep equal \n';
        expected.forEach((x) => message += `\t${stringify(x)}\n`);

        chai.assert(passed, message);
    } else {
        chai.assert.deepEqual(actual, expected);
    }
}
