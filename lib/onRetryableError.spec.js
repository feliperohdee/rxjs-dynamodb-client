const chai = require('chai');
const rx = require('rxjs');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');

const onRetryableError = require('./onRetryableError');
const testing = require('../testing');

chai.use(sinonChai);

const expect = chai.expect;

describe('lib/onRetryableError.js', () => {
    let fn;
    let callback;
    let err;

    beforeEach(() => {
        callback = sinon.stub();
        err = new Error();
        err.code = 'LimitExceededException';
        err.statusCode = 400;
        err.retryable = true;
        fn = () => {
            let index = 0;

            return new rx.Observable(subscriber => {
                err.retryDelay = index++;
                callback();

                subscriber.error(err);
            });
        };
    });

    it('should retry 1x by default', done => {
        fn()
            .pipe(
                onRetryableError()
            )
            .subscribe(null, testing.rx(err => {
                expect(callback).to.have.been.callCount(2);
                expect(err.retryDelay).to.equal(1);
            }, null, done));
    });

    it('should retry 2x', done => {
        fn()
            .pipe(
                onRetryableError(() => {
                    return 2;
                })
            )
            .subscribe(null, testing.rx(err => {
                expect(callback).to.have.been.callCount(3);
                expect(err.retryDelay).to.equal(2);
            }, null, done));
    });

    it('should retry 3x', done => {
        fn()
            .pipe(
                onRetryableError(3)
            )
            .subscribe(null, testing.rx(err => {
                expect(callback).to.have.been.callCount(4);
                expect(err.retryDelay).to.equal(3);
            }, null, done));
    });

    it('should retry 3x by 50ms (150ms total)', done => {
        fn()
            .pipe(
                onRetryableError({
                    max: 3,
                    delay: 50
                })
            )
            .subscribe(null, testing.rx(err => {
                expect(callback).to.have.been.callCount(4);
                expect(err.retryDelay).to.equal(3);
            }, null, done));
    });

    it('should retry 4x', done => {
        fn()
            .pipe(
                onRetryableError((err, index) => ({
                    max: 10,
                    retryable: index >= 4 ? false : err.retryable
                }))
            )
            .subscribe(null, testing.rx(err => {
                expect(callback).to.have.been.callCount(5);
                expect(err.retryDelay).to.equal(4);
            }, null, done));
    });

    describe('not retryable', () => {
        beforeEach(() => {
            err.retryable = false;
        });

        it('should not retry', done => {
            fn()
                .pipe(
                    onRetryableError()
                )
                .subscribe(null, testing.rx(err => {
                    expect(callback).to.have.been.callCount(1);
                    expect(err.retryDelay).to.equal(0);
                }, null, done));
        });
    });
});