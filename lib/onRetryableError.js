const _ = require('lodash');

const rx = require('./rx');

module.exports = (callback = {}) => {
    return source => {
        return source.pipe(
            rx.retryWhen(err => {
                return err.pipe(
                    rx.mergeMap((err, index) => {
                        let error = _.isFunction(callback) ? callback(err, index) : callback;
        
                        if (_.isNumber(error)) {
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
                            return rx.of(err)
                                .pipe(
                                    rx.delay(error.delay)
                                );
                        }
        
                        return rx.throwError(err);
                    })
                );
            })
        );
    };
};