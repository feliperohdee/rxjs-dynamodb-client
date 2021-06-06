const _ = require('lodash');

module.exports = (next, error, complete) => {
    return response => {
        try {
            if (_.isFunction(next)) {
                next(response);
            }
            
            if (_.isFunction(complete)) {
                complete();
            }
        } catch (err) {
            if (_.isFunction(error)) {
                return error(err);
            }

            throw err;
        }
    };
};