const _ = require('lodash');
const cuid = require('cuid');

module.exports = class ExpressionsHelper {
    constructor(request) {
        this.request = request;
    }

    getTokens(value) {
        return value.match(/([a-zA-Z-_]\d*)+/g);
    }

    attrNotExists(path) {
        return this.attrExists(path, true);
    }

    attrExists(path, not = false) {
        path = _.reduce(this.getTokens(path), (reduction, match) => {
            this.request.addPlaceholderName(match);

            return reduction = reduction.replace(match, `#${match}`);
        }, path);

        return not ? `attribute_not_exists(${path})` : `attribute_exists(${path})`;
    }

    prependList(path, value) {
        return this.appendList(path, value, true);
    }

    appendList(path, value, prepend = false) {
        const placeholder = `appendList_${cuid.slug()}`;

        path = _.reduce(this.getTokens(path), (reduction, match) => {
            this.request.addPlaceholderName(match);

            return reduction = reduction.replace(match, `#${match}`);
        }, path);

        this.request.addPlaceholderValue({
                [placeholder]: _.isArray(value) ? value : [value]
            })
            .addPlaceholderValue({
                emptyList: []
            });

        return prepend ?
            `${path} = list_append(:${placeholder}, if_not_exists(${path}, :emptyList))` :
            `${path} = list_append(if_not_exists(${path}, :emptyList), :${placeholder})`;
    }

    ifNotExists(path, value) {
        const placeholder = `ifNotExists_${cuid.slug()}`;

        path = _.reduce(this.getTokens(path), (reduction, match) => {
            this.request.addPlaceholderName(match);

            return reduction = reduction.replace(match, `#${match}`);
        }, path);

        this.request.addPlaceholderValue({
            [placeholder]: value
        });

        return `${path} = if_not_exists(${path}, :${placeholder})`;
    }

    contains(attribute, values = [], condition = 'OR') {
        if (!_.isArray(values)) {
            values = [values];
        }

        if (_.isEmpty(values)) {
            return;
        }

        const placeholders = _.reduce(values, (reduction, value, index) => {
            reduction[`cFilter_${cuid.slug()}`] = value;

            return reduction;
        }, {});

        const expression = _.map(placeholders, (value, key) => {
            return `contains(#${attribute}, :${key})`;
        }, '').join(` ${condition} `);

        this.request.addPlaceholderName(attribute);
        this.request.addPlaceholderValue(placeholders);

        return `(${expression})`;
    }

    between(attribute, min, max) {
        this.request.addPlaceholderName(attribute);

        if (min) {
            this.request.addPlaceholderValue({
                min
            });
        }

        if (max) {
            this.request.addPlaceholderValue({
                max
            });
        }

        if (min && max) {
            return `#${attribute} BETWEEN :min AND :max`;
        } else if (min) {
            return `#${attribute} >= :min`;
        } else {
            return `#${attribute} <= :max`;
        }
    }

    update(attributes, timestamp = true) {
        const updateAttributes = _.omit(attributes, [
            this.request.partitionAttr,
            this.request.sortAttr,
            'createdAt',
            'updatedAt'
        ]);

        return _.reduce(updateAttributes, (reduction, value, key) => {
            const condition = value && value.condition;

            if (condition) {
                value = value.value;
            }

            if (_.isUndefined(value)) {
                return reduction;
            }

            this.request
                .addPlaceholderName(key)
                .addPlaceholderValue({
                    [key]: value
                });

            if (condition === 'ifNotExists') {
                reduction.unshift(`#${key} = if_not_exists(#${key}, :${key})`);
            } else {
                reduction.unshift(`#${key} = :${key}`);
            }

            return reduction;
        }, timestamp ? [
            this.timestamp()
        ] : []).join(', ')
    }

    timestamp() {
        this.request.addPlaceholderName(['createdAt', 'updatedAt'])
            .addPlaceholderValue({
                now: _.now()
            });

        return '#createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now';
    }
}