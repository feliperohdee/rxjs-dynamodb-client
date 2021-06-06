const rx = require('./rx');

const {
	DynamoDB
} = require('../');
const {
	dynamoDbClient
} = require('./AWS');

const dynamodb = new DynamoDB({
	client: dynamoDbClient
});

module.exports = {
	dynamodb,
	rx
};
