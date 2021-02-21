const {
	DynamoDB
} = require('../');

const {
	dynamoDbClient: client
} = require('./AWS');

const dynamodb = new DynamoDB({
	client
});

module.exports = {
	dynamodb
};
