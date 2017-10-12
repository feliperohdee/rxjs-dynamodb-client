const {
	DynamoDB
} = require('../');

const {
	dynamoDbClient: client
} = require('./AWS');

const dynamoDb = new DynamoDB({
	client
});

module.exports = {
	dynamoDb
};
