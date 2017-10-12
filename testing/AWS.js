const AWS = require('aws-sdk');

AWS.config.update({
	accessKeyId: process.env.CIRCLECI ? process.env.accessKeyId : 'spec',
	secretAccessKey: process.env.CIRCLECI ? process.env.secretAccessKey : 'spec',
	region: 'us-east-1'
});

const dynamoDbClient = new AWS.DynamoDB(process.env.CIRCLECI ? {} : {
	endpoint: 'http://localhost:9090'
});

module.exports = {
	dynamoDbClient
};
