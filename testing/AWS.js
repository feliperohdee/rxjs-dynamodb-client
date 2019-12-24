const AWS = require('aws-sdk');
const production = process.env.NODE_ENV === 'production';

AWS.config.update({
	accessKeyId: production ? process.env.accessKeyId : 'spec',
	secretAccessKey: production ? process.env.secretAccessKey : 'spec',
	region: 'us-east-1'
});

const dynamoDbClient = new AWS.DynamoDB(production ? {} : {
	endpoint: 'http://localhost:9090'
});

module.exports = {
	dynamoDbClient
};
