import AWS from 'aws-sdk';

export default function() {
	AWS.config.update({
		accessKeyId: process.env.CIRCLECI ? 'AKIAIEMZIXLBHL4JQQVQ' : 'spec',
		secretAccessKey: process.env.CIRCLECI ? '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' : 'spec',
		region: 'us-east-1'
	});

	const dynamoDbClient = new AWS.DynamoDB(process.env.CIRCLECI ? {} : {
		endpoint: 'http://localhost:9090'
	});

	return {
		dynamoDbClient
	}
}
