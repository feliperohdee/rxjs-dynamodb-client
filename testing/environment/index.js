import {
	DynamoDB
} from 'src';

import AWS from 'testingEnv/AWS';

const {
	dynamoDbClient
} = AWS();

const dynamoDb = new DynamoDB({
	client: dynamoDbClient
});

export {
	dynamoDb
};
