import {
	DynamoDB
} from 'src';

import AWS from 'testingEnv/AWS';

const {
	dynamoDbClient
} = AWS();

const instance = new DynamoDB({
	client: dynamoDbClient
});

export {
	instance
};
