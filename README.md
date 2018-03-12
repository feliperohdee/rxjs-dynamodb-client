[![CircleCI](https://circleci.com/gh/feliperohdee/smallorange-dynamodb-client.svg?style=svg)](https://circleci.com/gh/feliperohdee/smallorange-dynamodb-client)

# DynamoDb client built with lodash and rxjs!

Hi, this client is built on top of RxJS Observables (a reactive programming library http://reactivex.io/rxjs/), so, would be good to have a small portion of knowledege about that to enjoy all the features which this lib provide, specially the operators. I'will explain a bit when needed.

To create an instance, is simple, just pass a dynamodb client to lib's constructor:

		import AWS from 'aws-sdk';
		import {
			DynamoDB
		} from 'smallorange-dynamodb-client';

		AWS.config.update({
			accessKeyId: 'yourAccessKeyId',
			secretAccessKey: 'yourSecretAccessKey',
			region: 'us-east-1'
		});

		const dynamoDb = new DynamoDB({
			client: new AWS.DynamoDB()
		});

## Methods
	
**.table(tableName: string, tableSchema: object)**
* Starts an operation chain
* Returns a request instance which allow chain more operations

		dynamoDb.table(tableName: string, tableSchema: {
			primaryKeys: {
				partition: string [required];
				sort: string [optional];
			}
			indexes: {
				someLocalIndexName: {
					partition: string [required];
					sort: string [required];
				},
				someGlobalIndexName: {
					partition: string [required];
					sort: string [required];
				}
			}
		});

**.addPlaceholderName(value: string | Array<string> | object)**
**.addPlaceholderValue(value: string | Array<string> | object)**
* Chain to create placeholders to be used in conjunction of expressions
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.addPlaceholderName({
				a: 'field1'	
			})
			.addPlaceholderValue({
				b: 'value'	
			})
			.filter(`begins_with(#a, :b)`) // any dynamodb expression http://docs.aws.amazon.com/amazondynamodb/latest/
			.query({...});

		dynamoDb.table({...})
			.addPlaceholderName('field1')
			.addPlaceholderValue('value')
			.filter(`begins_with(#field1, :value)`) // any dynamodb expression http://docs.aws.amazon.com/amazondynamodb/latest/
			.query({...});

		dynamoDb.table({...})
			.addPlaceholderName(['field1', 'field2'])
			.addPlaceholderValue(['value1', 'value2'])
			.filter(`begins_with(#field1, :field1) OR #field2 = :value2`) // any dynamodb expression http://docs.aws.amazon.com/amazondynamodb/latest/
			.query({...});

**.describe()**
* Describe existent table
* Returns an observable carrying the data about table if exists, throws if not, which can be used to create a table if not exists, se sample below.

		dynamoDb.table({...})
			.describe({...})
			.catch(() => createTable)
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.consumedCapacity(value: string)**
* Chain to inform request to returns consumedCapacity in their results
* Returns a request instance which allow chain more operations

		import {
			ConsumedCapacity
		} from 'smallorange-dynamodb-client';

		dynamoDb.table({...})
			.consumedCapacity(ConsumedCapacity.NONE | ConsumedCapacity.TOTAL | ConsumedCapacity.INDEXES)
			.query({...});

**.select(values: any)**
* Chain to inform requests what to returns when querying or getting an entity
* Returns a request instance which allow chain more operations

		import {
			Select
		} from 'smallorange-dynamodb-client';

		dynamoDb.table({...})
			.select('name, age, ...')
			.query({...});

		dynamoDb.table({...})
			.select(Select.ALL_ATTRIBUTES | Select.ALL_PROJECTED_ATTRIBUTES | Select.SPECIFIC_ATTRIBUTES | Select.COUNT)
			.query({...});

**.consistent()**
* Chain to inform requests to perform a consistent read
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.consistent()
			.query({...});

**.query(data: object | string)**
* Query table using primaryKeys or Indexes, or combined.
* Returns an observable carrying the data, which can be used with any RxJS operators, like, map, filter, reduce, debounceTime, ...

		dynamoDb.table({...})
			.index(...)
			.select(...)
			.limit(10)
			.query({
				[partitionAttr]: string;
				[sortAttr]: string;
				|
				[partitionAttr]: string;
				[localIndexAttr]: string;
				|
				[globalIndexartitionAttr]: string;
				[globalIndexortAttr]: string;
			})
			.filter(filterFn) // rxjs sample operators
			.map(mapFn) // rxjs sample operators
			.toArray() // Other way it will emit values by streaming, good if you are using real time responses, like webSocket [=.
			.subscribe(nextFn, errFn, completeFn);

		const request = dynamoDb.table({...});

		request
			.select(...)
			.limit(10)
			.addPlaceholderName({
				partition: 'partitionAttr',
				sort: 'sortAttr'
			})
			.addPlaceholderValue({
				partition: 'partitionValue',	
				sort: 'sortValue'	
			})
			.query(`#partition = :partition AND begins_with(#sort, :sort)`)
			.toArray() // Other way it will emit values by streaming, good if you are using real time responses, like webSocket [=.
			.map(response => {
				return {
					response,
					stats: request.queryStats // at the end, you can get queryStats which gives you before, after, count, scannedCount and iteractions
				}
			})
			.subscribe(nextFn, errFn, completeFn);

			// this response will be
			//
			// {
			//		response: Array<item>,
			//		stats: {
			//			before: object;
			//			after: object;
			//			count: number;
			//			scannedCount: number;
			//			iteractions: number;
			//		}
			// }

Note: DynamoDb just fetch max 1MB, this lib handles this and perform many requests as needed to fetch all data. So, always look for use `.limit(value: number)` when querying.

**.get(data: object)**
* Get a value table using primaryKeys or Indexes, or combined.
* Returns an observable carrying the data, which can be used with any RxJS operators, like, map, filter, reduce, debounceTime, ...

		dynamoDb.table({...})
			.select(...)
			.get({
				[partitionAttr]: string; // required
				[sortAttr]: string;	 // required
			})
			.filter(filterFn) // rxjs sample operators
			.map(mapFn) // rxjs sample operators
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.index(indexName: string)**
* Chain to inform requests that next request will be indexed by a previously created index;
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.index('someLocalIndexName')
			.query({...})
			.subscribe(nextFn, errFn, completeFn);

**.desc()**
* Chain to inform requests that next request will be made in descendent order.
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.desc()
			.query({...})
			.subscribe(nextFn, errFn, completeFn);

**.limit()**
* Chain to inform requests that next request will have a limit.
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.limit(100)
			.query({...})
			.subscribe(nextFn, errFn, completeFn);

**.filter(expression: string, append: boolean = false, condition = 'AND')**
* Chain to create a filterExpression, or append one, it shoud be used with `.addPlaceholderName()` and `.addPlaceholderValue()`.
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.addPlaceholderName({
				field1: 'field1'	
			})
			.addPlaceholderValue({
				field1: 'field1 value'	
			})
			.filter(`begins_with(#field1, :field1)`) // any dynamodb expression http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#Expressions.SpecifyingConditions.ConditionExpressions
			.query({
				[partitionAttr]: string; // required
				[sortAttr]: string; // required
				...
			})
			.subscribe(nextFn, errFn, completeFn);

			// you can append a filter, and sepecify the condition

			dynamoDb.table({...})
				.addPlaceholderName({
					field1: 'field1'	
				})
				.addPlaceholderValue({
					field1: 'field1 value'	
				})
				.filter(`begins_with(#field1, :field1)`)
				.addPlaceholderName('field1') // placeholder can be overlapped without problems, the lib omit duplicates
				.addPlaceholderValue('value2')
				.filter(`begins_with(#field2, :value2)`, true, 'OR');

			// the final expression will be:
			// begins_with(#field1, :field1) OR begins_with(#field2, :value2)

**.resume(keys: object)**
* Chain to inform requests that next request be resumed according to keys' arguments.
* Returns a request instance which allow chain more operations

		dynamoDb.table({...})
			.resume({
				[partitionAttr]: string;
				[sortAttr]: string;
				|
				[partitionAttr]: string;
				[sortAttr]: string;
				[localIndexAttr]: string;
				|
				[globalIndexartitionAttr]: string;
				[globalIndexortAttr]: string;	
			})
			.query({...})
			.subscribe(nextFn, errFn, completeFn);

**.insert(data: object)**
* Insert, how the name says, just inserts, not updates nor replaces an entity.
* Returns an observable carrying the data just added plus createdAt and updatedAt attributes with the same value, which can be used with any RxJS operators, like, map, filter, reduce, debounceTime, ...

		dynamoDb.table({...})
			.insert({...})
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.insertOrReplace(data: object)**
* Insert or replace, how the name says, just inserts or replaces, not update an entity.
* Returns an observable carrying the data just added plus createdAt and updatedAt attributes with the same value, which can be used with any RxJS operators, like, map, filter, reduce, debounceTime, ...

		dynamoDb.table({...})
			.insertOrReplace({...})
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.insertOrUpdate(data: object)**
* Insert or replace, how the name says, just inserts or updates, not replaces an entity.
* Returns an observable carrying the data just added plus createdAt and updatedAt attributes with the same value if inserted, and updatedAt changed if it was updated, which can be used with any RxJS operators, like, map, filter, reduce, debounceTime, ...

		dynamoDb.table({...})
			.insertOrUpdate({...})
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.update(data: object)**
* Just updated a previously inserted value, not inserts nor replaces.
* Returns an observable carrying the data defined by `.return()`.
		
		import {
			ReturnValues
		} from 'smallorange-dynamodb-client';

		dynamoDb.table({...})
			.return(ReturnValues.NONE | ReturnValues.ALL_OLD | ReturnValues.UPDATED_OLD | ReturnValues.ALL_NEW | ReturnValues.UPDATED_NEW)
			.update({
				[partitionAttr]: string; // required
				[sortAttr]: string; // required
				...
			})
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.delete(data: object)**
* Just delete a previously inserted value.
* Returns an observable carrying the data defined by `.return()`.
		
		import {
			ReturnValues
		} from 'smallorange-dynamodb-client';

		dynamoDb.table({...})
			.return(ReturnValues.NONE | ReturnValues.ALL_OLD)
			.delete({
				[partitionAttr]: string; // required
				[sortAttr]: string; // required
			})
			.subscribe(nextFn, errFn, completeFn);

Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.batchGet(data: Array<object>)**
* Returns an observable carrying a stream of gotten values

		dynamoDb.table({...})
			.batchGet([{
				[partitionAttr]: string; // required
				[sortAttr]: string; // required
			}])
			.filter(filterFn) // rxjs sample operators
			.map(mapFn) // rxjs sample operators
			.toArray() // Other way it will emit values by streaming, good if you are using real time responses, like webSocket [=.
			.subscribe(nextFn, errFn, completeFn);

Note: Dynamo gets just max 100 entities, but this lib handle this and perform many requests as needed to get all data.
Note: If you are using Promises, you can easily tranform Observables into Promises calling .toPromise() instead of .subscribe(), but we really advice you to learn RxJS, its amazing powerful.

**.batchWrite(toDelete: Array<object> | null, toInsert: Array<object> | null)**
* Returns an observable carrying an array of unprocessed items

		dynamoDb.table({...})
			.batchWrite(toDelete => [{
				[partitionAttr]: string; // required
				[sortAttr]: string; // required
			}], toInsert => [{
				[partitionAttr]: string; // required
				[sortAttr]: string; // required,
				...
			}])
			.subscribe(nextFn, errFn, completeFn);

Note: Dynamo handles 25 write operations max, but this lib handle this and perform many requests as needed to perform all operations.

# Low level requests

If you need to perform low level requests, like create a table, you can do it using `.routeCall(opertaionName: string, args: object)`. This sample creates a table if it not exists, and insert 10 items. We are using RxJS operators, if you have doubts about that, you can learn at http://reactivex.io/rxjs/.

			request.describe() // verify if table exists
				.catch(() => request.routeCall('createTable', { //if not, create a table
					TableName: 'tblSpec',
					ProvisionedThroughput: {
						ReadCapacityUnits: 1,
						WriteCapacityUnits: 1
					},
					AttributeDefinitions: [{
						AttributeName: 'namespace',
						AttributeType: 'S'
					}, {
						AttributeName: 'key',
						AttributeType: 'S'
					}, {
						AttributeName: 'localIndexedAttr',
						AttributeType: 'S'
					}, {
						AttributeName: 'globalIndexedPartitionAttr',
						AttributeType: 'S'
					}, {
						AttributeName: 'globalIndexedSortAttr',
						AttributeType: 'S'
					}],
					KeySchema: [{
						AttributeName: 'namespace',
						KeyType: 'HASH'
					}, {
						AttributeName: 'key',
						KeyType: 'RANGE'
					}],
					LocalSecondaryIndexes: [{
						IndexName: 'localIndexedSpec',
						KeySchema: [{
							AttributeName: 'namespace',
							KeyType: 'HASH'
						}, {
							AttributeName: 'localIndexedAttr',
							KeyType: 'RANGE'
						}],
						Projection: {
							ProjectionType: 'ALL'
						}
					}],
					GlobalSecondaryIndexes: [{
						IndexName: 'globalIndexedSpec',
						KeySchema: [{
							AttributeName: 'globalIndexedPartitionAttr',
							KeyType: 'HASH'
						}, {
							AttributeName: 'globalIndexedSortAttr',
							KeyType: 'RANGE'
						}],
						Projection: {
							ProjectionType: 'ALL'
						},
						ProvisionedThroughput: {
							ReadCapacityUnits: 1,
							WriteCapacityUnits: 1
						}
					}]
				}))
				.mergeMap(() => Observable.range(0, 10) // insert 10 items to the table
					.mergeMap(n => request.insert({
						namespace,
						key: `key-${n}`,
						message: `message-${n}`,
						localIndexedAttr: `local-indexed-${n}`,
						globalIndexedPartitionAttr: `global-indexed-${namespace}`,
						globalIndexedSortAttr: `global-indexed-${n}`,
					}, true)))
				.subscribe(null, null, done);

## CRUD

This lib follows with a Crud class helper, at this way you can extend your models with Crud. In order to start with it:

		import AWS from 'aws-sdk';
		import {
			Crud,
			DynamoDB
		} from 'smallorange-dynamodb-client';

		AWS.config.update({
			accessKeyId: 'yourAccessKeyId',
			secretAccessKey: 'yourSecretAccessKey',
			region: 'us-east-1'
		});
		
		// instance the lib
		const dynamoDb = new DynamoDB({
			client: new AWS.DynamoDB()
		});

		const crud = new Crud(tableName: string, schema: object, {
			dynamoDb	
		});

## Crud operations

		fetch(
			args: {
				[partition | globalIndexPartition]: string (required);
				[sort | localIndexAttribute | globalIndexSort]: string;
				after: base64<string>;
				before: base64<string>;
				consistent: boolean;
				desc: boolean;
				indexName: string;
				limit: number;
				prefix: boolean = true;
				resume: base64<string>;
				select: string (comma separated);
			}, 
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number) : Array<args>,
			dataSelector: function(Observable<object>): Observable<object>,
			customReducer: function(Observable<object>): Observable<any>,
			) : Observable<{
				data: Array<object>, 
				stats: {
					count: number, 
					scannedCount: number, 
					iterations: number, 
					before: base64<string>,
					after: base64<string>
				}}>

		get(
			args: {
				[partition]: string (required);
				[sort]: string;
				consistent: boolean;
				select: string (comma separated);
			}, 
			hook: function(
				request: Request, 
				partition: string, 
				sort: string | number) : Array<args>) : Observable<object>

		multiGet(
			args: {
				items: Array<{
					[partition]: string (required);
					[sort]: string;
				}>
				select: string (comma separated);
			}, 
			hook: function(
				request: Request, 
				items: Array<object>) : Observable<object>

		insert(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: any;
			}, 
			hook: function(
				request: Request, 
				partition: string, 
				sort: string | number, 
				...args: any) : Array<args>) : Observable<object>

		insertOrReplace(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: any;
			}, 
			hook: function(
				request: Request, 
				partition: string, 
				sort: string | number, 
				...args: any) : Array<args>) : Observable<object>

		insertOrUpdate(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: any;
			},
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				partition: string, 
				sort: string | number, 
				...args: any) : Array<args>) : Observable<object>

		update(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: any;
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				partition: string, 
				sort: string | number, 
				...args: any) : Array<args>) : Observable<object>

		updatePrimaryKeys(
			args: {
				[partition]: string (required);
				[sort]: string (required);
			},
			primaryKeys: {
				[partition]: string (optional);
				[sort]: string (optional);
			}
		): Observable<object>

		delete(
			args: {
				[partition]: string (required);
				[sort]: string (required);
			}, 
			hook: function(
				request: Request, 
				partition: string, 
				sort: string | number) : Array<args>) : Observable<object>

		prependToList(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: Array<any>
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, 
				attributes: Array<any>) : Array<args>) : Observable<object>

		appendToList(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: Array<any>
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, 
				attributes: Array<any>) : Array<args>) : Observable<object>

		removeFromList(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: number | Array<number>
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, 
				attributes: Array<number>) : Array<args>) : Observable<object>

		updateAtList(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: {[index: number]: value: any}
			} 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, 
				attributes: Array<{[index: number]: value: any}>) : Array<args>) : Observable<object>

		addToSet(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: Array<string> | string | Array<number> | number
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, 
				attributes: Array<string> | string | Array<number> | number) : Array<args>) : Observable<object>

		removeFromSet(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: Array<string> | string | Array<number> | number
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, 
				attributes: Array<string> | string | Array<number> | number) : Array<args>) : Observable<object>

		removeAttributes(
			args: {
				[partition]: string (required);
				[sort]: string (required);
				...args: any
			}, 
			returns: string = 'ALL_NEW',
			hook: function(
				request: Request, 
				expression: string, 
				partition: string, 
				sort: string | number, attributes: any) : Array<args>) : Observable<object>

		multiGet(
			items: Array<{
				[partition]: string (required);
				[sort]: string | number (required);
			}>
		) : Observable<Array<object>>

		clear(
			args: {
				[partition]: string (required);
				[sort]: string;
			}) : Observable<object>

## Helpers
	
	DynamoDB has two main types of http errors: retryable and non-retryable. The most common type of retryable error is a throughput exception. To handle this, you can chain an Observable helper to handle them:
		
		// most simple way, if err.retryable, will retry once, you can configure this behavior like samples below:
		request.insert()
			.onRetryableError();

		// shorthand for max retries, will obey err.retryDelay returned by AWS
		request.insert()
			.onRetryableError(5);
		
		// config retries with a callback function
		request.insert()
			.onRetryableError((err, index) => ({
				delay: (err.retryDelay * 1000) * index, // will retry after n seconds with index factor
				max: 10,
				retryable: index >= 4 ? false : err.retryable // will retry 5x before throw (index starts with 0)
			}));
		
		// or with a plain object
		request.insert()
			.onRetryableError({
				return {
					max: 10, // will retry 10x before throw
					delay: 1000 // will retry after 1 second
				};
			});

## Final Words

If you have any questions, contact me at felipe@smallorange.co.
I intend to write a better documentation, as soon I have more time.


