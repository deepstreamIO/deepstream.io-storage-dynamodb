'use strict';

const EventEmitter = require( 'events' ).EventEmitter;
const AWS = require( 'aws-sdk' );
const pkg = require( '../package.json' );
const dataTransform = require( './transform-data' );

/**
 * This class connects deepstream nodes within DSH to AWS DynamoDB.
 * It makes the following assumptions:
 *
 * - Every item-id needs to start with a 6 character application id
 *   that will be used to identify the table the entry will be stored in.
 *
 * - Tables are expected to be created as part of the app-creation and thus to
 *   already exists at the time of calling get, set or delete
 *
 * - The callbacks for the creation and deletion of tables are invoked once the
 *   action is complete, not when the request has been received. That leads to
 *   an ~20 seconds delay for these operations. This behaviour can be changed by passing
 *   true as the third argument to createTable and delete table
 *
 * - Write and delete operations will be batched. The first write or delete will be executed
 *   immediatly, the next will be executed after [options.bufferTimeout] milliseconds
 *
 * - One connector is scoped to one AWS region, defined in [options.region]. If you need more
 *   regions, create more connectors.
 */
module.exports = class DynamoDbConnector extends EventEmitter {

	/**
	 * Constructor. Please note, the DynamoDB API is request/response based (HTTPS).
	 * The connector therefor does not create a connection at initialisation and is
	 * therefor immediatly ready
	 *
	 * @param {Object} options - { 'region': STRING, bufferTimeout: NUMBER }
	 *
	 * e.g. new DynamoDbConnector({
	 *			region: 'eu-central-1',
	 *			bufferTimeout: 500
	 *		});
	 *
	 * @constructor
	 * @public
	 */
	constructor(options) {
		super();


		this.isReady = true;
		this.name = pkg.name;
		this.version = pkg.version;

		if (!options.region) {
			throw new Error( 'options.region is required' );
		}

		if(!options.bufferTimeout) {
			throw new Error( 'options.bufferTimeout is required' );
		}

		AWS.config.update({
			region: options.region
		});

		this._writeBatchFn = this._writeBatch.bind( this );
		this._options = options;
		this._batch = null;
		this._batchCallbacks = null;
		this._batchTimeout = null;

		this._db = new AWS.DynamoDB();
		this._documentClient = new AWS.DynamoDB.DocumentClient();
	}

	/**
	 * Creates a new table within DynamoDB. This will take about
	 * 20 seconds. Use the dontWait flag to only acknowledge
	 * that the table creation request was received,
	 * but not wait until it is completed.
	 *
	 * @param   {String}   tableName the six character app-id
	 * @param   {Function} callback  Callback function that will be invoked with an error and data
	 * @param   {Boolean}  dontWait  If true, callback will be invoked as soon as the request is confirmed
	 *
	 * @public
	 * @returns {void}
	 */
	createTable( tableName, callback, dontWait ) {
		var params = {
			AttributeDefinitions: [{
				AttributeName: 'ds_id',
				AttributeType: 'S'
			}],
			KeySchema: [{
				AttributeName: 'ds_id',
				KeyType: 'HASH'
			}],
			ProvisionedThroughput: {
				ReadCapacityUnits: 5,
				WriteCapacityUnits: 5
			},
			TableName: tableName,

			StreamSpecification: {
			  StreamEnabled: true,
			  StreamViewType: 'NEW_AND_OLD_IMAGES'
			}
		};

		this._db.createTable( params, ( err, data ) => {
			if( err ) {
				callback( err );
				return;
			}

			if( dontWait === true ) {
				callback( null, data );
				return;
			}

			this._db.waitFor( 'tableExists', { TableName: tableName }, function( err, data ){
				callback( err, data );
			});
		});
	}

	/**
	 * Deletes an existing table within DynamoDB. This will take about
	 * 20 seconds. Use the dontWait flag to only acknowledge
	 * that the table deletion request was received,
	 * but not wait until it is completed.
	 *
	 * @param   {String}   tableName the six character app-id
	 * @param   {Function} callback  Callback function that will be invoked with an error and data
	 * @param   {Boolean}  dontWait  If true, callback will be invoked as soon as the request is confirmed
	 *
	 * @public
	 * @returns {void}
	 */
	deleteTable( tableName, callback, dontWait ) {
		var params = {
			TableName : tableName
		};

		this._db.deleteTable( params, ( err, data ) => {
			if( err ) {
				callback( err );
				return;
			}

			if( dontWait === true ) {
				callback( null, data );
				return;
			}

			this._db.waitFor( 'tableNotExists', { TableName: tableName }, function( err, data ){
				callback( err, data );
			});
		});
	}

	/**
	 * Writes a value to the database
	 *
	 * @param {String}   the combined identifier string, consisting of <app-id><name>
	 * @param {Object}   data
	 * @param {Function} callback Should be called with null for successful set operations or with an error message string
	 *
	 * @public
	 * @returns {void}
	 */
	set(id, data, cb) {
		data = dataTransform.transformValueForStorage( data );
		data.ds_id = this._getId( id );

		const params = {
			PutRequest: {
				Item: data
			}
		}

		this._addToBatch( this._getTableName( id ), params, cb );
	}

	/**
	 * Retrieves a value from the database
	 *
	 * @param {String}   id
	 * @param {Function} callback Will be called with null and the stored object
	 *                            for successful operations or with an error message string
	 *
	 * @public
	 * @returns {void}
	 */
	get(id, cb) {
		const params = {
			TableName: this._getTableName( id ),
			Key: {
				ds_id: this._getId( id )
			}
		};

		this._documentClient.get(params, (err, res) => {
			if (err) {
				if( err.code === 'ResourceNotFoundException' ) {
					cb( null, null );
				} else {
					cb(err.message);
				}
			}
			else if (!res || !res.Item) {
				cb(null, null);
			}
			else {
				delete res.Item.ds_id;
				var data = dataTransform.transformValueFromStorage( res.Item );
				cb(null, data );
			}
		});
	}

	/**
	 * Deletes an entry from the database.
	 *
	 * @param   {String}   id
	 * @param   {Function} callback Will be called with null for successful deletions or with
	 *                     an error message string
	 *
	 * @private
	 * @returns {void}
	 */
	delete(id, cb) {
		const params = {
			DeleteRequest: {
				Key: { ds_id: this._getId( id ) }
			}
		};

		this._addToBatch( this._getTableName( id ), params, cb );
	}

	/**
	 * Returns the index of an existing item from within the current batch.
	 * This is executed for both sets and deletes.
	 *
	 * Returns -1 if no item could be found
	 *
	 * @param   {String} table the name of the table to search for
	 * @param   {Object} item  a dynamo-db interaction item
	 *
	 * @private
	 * @returns {Number} index
	 */
	_getIndexWithinBatch( table, item ) {
		for( var i = 0; i < this._batch.RequestItems[ table ].length; i++ ) {
			if( item.PutRequest && this._batch.RequestItems[ table ][ i ].PutRequest ) {
				if( item.PutRequest.Item.ds_id === this._batch.RequestItems[ table ][ i ].PutRequest.Item.ds_id ) {
					return i;
				}
			}

			if( item.DeleteRequest && this._batch.RequestItems[ table ][ i ].DeleteRequest ) {
				if( item.DeleteRequest.Key.ds_id === this._batch.RequestItems[ table ][ i ].DeleteRequest.Key.ds_id ) {
					return i;
				}
			}
		}

		return -1;
	}

	/**
	 * Adds an item to the object containing the batch for the next
	 * database interaction. Please find more details for
	 * item structures here:
	 *
	 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#batchWrite-property
	 *
	 * @param {String}   table the name of the table to write or delete the item from
	 * @param {Object}   item  dynamoDB item
	 * @param {Function} cb    callback that will be invoked once write is complete
	 *
	 * @private
	 * @returns {void}
	 */
	_addToBatch( table, item, cb ) {
		if( !this._batch ) {
			this._batch = { RequestItems: {} };
			this._batchCallbacks = [];
		}

		if( !this._batch.RequestItems[ table ] ) {
			this._batch.RequestItems[ table ] = [];
		}

		var indexWithinBatch = this._getIndexWithinBatch( table, item );

		if( indexWithinBatch === -1 ) {
			this._batch.RequestItems[ table ].push( item );
		} else {
			this._batch.RequestItems[ table ][ indexWithinBatch ] = item;
		}

		this._batchCallbacks.push( cb );

		if( this._batchTimeout === null ) {
			this._writeBatch();
		}
	}

	/**
	 * Executes a batch request with the currently batched items. Clears down the current batch
	 * and schedules the next write
	 *
	 * @private
	 * @returns {void}
	 */
	_writeBatch() {
		if( !this._batch ) {
			this._batchTimeout = null;
			return;
		}

		var callbacks = Array.prototype.slice.call( this._batchCallbacks );

		this._documentClient.batchWrite( this._batch, this._onBatchWriteComplete.bind( this, callbacks ) );
		this._batchCallbacks = [];
		this._batch = null;
		this._batchTimeout = setTimeout( this._writeBatchFn, this._options.bufferTimeout );
	}

	/**
	 * Callback for completed batch-executions. Iterates trough the existing
	 * array of callbacks and invokes them with the resulting error and data
	 *
	 * @param   {Array}  callbacks Callbacks for this batch interaction
	 * @param   {Error}  err       AWS.DynamoDB.BatchError
	 * @param   {Object} data      Execution report
	 *
	 * @private
	 * @returns {void}
	 */
	_onBatchWriteComplete( callbacks, err, data ) {
		for( var i = 0; i < callbacks.length; i++ ) {
			callbacks[ i ]( err, data );
		}
	}

	/**
	 * Extracts the item id from a given item name
	 *
	 * @param   {String} name the full name of a dsh item
	 *
	 * @private
	 * @returns {String} id the item id for a dsh item
	 */
	_getId( name ) {
		return name.substr( 6 );
	}

	/**
	 * Extracts the app-id/table name from a given item name
	 *
	 * @param   {String} name the full name of a dsh item
	 *
	 * @private
	 * @returns {String} app-id/table name for a dsh item
	 */
	_getTableName( name ) {
		return name.substr( 0, 6 );
	}
}