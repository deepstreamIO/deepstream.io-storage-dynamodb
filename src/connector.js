'use strict';

const EventEmitter = require( 'events' ).EventEmitter;
const AWS = require( 'aws-sdk' );
const pkg = require( '../package.json' );
const dataTransform = require( './transform-data' );

/**
 *
 */
module.exports = class DynamoDbConnector extends EventEmitter {
	/**
	 * Constructor
	 *
	 * @param {Object} options - "table" required, "db" optional, "region" optional
	 *
	 * @constructor
	 */
	constructor(options) {
		super();

		// Basic properties
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

			this._db.waitFor('tableExists', { TableName: tableName }, function( err, data ){
				callback( err, data );
			});
		});
	}

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
	 * Writes a value to the cache.
	 *
	 * @param {String}   id
	 * @param {Object}   data
	 * @param {Function} callback Should be called with null for successful set operations or with an error message string
	 *
	 * @private
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
	 * Retrieves a value from the cache
	 *
	 * @param {String}   id
	 * @param {Function} callback Will be called with null and the stored object
	 *                            for successful operations or with an error message string
	 *
	 * @private
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
	 * Deletes an entry from the cache.
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

	_onBatchWriteComplete( callbacks, err, data ) {
		for( var i = 0; i < callbacks.length; i++ ) {
			callbacks[ i ]( err, data );
		}
	}

	_getId( name ) {
		return name.substr( 6 );
	}

	_getTableName( name ) {
		return name.substr( 0, 6 );
	}
}