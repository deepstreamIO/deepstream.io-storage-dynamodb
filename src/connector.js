'use strict';

const EventEmitter = require( 'events' ).EventEmitter;
const AWS = require( 'aws-sdk' );
const pkg = require( '../package.json' );
const dataTransform = require( './transform-data' );

/**
 *
 */
module.exports = class DynamoDbConnector extends EventEmitter
{
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


		this.db = new AWS.DynamoDB();
		this.documentClient = new AWS.DynamoDB.DocumentClient();
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

		this.db.createTable( params, ( err, data ) => {
			if( err ) {
				callback( err );
				return;
			}

			if( dontWait === true ) {
				callback( null, data );
				return;
			}

			this.db.waitFor('tableExists', { TableName: tableName }, function( err, data ){
				callback( err, data );
			});
		});
	}

	deleteTable( tableName, callback, dontWait ) {
		var params = {
			TableName : tableName
		};

		this.db.deleteTable( params, ( err, data ) => {
			if( err ) {
				callback( err );
				return;
			}

			if( dontWait === true ) {
				callback( null, data );
				return;
			}

			this.db.waitFor( 'tableNotExists', { TableName: tableName }, function( err, data ){
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
			TableName: this._getTableName( id ),
			Item: data
		};

		this.documentClient.put(params, function( err ) {
			cb(err ? err.message : null);
		});
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

		this.documentClient.get(params, (err, res) => {
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
			TableName: this._getTableName( id ),
			Key: {
				ds_id: this._getId( id )
			}
		};
		this.documentClient.delete(params, err => {
			cb(err ? err.message : null);
		});
	}

	_getId( name ) {
		return name.substr( 6 );
	}

	_getTableName( name ) {
		return name.substr( 0, 6 );
	}
}