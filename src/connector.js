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

		AWS.config.update({
			region: options.region
		});


		this.db = new AWS.DynamoDB();
		this.documentClient = options.documentClient || new AWS.DynamoDB.DocumentClient();
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

		const params = {
			TableName: this.table,
			Item: dataTransform.transformValueForStorage( data )
		};
		// console.log('Set', params);
		this.documentClient.put(params, err => {
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
			TableName: this.table,
			Key: {
				ds_id: id
			}
		};

		this.documentClient.get(params, (err, res) => {
			if (err) {
				return cb(err.message);
			}
			if (!res || !res.Item) {
				return cb(null, null);
			}

			return cb(null, dataTrasform.transformValueFromStorage( res.Item ) );
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
			TableName: this.table,
			Key: {
				ds_id: id
			}
		};
		this.documentClient.delete(params, err => {
			cb(err ? err.message : null);
		});
	}
}