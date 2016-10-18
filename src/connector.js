'use strict';

const EventEmitter = require( 'events' ).EventEmitter;
const AWS = require( 'aws-sdk' );
const pkg = require( '../package.json' );

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

		// Set region
		if (options.region) {
			AWS.config.update({
				region: options.region
			});
		}

		// Set table
		if (!options.table) {
			throw new Error('"table" option is required');
		}
		this.table = options.table;

		// Set DynamoDb instance
		if (!options.db) {
			options.db = new AWS.DynamoDB.DocumentClient();
		}
		this.db = options.db;

	}

	createTable( tableName, callback ) {
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
			TableName: tableName
		 
		 
			// StreamSpecification: {
			//   StreamEnabled: true || false,
			//   StreamViewType: 'NEW_IMAGE | OLD_IMAGE | NEW_AND_OLD_IMAGES | KEYS_ONLY'
			// }
		};
			( new AWS.DynamoDB() ).createTable(params, callback );
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
		data.ds_id = id;
		const params = {
			TableName: this.table,
			Item: data
		};
		// console.log('Set', params);
		this.db.put(params, err => {
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
		// console.log('Get', params);
		this.db.get(params, (err, res) => {
			if (err) {
				return cb(err.message);
			}
			if (!res || !res.Item) {
				return cb(null, null);
			}
			delete res.Item.ds_id;
			return cb(null, res.Item)
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
		this.db.delete(params, err => {
			cb(err ? err.message : null);
		});
	}
}