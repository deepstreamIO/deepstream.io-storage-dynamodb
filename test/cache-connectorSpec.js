'use strict'

/* global describe, expect, it, jasmine */
const expect = require('chai').expect
const CacheConnector = require('../src/connector')
const EventEmitter = require('events').EventEmitter

const settings = {
	'region': 'eu-central-1'
}

const MESSAGE_TIME = 200

describe( 'the message connector has the correct structure', () => {
	var cacheConnector

	it( 'throws an error if required connection parameters are missing', () => {
		expect( () => { new CacheConnector( 'gibberish' ) } ).to.throw()
	})

	it( 'creates the cacheConnector', () => {
		cacheConnector = new CacheConnector( settings )
		expect( cacheConnector.isReady ).to.equal( true )
	})

	it( 'implements the cache/storage connector interface', () =>  {
		expect( cacheConnector.name ).to.be.a( 'string' )
		expect( cacheConnector.version ).to.be.a( 'string' )
		expect( cacheConnector.get ).to.be.a( 'function' )
		expect( cacheConnector.set ).to.be.a( 'function' )
		expect( cacheConnector.delete ).to.be.a( 'function' )
		expect( cacheConnector.createTable ).to.be.a( 'function' )
		expect( cacheConnector.deleteTable ).to.be.a( 'function' )
		expect( cacheConnector instanceof EventEmitter ).to.equal( true )
	});
});

describe( 'creates and deletes tables', function() {
	var cacheConnector;
	var TABLE_NAME = 'some-test-table';

	this.timeout( 100000 );

	it( 'creates the cacheConnector', () => {
		cacheConnector = new CacheConnector( settings )
		expect( cacheConnector.isReady ).to.equal( true )
	})

	it( 'creates a table', ( done ) => {
		cacheConnector.createTable( TABLE_NAME, ( err, data ) => {
			expect( err ).to.be.null;
			done();
		});
	});

	it( 'fails when trying to create an existing table', ( done ) => {
		cacheConnector.createTable( TABLE_NAME, ( err, data ) => {
			expect( err ).to.not.be.null;
			done();
		});
	});

	it( 'deletes a table', ( done ) => {
		cacheConnector.deleteTable( TABLE_NAME, ( err, data ) => {
			expect( err ).to.be.null;
			done();
		});
	});

	it( 'fails when trying to delete a table a second time', ( done ) => {
		cacheConnector.deleteTable( TABLE_NAME, ( err, data ) => {
			expect( err ).to.not.be.null;
			done();
		});
	});
});


	// it( 'retrieves a non existing value', ( done ) => {
		// cacheConnector.get( 'someValue', ( error, value ) => {
			// expect( error ).to.equal( null )
			// expect( value ).to.equal( null )
			// done()
		// })
	// })

	// it( 'sets a value', ( done ) => {
		// cacheConnector.set( 'someValue', {  _d: { v: 10 }, firstname: 'Wolfram' }, ( error ) => {
			// expect( error ).to.equal( null )
			// done()
		// })
	// })

	// it( 'retrieves an existing value', ( done ) => {
		// cacheConnector.get( 'someValue', ( error, value ) => {
			// expect( error ).to.equal( null )
			// expect( value ).to.deep.equal( {  _d: { v: 10 }, firstname: 'Wolfram' } )
			// done()
		// })
	// })

	// it( 'deletes a value', ( done ) => {
		// cacheConnector.delete( 'someValue', ( error ) => {
			// expect( error ).to.equal( null )
			// done()
		// })
	// })

	// it( 'Can\'t retrieve a deleted value', ( done ) => {
		// cacheConnector.get( 'someValue', ( error, value ) => {
			// expect( error ).to.equal( null )
			// expect( value ).to.equal( null )
			// done()
		// })
	// })