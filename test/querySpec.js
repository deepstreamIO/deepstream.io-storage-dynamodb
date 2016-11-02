'use strict'

/* global describe, expect, it, jasmine */
const expect = require('chai').expect
const DbConnector = require('../src/connector')
const EventEmitter = require('events').EventEmitter

const settings = {
	'region': 'eu-central-1',
	'bufferTimeout': 1200
};
const MESSAGE_TIME = 200;
const APP_ID = 'vf43ss';

/* Only run once */
// describe.only( 'creates tables', function() {
// 	var dbConnector;
// 	this.timeout( 100000 );

// 	it( 'creates the dbConnector', () => {
// 		dbConnector = new DbConnector( settings )
// 		expect( dbConnector.isReady ).to.equal( true )
// 	})

// 	it( 'creates a table', ( done ) => {
// 		dbConnector.createTable( APP_ID, ( err, data ) => {
// 			expect( err ).to.be.null;
// 			done();
// 		});
// 	});
// });

describe( 'writes multiple values', function(){
	var dbConnector;
	var key = APP_ID + 'someValue';
	var getValue = () => { return {  _d: { firstname: 'Wolfram' }, v: 10 }; };

	it( 'creates the dbConnector', () => {
		dbConnector = new DbConnector( settings )
		expect( dbConnector.isReady ).to.equal( true )
	})

	it( 'sets value a', ( done ) => {
		var key = APP_ID + 'test-entries/a';
		var value = {  _d: { letter: 'a' }, v: 10 }
		dbConnector.set( key, value, ( error ) => {
			expect( error ).to.equal( null )
			done()
		})
	});

	it( 'sets value b', ( done ) => {
		var key = APP_ID + 'test-entries/b';
		var value = {  _d: { letter: 'b' }, v: 10 }
		dbConnector.set( key, value, ( error ) => {
			expect( error ).to.equal( null )
			done()
		})
	});

	it( 'sets value c', ( done ) => {
		var key = APP_ID + 'test-entries/c';
		var value = {  _d: { letter: 'c' }, v: 10 }
		dbConnector.set( key, value, ( error ) => {
			expect( error ).to.equal( null )
			done()
		})
	});
});

describe.only( 'retrieve all indices', function(){
	var dbConnector;

	it( 'creates the dbConnector', () => {
		dbConnector = new DbConnector( settings )
		expect( dbConnector.isReady ).to.equal( true )
	})

	it( 'queries the db', done => {
		dbConnector.getAllKeysForApp( APP_ID, ( err, result ) => {
			console.log( err, result );
			done();
		})
	})
})