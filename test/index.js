var kvpStream = require('../')
var query = require('../lib/query')
var ok = require('okay')
var concat = require('concat-stream')
var assert = require('assert')

var table = 'test_store'

describe('kvpStream', function() {
  after(function(done) {
    query({
      type: 'drop-table',
      ifExists: true,
      table: table
    }, done)
  })
  before(function(done) {
    var q = {
      type: 'create-table',
      ifNotExists: true,
      table: table,
      definition: {
        id: {
          type: 'serial',
          primaryKey: true
        },
        data: {
          type: 'json'
        }
      }
    }
    query({
      type: 'drop-table',
      ifExists: true,
      table: table
    }, function() {
      query(q, ok(done, function() {
        query({
          type: 'insert',
          table: table,
          values: require('./beatles')
        }, done)
      }))

    })
  })

  var meetTheBeatles = function() {
    var options = {
      keyColumn: 'id',
      valueColumn: 'data',
      table: table
    }
    return kvpStream(options)
  }

  it('can read', function(done) {
    meetTheBeatles().pipe(concat(function(rows) {
      assert.equal(rows.length, 4)
      assert.equal(rows[0].value.name, 'John')
      assert.equal(rows[1].key, 2)
      assert.equal(rows[2].value.age, 27)
      assert.equal(rows[3].value.drummer, true, 'should be drummer')
      done()
    }))
  })

  it('can close')

  it('can save', function(done) {
    var theBeatles = meetTheBeatles()
    .once('readable', function() {
      var john = theBeatles.read()
      assert(john)
      john.value.drummer = false
      john.save(ok(done, function() {
        setTimeout(done, 100)
      }))
    })
  })

  it('save works', function() {
    var theBeatles = meetTheBeatles().pipe(concat(function(rows) {
      assert.equal(rows[0].value.name, 'John')
      assert.strictEqual(rows[0].value.drummer, false, 'john should not be a dummer')
      assert.strictEqual(rows[1].value.drummer, undefined)
    }))
  })

  it('can start farther down', function(done) {
    var stream = kvpStream({
      keyColumn: 'id',
      valueColumn: 'data',
      table: table,
      start: 2
    })
    stream.pipe(concat(function(res) {
      assert(res, 'should have result')
      assert.equal(res.length, 3, 'should only return 3 but returned ' + res.length)
      assert.equal(res[0].key, 2, 'first id should be 2 but was', + res[0].key)
      done()
    }))
  })

  it('can end', function(done) {
    var stream = kvpStream({
      keyColumn: 'id',
      valueColumn: 'data',
      table: table,
      start: 2,
      end: 3
    })
    stream.pipe(concat(function(res) {
      assert(res, 'should have result')
      assert.equal(res.length, 2, 'should only return 2 but returned ' + res.length)
      assert.equal(res[0].key, 2, 'first id should be 2 but was', + res[0].key)
      assert.equal(res[1].key, 3, 'second id should be 3 but was', + res[1].key)
      done()
    }))
  })

  it('can limit', function(done) {
    var stream = kvpStream({
      keyColumn: 'id',
      valueColumn: 'data',
      table: table,
      limit: 1
    })
    stream.pipe(concat(function(res) {
      assert(res, 'should have result')
      assert.equal(res.length, 1, 'should only return 1 but returned ' + res.length)
      assert.equal(res[0].key, 1, 'first id should be 1 but was', + res[0].key)
      done()
    }))
  })
})
