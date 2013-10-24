var ok = require('okay')
var query = require('../lib/query')
var beatles = module.exports = [{
  id: 1,
  data: {
    name: 'John',
    age: 30
  }
}, {
  id: 2,
  data: {
    name: 'Paul',
    age: 28
  }
}, {
  id: 3,
  data: {
    name: 'George',
    age: 27
  }
}, {
  id: 4,
  data: {
    name: 'Ringo',
    age: 30,
    drummer: true
  }
}]

beatles.recreate = function(table, done) {
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
}
