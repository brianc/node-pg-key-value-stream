var mosql = require('mongo-sql')
var ok = require('okay')
var pg = require('./pg')

var query = module.exports = function(mongoQuery, cb) {
  var q = mosql.sql(mongoQuery).toQuery()
  pg.connect(ok(cb, function(client, done) {
    client.query(q.text, q.values, function(err, result) {
      done()
      cb(err, (result||0).rows, result)
    })
  }))
}
