var QueryStream = require('pg-query-stream')
var assert = require('assert')
var pg = require('./lib/pg')
var mosql = require('mongo-sql')
var stream = require('stream')
var query = require('./lib/query')

var getStreamQuery = function(config) {
  var q = {
    type: 'select',
    table: config.table,
    columns: [{
      name: config.keyColumn,
      alias: 'key'
    }, {
      name: config.valueColumn,
      alias: 'value'
    }],
    where: {}
  }
  if(!config.noOrder) {
    q.order = config.keyColumn
  }
  if(config.start) {
    q.where[config.keyColumn] = {$gte: config.start}
  }
  if(config.end) {
    q.where[config.keyColumn] = q.where[config.keyColumn] || {}
    q.where[config.keyColumn]['$lte'] = config.end
  }
  if(config.limit) {
    q.limit = config.limit
  }
  if(config.offset) {
    q.offset = config.offset
  }
  return mosql.sql(q).toQuery()
}

var getMapStream = function(config) {
  var mapStream = new stream.Transform({
    objectMode: true,
    highWaterMark: config.highWaterMark
  })
  mapStream._transform = function(data, _, cb) {
    data.save = config.saveFn
    this.push(data)
    cb()
  }
  return mapStream
}

var attachSaveFn = function(config) {
  config.saveFn = function(cb) {
    var q = {
      type: 'update',
      table: config.table,
      updates: {},
      where: {}
    }
    q.updates[config.valueColumn] = this.value
    q.where[config.keyColumn] = this.key
    query(q, cb)
  }
}

module.exports = function(config) {
  assert(config, 'missing required configuration')
  attachSaveFn(config)
  var moquery = getStreamQuery(config)
  var query = new QueryStream(moquery.text, moquery.values, config)
  var client = new pg.Client()
  var stream = client.query(query)
  client.on('drain', client.end.bind(client))
  client.on('error', stream.emit.bind(stream, 'error'))
  client.connect()
  return stream.pipe(getMapStream(config))
}
