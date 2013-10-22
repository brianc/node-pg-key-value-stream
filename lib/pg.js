try {
  module.exports = require('pg.js')
}
catch(e)  {
  module.exports = require('pg')
}

