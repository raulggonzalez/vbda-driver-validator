describe("vdba.<driver>.Query (Simple Aggregation Query)", function() {
  var user = config.database.user;
  var users = user.rowsWithId;
  var session = config.database.session;
  var sessions = session.rowsWithId;
  var drv, cx, db, s, u, q;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  before(function(done) {
    drv.openConnection(config.connection.config, function(error, con) {
      cx = con;
      db = cx.database;
      done();
    });
  });

  before(function(done) {
    db.createTable(user.schema, user.name, user.columns, done);
  });

  before(function(done) {
    db.createTable(session.schema, session.name, session.columns, done);
  });

  before(function(done) {
    db.findTable(user.schema, user.name, function(error, table) {
      u = table;
      done();
    });
  });

  before(function(done) {
    db.findTable(session.schema, session.name, function(error, table) {
      s = table;
      done();
    });
  });

  before(function(done) {
    u.insert(users, done);
  });

  before(function(done) {
    s.insert(sessions, done);
  });

  after(function(done) {
    db.dropTable(session.schema, session.name, done);
  });

  after(function(done) {
    db.dropTable(user.schema, user.name, done);
  });

  after(function(done) {
    cx.close(done);
  });

  describe("count", function() {
    beforeEach(function() {
      q = u.query();
      q.group("enabled");
    });

    describe("Error handling", function() {
      it("No group() called previously", function() {
        (function() {
          u.query().count();
        }).should.throwError("No grouping column specified.");
      });
    });

    it("count()", function() {
      var agg;

      q.count().should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("count");
      agg.column.should.be.eql("*");
      agg.alias.should.be.eql("count");
      should.assert(agg.filter === undefined);
    });

    it("count(callback)", function(done) {
      q.count(function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{enabled: false, count: 2}, {enabled: true, count: 5}]);
        done();
      });
    });

    it("count(filter)", function() {
      var agg;

      q.count({value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({count: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("count");
      agg.column.should.be.eql("*");
      agg.alias.should.be.eql("count");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("count(filter, callback)", function(done) {
      q.count({value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{enabled: true, count: 5}]);
        done();
      });
    });

    it("count('*', filter)", function() {
      var agg;

      q.count("*", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({count: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("count");
      agg.column.should.be.eql("*");
      agg.alias.should.be.eql("count");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("count('*', filter, callback)", function(done) {
      q.count("*", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{enabled: true, count: 5}]);
        done();
      });
    });

    it("count('*', alias, filter)", function() {
      var agg;

      q.count("*", "total", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({total: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("count");
      agg.column.should.be.eql("*");
      agg.alias.should.be.eql("total");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("count('*', alias, filter, callback)", function(done) {
      q.count('*', "total", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{enabled: true, total: 5}]);
        done();
      });
    });
  });

  describe("sum", function() {
    beforeEach(function() {
      q = s.query();
      q.group("userId");
    });

    describe("Error handling", function() {
      it("No group() called previously", function() {
        (function() {
          s.query().sum("minutes");
        }).should.throwError("No grouping column specified.");
      });

      it("sum()", function() {
        (function() {
          q.sum();
        }).should.throwError("Column name expected.");
      });
    });

    it("sum(column)", function() {
      var agg;

      q.sum("minutes").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("sum");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("sum");
    });

    it("sum(column, callback)", function(done) {
      q.sum("minutes", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, sum: 10.45}, {userId: 2, sum: 1.13}, {userId: 3, sum: null}]);
        done();
      });
    });

    it("sum(column, alias)", function() {
      var agg;

      q.sum("minutes", "total").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("sum");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("total");
    });

    it("sum(column, alias, callback)", function(done) {
      q.sum("minutes", "total", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, total: 10.45}, {userId: 2, total: 1.13}, {userId: 3, total: null}]);
        done();
      });
    });

    it("sum(column, filter)", function() {
      var agg;

      q.sum("minutes", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({sum: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("sum");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("sum");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("sum(column, filter, callback)", function(done) {
      q.sum("minutes", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, sum: 10.45}]);
        done();
      });
    });

    it("sum(column, alias, filter)", function() {
      var agg;

      q.sum("minutes", "total", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({total: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("sum");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("total");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("sum(column, alias, filter, callback)", function(done) {
      q.sum("minutes", "total", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, total: 10.45}]);
        done();
      });
    });
  });

  describe("min", function() {
    beforeEach(function() {
      q = s.query();
      q.group("userId");
    });

    describe("Error handling", function() {
      it("No group() called previously", function() {
        (function() {
          s.query().min("minutes");
        }).should.throwError("No grouping column specified.");
      });

      it("min()", function() {
        (function() {
          q.min();
        }).should.throwError("Column name expected.");
      });
    });

    it("min(column)", function() {
      var agg;

      q.min("minutes").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("min");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("min");
    });

    it("min(column, callback)", function(done) {
      q.min("minutes", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, min: 0.25}, {userId: 2, min: 1.13}, {userId: 3, min: null}]);
        done();
      });
    });

    it("min(column, alias)", function() {
      var agg;

      q.min("minutes", "minimum").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("min");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("minimum");
    });

    it("min(column, alias, callback)", function(done) {
      q.min("minutes", "minimum", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, minimum: 0.25}, {userId: 2, minimum: 1.13}, {userId: 3, minimum: null}]);
        done();
      });
    });

    it("min(column, filter)", function() {
      var agg;

      q.min("minutes", {value: {$gt: 1}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({min: {$gt: 1}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("min");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("min");
      agg.filter.should.be.eql({value: {$gt: 1}});
    });

    it("min(column, filter, callback)", function(done) {
      q.min("minutes", {value: {$gt: 1}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 2, min: 1.13}]);
        done();
      });
    });

    it("min(column, alias, filter)", function() {
      var agg;

      q.min("minutes", "minimum", {value: {$gt: 1}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({minimum: {$gt: 1}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("min");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("minimum");
      agg.filter.should.be.eql({value: {$gt: 1}});
    });

    it("min(column, alias, filter, callback)", function(done) {
      q.min("minutes", "minimum", {value: {$gt: 1}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 2, minimum: 1.13}]);
        done();
      });
    });
  });

  describe("max", function() {
    beforeEach(function() {
      q = s.query();
      q.group("userId");
    });

    describe("Error handling", function() {
      it("No group() called previously", function() {
        (function() {
          s.query().max("minutes");
        }).should.throwError("No grouping column specified.");
      });

      it("max()", function() {
        (function() {
          q.max();
        }).should.throwError("Column name expected.");
      });
    });

    it("max(column)", function() {
      var agg;

      q.max("minutes").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("max");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("max");
    });

    it("max(column, callback)", function(done) {
      q.max("minutes", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, max: 10.2}, {userId: 2, max: 1.13}, {userId: 3, max: null}]);
        done();
      });
    });

    it("max(column, alias)", function() {
      var agg;

      q.max("minutes", "maximum").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("max");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("maximum");
    });

    it("max(column, alias, callback)", function(done) {
      q.max("minutes", "maximum", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, maximum: 10.2}, {userId: 2, maximum: 1.13}, {userId: 3, maximum: null}]);
        done();
      });
    });

    it("max(column, filter)", function() {
      var agg;

      q.max("minutes", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({max: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("max");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("max");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("max(column, filter, callback)", function(done) {
      q.max("minutes", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, max: 10.2}]);
        done();
      });
    });

    it("max(column, alias, filter)", function() {
      var agg;

      q.max("minutes", "maximum", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({maximum: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("max");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("maximum");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("max(column, alias, filter, callback)", function(done) {
      q.max("minutes", "maximum", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, maximum: 10.2}]);
        done();
      });
    });
  });

  describe("avg", function() {
    beforeEach(function() {
      q = s.query();
      q.group("userId");
    });

    describe("Error handling", function() {
      it("avg() with no group() called previously", function() {
        (function() {
          s.query().avg("minutes");
        }).should.throwError("No grouping column specified.");
      });

      it("avg()", function() {
        (function() {
          q.avg();
        }).should.throwError("Column name expected.");
      });
    });

    it("avg(column)", function() {
      var agg;

      q.avg("minutes").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("avg");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("avg");
    });

    it("avg(column, callback)", function(done) {
      q.avg("minutes", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, avg: 5.225}, {userId: 2, avg: 1.13}, {userId: 3, avg: null}]);
        done();
      });
    });

    it("avg(column, alias)", function() {
      var agg;

      q.avg("minutes", "average").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("avg");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("average");
    });

    it("avg(column, alias, callback)", function(done) {
      q.avg("minutes", "average", function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, average: 5.225}, {userId: 2, average: 1.13}, {userId: 3, average: null}]);
        done();
      });
    });

    it("avg(column, filter)", function() {
      var agg;

      q.avg("minutes", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({avg: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("avg");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("avg");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("avg(column, filter, callback)", function(done) {
      q.avg("minutes", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, avg: 5.225}]);
        done();
      });
    });

    it("avg(column, alias, filter)", function() {
      var agg;

      q.avg("minutes", "average", {value: {$gt: 2}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(1);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({average: {$gt: 2}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("avg");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("average");
      agg.filter.should.be.eql({value: {$gt: 2}});
    });

    it("avg(column, alias, filter, callback)", function(done) {
      q.avg("minutes", "average", {value: {$gt: 2}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, average: 5.225}]);
        done();
      });
    });
  });

  describe("Several aggregations", function() {
    beforeEach(function() {
      q = s.query();
      q.group("userId");
    });

    it("count().sum(column)", function() {
      var agg;

      q.count().sum("minutes").should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(2);
      q.groupBy.hasFilter().should.be.eql(false);

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("count");
      agg.column.should.be.eql("*");
      agg.alias.should.be.eql("count");
      should.assert(agg.filter === undefined);

      agg = q.groupBy.aggregations[1];
      agg.name.should.be.eql("sum");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("sum");
      should.assert(agg.filter === undefined);
    });

    it("count().sum(column).find(callback)", function(done) {
      q.count().sum("minutes").find(function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([
          {userId: 1, count: 2, sum: 10.45},
          {userId: 2, count: 1, sum: 1.13},
          {userId: 3, count: 1, sum: null}
        ]);
        done();
      });
    });

    it("count(filter).sum(column, filter)", function() {
      var agg;

      q.count({value: {$ge: 1}}).sum("minutes", {value: {$gt: 5}}).should.be.exactly(q);
      q.groupBy.aggregations.length.should.be.eql(2);
      q.groupBy.hasFilter().should.be.eql(true);
      q.groupBy.filter.should.be.eql({count: {$ge: 1}, sum: {$gt: 5}});

      agg = q.groupBy.aggregations[0];
      agg.name.should.be.eql("count");
      agg.column.should.be.eql("*");
      agg.alias.should.be.eql("count");
      agg.filter.should.be.eql({value: {$ge: 1}});

      agg = q.groupBy.aggregations[1];
      agg.name.should.be.eql("sum");
      agg.column.should.be.eql("minutes");
      agg.alias.should.be.eql("sum");
      agg.filter.should.be.eql({value: {$gt: 5}});
    });

    it("count(filter).sum(column, filter).find(callback)", function(done) {
      q.count({value: {$ge: 1}}).sum("minutes", {value: {$gt: 5}}).find(function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([{userId: 1, count: 2, sum: 10.45}]);
        done();
      });
    });
  });
});