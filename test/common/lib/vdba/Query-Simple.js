describe("vdba.<driver>.Query (Simple)", function() {
  var user = config.database.user;
  var users = user.rowsWithId;
  var drv, cx, db, u, q;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  beforeEach(function(done) {
    drv.openConnection(config.connection.config, function(error, con) {
      cx = con;
      db = cx.database;
      done();
    });
  });

  beforeEach(function(done) {
    db.createTable(user.schema, user.name, user.columns, done);
  });

  beforeEach(function(done) {
    db.findTable(user.schema, user.name, function(error, table) {
      u = table;
      done();
    });
  });

  beforeEach(function(done) {
    u.insert(users, done);
  });

  beforeEach(function() {
    q = u.query();
  });

  afterEach(function(done) {
    db.dropTable(user.schema, user.name, done);
  });

  afterEach(function(done) {
    cx.close(done);
  });

  describe("#limit()", function() {
    describe("Error handling", function() {
      it("limit()", function() {
        (function() {
          q.limit();
        }).should.throwError("Count expected.");
      });
    });

    describe("No callback", function() {
      it("limit(count)", function() {
        q.limit(3).should.be.exactly(q);
        q.limitTo.should.be.eql({count: 3, start: 0});
      });

      it("limit(count, start)", function() {
        q.limit(3, 2).should.be.exactly(q);
        q.limitTo.should.be.eql({count: 3, start: 2});
      });
    });

    describe("With callback", function() {
      it("limit(count, callback)", function(done) {
        q.limit(3, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.length.should.be.eql(3);
          done();
        });
      });

      it("limit(count, start, callback)", function(done) {
        q.limit(3, 2, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.length.should.be.eql(3);
          done();
        });
      });
    });
  });

  describe("#sort()", function() {
    describe("Error handling", function() {
      it("sort()", function() {
        (function() {
          q.sort();
        }).should.throwError("Ordering column(s) expected.");
      });

      it("sort([])", function() {
        (function() {
          q.sort([]);
        }).should.throwError("Ordering column(s) expected.");
      });

      it("sort({})", function() {
        (function() {
          q.sort({});
        }).should.throwError("Ordering column(s) expected.");
      });
    });

    describe("No callback", function() {
      it("sort(column)", function() {
        q.sort("userId").should.be.exactly(q);
        q.orderBy.should.be.eql({userId: "ASC"});
      });

      it("sort([columns])", function() {
        q.sort(["username", "password"]).should.be.exactly(q);
        q.orderBy.should.be.eql({username: "ASC", password: "ASC"});
      });

      it("sort({columns})", function() {
        q.sort({username: "DESC", password: "ASC"}).should.be.exactly(q);
        q.orderBy.should.be.eql({username: "DESC", password: "ASC"});
      });
    });

    describe("With callback", function() {
      it("sort(column, callback)", function(done) {
        q.sort("enabled", function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([users[1], users[4], users[0], users[2], users[3], users[5], users[6]]);
          done();
        });
      });

      it("sort([columns], callback)", function(done) {
        q.sort(["enabled", "userId"], function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([users[1], users[4], users[0], users[2], users[3], users[5], users[6]]);
          done();
        });
      });

      it("sort({columns}, callback)", function(done) {
        q.sort({enabled: "ASC", "userId": "DESC"}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([users[4], users[1], users[6], users[5], users[3], users[2], users[0]]);
          done();
        });
      });
    });
  });

  describe("#filter()", function() {
    describe("Error handling", function() {
      it("filter()", function() {
        (function() {
          q.filter();
        }).should.throwError("Filter expected.");
      });
    });

    describe("No callback", function() {
      it("filter({})", function() {
        q.filter({}).should.be.exactly(q);
        q.filterBy.should.be.eql({});
      });

      it("filter({simple})", function() {
        q.filter({userId: 3}).should.be.exactly(q);
        q.filterBy.should.be.eql({userId: 3});
      });
    });

    describe("With callback", function() {
      it("filter({}, callback)", function(done) {
        q.filter({}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(users);
          done();
        });
      });

      it("filter({simple}, callback)", function(done) {
        q.filter({userId: 3}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([users[2]]);
          done();
        });
      });

      it("filter({compound}, callback)", function(done) {
        q.filter({username: {$like: "user_1"}, password: {$like: "%11"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([users[5]]);
          done();
        });
      });

      it("filter(filter, callback) - No rows", function(done) {
        q.filter({username: "xxxxxx"}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([]);
          done();
        });
      });
    });
  });

  describe("#findAll()", function() {
    describe("Error handling", function() {
      it("findAll()", function() {
        (function() {
          q.findAll();
        }).should.throwError("Callback expected.");
      });
    });

    it("findAll(callback)", function(done) {
      q.findAll(function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql(users);
        done();
      });
    });
  });

  describe("#find()", function() {
    describe("Error handling", function() {
      it("find()", function() {
        (function() {
          q.find();
        }).should.throwError("Callback expected.");
      });

      it("find(filter)", function() {
        (function() {
          q.find({});
        }).should.throwError("Callback expected.");
      });
    });

    it("find(callback)", function(done) {
      q.find(function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql(users);
        done();
      });
    });

    it("find({}, callback)", function(done) {
      q.find({}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql(users);
        done();
      });
    });

    it("find(filter, callback)", function(done) {
      q.find({username: {$like: "user_1"}}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql([users[0], users[5]]);
        done();
      });
    });

    it("find(filter, callback) - No rows", function(done) {
      q.find({username: "xxxxxx"}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql([]);
        done();
      });
    });
  });

  describe("#findOne()", function() {
    describe("Error handling", function() {
      it("findOne()", function() {
        (function() {
          q.findOne();
        }).should.throwError("Callback expected.");
      });

      it("findOne(filter)", function() {
        (function() {
          q.findOne({});
        }).should.throwError("Callback expected.");
      });
    });

    it("findOne(callback)", function(done) {
      q.findOne(function(error, row) {
        should.assert(error === undefined);
        should.assert(row !== undefined);
        done();
      });
    });

    it("findOne({}, callback)", function(done) {
      q.findOne({}, function(error, row) {
        should.assert(error === undefined);
        should.assert(row !== undefined);
        done();
      });
    });

    it("findOne(filter, callback)", function(done) {
      q.findOne({userId: 3}, function(error, row) {
        should.assert(error === undefined);
        row.should.be.eql(users[2]);
        done();
      });
    });

    it("findOne(filter, callback) - No row", function(done) {
      q.findOne({username: "xxxxxx"}, function(error, row) {
        should.assert(error === undefined);
        should.assert(row === undefined);
        done();
      });
    });
  });
});