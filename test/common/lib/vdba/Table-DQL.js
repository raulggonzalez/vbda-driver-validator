describe("vdba.<driver>.Table (DQL)", function() {
  var user = config.database.user;
  var User = config.orm.User;
  var rows = user.rowsWithId;
  var drv, cx, db, tbl;

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
    db.findTable(user.schema, user.name, function(error, tab) {
      tbl = tab;
      tbl.insert(rows, done);
    });
  });

  after(function(done) {
    db.dropTable(user.schema, user.name, done);
  });

  after(function(done) {
    cx.close(done);
  });

  describe("#count()", function() {
    describe("Error handling", function() {
      it("count()", function() {
        (function() {
          tbl.count();
        }).should.throwError("Callback expected.");
      });
    });

    it("count(callback)", function(done) {
      tbl.count(function(error, count) {
        should.assert(error === undefined);
        count.should.be.eql(rows.length);
        done();
      });
    });
  });

  describe("#findAll()", function() {
    describe("Error handling", function() {
      it("findAll()", function() {
        (function() {
          tbl.findAll();
        }).should.throwError("Callback expected.");
      });
    });

    it("findAll(callback)", function(done) {
      tbl.findAll(function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql(rows);
        done();
      });
    });
  });

  describe("#find()", function() {
    describe("Error handling", function() {
      it("find()", function() {
        (function() {
          tbl.find();
        }).should.throwError("Callback expected.");
      });

      it("find(filter)", function() {
        (function() {
          tbl.find({});
        }).should.throwError("Callback expected.");
      });
    });

    it("find(callback)", function(done) {
      tbl.find(function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql(rows);
        done();
      });
    });

    it("find({}, callback)", function(done) {
      tbl.find({}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.rows.should.be.eql(rows);
        done();
      });
    });

    it("find(filter, callback) - No rows matched", function(done) {
      tbl.find({username: "xxxxxx"}, function(error, result) {
        should.assert(error === undefined);
        result.length.should.be.eql(0);
        done();
      });
    });

    describe("Simple", function() {
      it("find({col: value}, callback)", function(done) {
        tbl.find({username: "user03"}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([rows[2]]);
          done();
        });
      });

      it("find({col: {$eq: value}}, callback)", function(done) {
        tbl.find({username: {$eq: "user03"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([rows[2]]);
          done();
        });
      });

      it("find({col: {$ne: value}}, callback)", function(done) {
        tbl.find({username: {$ne: "user03"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(0, 2).concat(rows.slice(3)));
          done();
        });
      });

      it("find({col: {$gt: value}}, callback)", function(done) {
        tbl.find({username: {$gt: "user03"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(3));
          done();
        });
      });

      it("find({col: {$ge: value}}, callback)", function(done) {
        tbl.find({username: {$ge: "user03"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(2));
          done();
        });
      });

      it("find({col: {$lt: value}}, callback)", function(done) {
        tbl.find({username: {$lt: "user03"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(0, 2));
          done();
        });
      });

      it("find({col: {$le: value}}, callback)", function(done) {
        tbl.find({username: {$le: "user03"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(0, 3));
          done();
        });
      });

      it("find({col: {$like: value}}, callback)", function(done) {
        tbl.find({username: {$like: "user_1"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([rows[0], rows[5]]);
          done();
        });
      });

      it("find({col: {$notLike: value}}, callback)", function(done) {
        tbl.find({username: {$notLike: "user_1"}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(1, 5).concat(rows.slice(6)));
          done();
        });
      });

      it("find({col: {$in: value}}, callback)", function(done) {
        tbl.find({username: {$in: ["user01", "user11"]}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([rows[0], rows[5]]);
          done();
        });
      });

      it("find({col: {$notIn: value}}, callback)", function(done) {
        tbl.find({username: {$notIn: ["user01", "user11"]}}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(rows.slice(1, 5).concat(rows.slice(6)));
          done();
        });
      });
    });

    describe("Compound", function() {
      it("find(filter, callback)", function(done) {
        tbl.find({username: {$like: "user_1"}, password: "pwd01"}, function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql([rows[0]]);
          done();
        });
      });
    });
  });

  describe("#findOne()", function() {
    describe("Error handling", function() {
      it("findOne()", function() {
        (function() {
          tbl.findOne();
        }).should.throwError("Callback expected.");
      });

      it("findOne(filter)", function() {
        (function() {
          tbl.findOne({});
        }).should.throwError("Callback expected.");
      });
    });

    it("findOne(filter, callback) - No rows matched", function(done) {
      tbl.findOne({username: "xxxxx"}, function(error, row) {
        should.assert(error === undefined);
        should.assert(row === undefined);
        done();
      });
    });

    it("findOne(callback)", function(done) {
      tbl.findOne(function(error, row) {
        should.assert(error === undefined);
        should.assert(row);
        done();
      });
    });

    it("findOne({}, callback)", function(done) {
      tbl.findOne({}, function(error, row) {
        should.assert(error === undefined);
        should.assert(row);
        done();
      });
    });

    describe("Simple", function() {
      it("findOne(filter, callback)", function(done) {
        tbl.findOne({username: "user03"}, function(error, row) {
          should.assert(error === undefined);
          row.should.be.eql(rows[2]);
          done();
        });
      });
    });

    describe("Compound", function() {
      it("findOne(filter, callback)", function(done) {
        tbl.findOne({username: {$like: "user_1"}, password: "pwd01"}, function(error, row) {
          should.assert(error === undefined);
          row.should.be.eql(rows[0]);
          done();
        });
      });
    });
  });

  describe("#mapAll()", function() {
    describe("Error handling", function() {
      it("mapAll()", function() {
        (function() {
          tbl.mapAll();
        }).should.throwError("Map expected.");
      });

      it("mapAll(map)", function() {
        (function() {
          tbl.mapAll({clss: User});
        }).should.throwError("Callback expected.");
      });
    });

    it("mapAll(map, callback)", function(done) {
      tbl.mapAll({clss: User}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);

        for (var i = 0; i < result.length; ++i) {
          var row = result.rows[i];
          row.should.be.instanceOf(User);
          row.should.have.properties(rows[i]);
        }

        done();
      });
    });
  });

  describe("#map()", function() {
    describe("Error handling", function() {
      it("map()", function() {
        (function() {
          tbl.map();
        }).should.throwError("Map expected.");
      });

      it("map(map)", function() {
        (function() {
          tbl.map({clss: User});
        }).should.throwError("Callback expected.");
      });

      it("map(map, filter)", function() {
        (function() {
          tbl.map({clss: User}, {});
        }).should.throwError("Callback expected.");
      });
    });

    it("map(map, callback)", function(done) {
      tbl.map({clss: User}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);

        for (var i = 0; i < result.length; ++i) {
          var row = result.rows[i];
          row.should.be.instanceOf(User);
          row.should.have.properties(rows[i]);
        }

        done();
      });
    });

    it("map(map, {}, callback)", function(done) {
      tbl.map({clss: User}, {}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);

        for (var i = 0; i < result.length; ++i) {
          var row = result.rows[i];
          row.should.be.instanceOf(User);
          row.should.have.properties(rows[i]);
        }

        done();
      });
    });

    it("map(map, filter, callback) - No rows matched", function(done) {
      tbl.map({clss: User}, {username: "xxxxxx"}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);
        result.length.should.be.eql(0);
        done();
      });
    });

    it("map(map, filter, callback)", function(done) {
      tbl.map({clss: User}, {username: {$ne: "user01"}}, function(error, result) {
        should.assert(error === undefined);
        result.should.be.instanceOf(vdba.Result);

        for (var i = 1; i < rows.length; ++i) {
          var row = result.rows[i-1];
          row.should.be.instanceOf(User);
          row.should.have.properties(rows[i]);
        }

        done();
      });
    });
  });

  describe("#mapOne()", function() {
    describe("Error handling", function() {
      it("mapOne()", function() {
        (function() {
          tbl.mapOne();
        }).should.throwError("Map expected.");
      });

      it("mapOne(map)", function() {
        (function() {
          tbl.mapOne({clss: User});
        }).should.throwError("Callback expected.");
      });

      it("mapOne(map, filter)", function() {
        (function() {
          tbl.mapOne({clss: User}, {});
        }).should.throwError("Callback expected.");
      });
    });

    it("mapOne(map, filter, callback) - No rows matched", function(done) {
      tbl.mapOne({clss: User}, {username: "xxxxx"}, function(error, row) {
        should.assert(error === undefined);
        should.assert(row === undefined);
        done();
      });
    });

    describe("Simple", function() {
      it("mapOne(map, filter, callback)", function(done) {
        tbl.mapOne({clss: User}, {username: "user03"}, function(error, row) {
          should.assert(error === undefined);
          row.should.be.instanceOf(User);
          row.should.have.properties(rows[2]);
          done();
        });
      });
    });

    describe("Compound", function() {
      it("mapOne(map, filter, callback)", function(done) {
        tbl.mapOne({clss: User}, {username: {$like: "user_1"}, password: "pwd01"}, function(error, row) {
          should.assert(error === undefined);
          row.should.be.instanceOf(User);
          row.should.have.properties(rows[0]);
          done();
        });
      });
    });
  });
});