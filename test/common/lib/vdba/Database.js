describe("vdba.<driver>.Database", function() {
  var user = config.database.user;
  var session = config.database.session;
  var drv, cx, db;

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

  afterEach(function(done) {
    cx.close(done);
  });

  describe("#createTable()", function() {
    afterEach(function(done) {
      db.dropTable(user.name, done);
    });

    describe("Error handling", function() {
      it("createTable()", function() {
        (function() {
          db.createTable();
        }).should.throwError("Table name expected.");
      });
    });

    it("createTable(name, columns)", function(done) {
      db.createTable(user.name, user.columns);

      setTimeout(function() {
        db.hasTable(user.name, function(error, exists) {
          should.assert(error === undefined);
          exists.should.be.eql(true);
          done();
        });
      }, 500);
    });

    it("createTable(name, columns, callback)", function(done) {
      db.createTable(user.name, user.columns, function(error) {
        should.assert(error === undefined);

        db.hasTable(user.name, function(error, exists) {
          should.assert(error === undefined);
          exists.should.be.eql(true);
          done();
        })
      });
    });

    it("createTable(name, columns, callback) - Existing table", function(done) {
      db.createTable(user.name, user.columns, function(error) {
        should.assert(error === undefined);

        db.createTable(user.name, user.columns, function(error) {
          error.should.be.instanceOf(Error);
          done();
        });
      });
    });
  });

  describe("#dropTable()", function() {
    beforeEach(function(done) {
      db.createTable(user.name, user.columns, done);
    });

    afterEach(function(done) {
      db.dropTable(user.name, done);
    });

    describe("Error handling", function() {
      it("dropTable()", function() {
        (function() {
          db.dropTable();
        }).should.throwError("Table name expected.");
      });
    });

    it("dropTable(name)", function(done) {
      db.dropTable(user.name);

      setTimeout(function() {
        db.hasTable(user.name, function(error, exists) {
          should.assert(error === undefined);
          exists.should.be.eql(false);
          done();
        });
      }, 500);
    });

    it("dropTable('Unknown')", function(done) {
      db.dropTable("Unknown");
      done();
    });

    it("dropTable(name, callback)", function(done) {
      db.dropTable(user.name, function(error) {
        should.assert(error === undefined);

        db.hasTable(user.name, function(error, exists) {
          should.assert(error === undefined);
          exists.should.be.eql(false);
          done();
        });
      });
    });

    it("dropTable('Unknown', callback)", function(done) {
      db.dropTable("Unknown", function(error) {
        should.assert(error === undefined);
        done();
      });
    });
  });

  describe("#findTable()", function() {
    beforeEach(function(done) {
      db.createTable(user.name, user.columns, done);
    });

    afterEach(function(done) {
      db.dropTable(user.name, done);
    });

    describe("Error handling", function() {
      it("findTable()", function() {
        (function() {
          db.findTable();
        }).should.throwError("Table name expected.");
      });

      it("findTable(name)", function() {
        (function() {
          db.findTable(user.name);
        }).should.throwError("Callback expected.");
      });
    });

    it("findTable(name, callback)", function(done) {
      db.findTable(user.name, function(error, tab) {
        should.assert(error === undefined);
        tab.should.be.instanceOf(vdba.Table);
        tab.database.should.be.exactly(db);
        tab.name.should.be.eql(user.name);
        done();
      });
    });

    it("findTable('Unknown', callback)", function(done) {
      db.findTable("Unknown", function(error, tab) {
        should.assert(error === undefined);
        should.assert(tab === undefined);
        done();
      });
    });
  });

  describe("#hasTable()", function() {
    beforeEach(function(done) {
      db.createTable(user.name, user.columns, done);
    });

    afterEach(function(done) {
      db.dropTable(user.name, done);
    });

    describe("Error handling", function() {
      it("hasTable()", function() {
        (function() {
          db.hasTable();
        }).should.throwError("Table name expected.");
      });

      it("hasTable(name)", function() {
        (function() {
          db.hasTable(user.name);
        }).should.throwError("Callback expected.");
      });
    });

    it("hasTable(name, callback)", function(done) {
      db.hasTable(user.name, function(error, exists) {
        should.assert(error === undefined);
        exists.should.be.eql(true);
        done();
      });
    });

    it("hasTable('Unknown', callback)", function(done) {
      db.hasTable("Unknown", function(error, exists) {
        should.assert(error === undefined);
        exists.should.be.eql(false);
        done();
      });
    });
  });

  describe("#hasTables()", function() {
    beforeEach(function(done) {
      db.createTable(user.name, user.columns, done);
    });

    beforeEach(function(done) {
      db.createTable(session.name, session.columns, done);
    });

    afterEach(function(done) {
      db.dropTable(user.name, done);
    });

    afterEach(function(done) {
      db.dropTable(session.name, done);
    });

    describe("Error handling", function() {
      it("hasTables()", function() {
        (function() {
          db.hasTables();
        }).should.throwError("Table names expected.");
      });

      it("hasTables([])", function() {
        (function() {
          db.hasTables([]);
        }).should.throwError("Table names expected.");
      });

      it("hasTables(tables)", function() {
        (function() {
          db.hasTables([user.name, session.name]);
        }).should.throwError("Callback expected.");
      });
    });

    it("hasTables([e], callback)", function(done) {
      db.hasTables([user.name], function(error, exist) {
        should.assert(error === undefined);
        exist.should.be.eql(true);
        done();
      });
    });

    it("hasTables([u], callback)", function(done) {
      db.hasTables(["Unknown"], function(error, exist) {
        should.assert(error === undefined);
        exist.should.be.eql(false);
        done();
      });
    });

    it("hasTables([e, e], callback)", function(done) {
      db.hasTables([user.name, session.name], function(error, exist) {
        should.assert(error === undefined);
        exist.should.be.eql(true);
        done();
      });
    });

    it("hasTables([e, u], callback)", function(done) {
      db.hasTables([user.name, "Unknown"], function(error, exist) {
        should.assert(error === undefined);
        exist.should.be.eql(false);
        done();
      });
    });

    it("hasTables([u, u], callback)", function(done) {
      db.hasTables(["Unknown1", "Unknown2"], function(error, exist) {
        should.assert(error === undefined);
        exist.should.be.eql(false);
        done();
      });
    });

    it("hasTables([u, e], callback)", function(done) {
      db.hasTables(["Unknown", session.name], function(error, exist) {
        should.assert(error === undefined);
        exist.should.be.eql(false);
        done();
      });
    });
  });
});