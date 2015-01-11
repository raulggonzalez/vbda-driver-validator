describe("vdba.<driver>.Query (Multi table)", function() {
  var user = config.database.user;
  var users = user.rowsWithId;
  var session = config.database.session;
  var sessions = session.rowsWithId;
  var profile = config.database.profile;
  var profiles = profile.rows;
  var drv, cx, db, u, s, p, q;

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
    db.createTable(session.schema, session.name, session.columns, done);
  });

  beforeEach(function(done) {
    db.createTable(profile.schema, profile.name, profile.columns, done);
  });

  beforeEach(function(done) {
    db.findTable(user.schema, user.name, function(error, table) {
      u = table;
      done();
    });
  });

  beforeEach(function(done) {
    db.findTable(session.schema, session.name, function(error, table) {
      s = table;
      done();
    });
  });

  beforeEach(function(done) {
    db.findTable(profile.schema, profile.name, function(error, table) {
      p = table;
      done();
    });
  });

  beforeEach(function(done) {
    u.insert(users, done);
  });

  beforeEach(function(done) {
    s.insert(sessions, done);
  });

  beforeEach(function(done) {
    p.insert(profiles, done);
  });

  beforeEach(function() {
    q = u.query();
  });

  afterEach(function(done) {
    db.dropTable(user.schema, user.name, done);
  });

  afterEach(function(done) {
    db.dropTable(session.schema, session.name, done);
  });

  afterEach(function(done) {
    db.dropTable(profile.schema, profile.name, done);
  });

  afterEach(function(done) {
    cx.close(done);
  });

  describe("#join()", function() {
    describe("Error handling", function() {
      it("join()", function() {
        (function() {
          q.join();
        }).should.throwError("Target table expected.");
      });

      it("join(table)", function() {
        (function() {
          q.join(session.qn);
        }).should.throwError("Source column name expected.");
      });
    });

    describe("No callback", function() {
      it("join(table, column)", function() {
        q.join(session.qn, "userId").should.be.exactly(q);
        q.joins[0].should.have.properties({
          mode: "none",
          type: "inner",
          target: session.qn,
          sourceColumn: "userId",
          targetColumn: "userId"
        });
      });

      it("join(table, column, column)", function() {
        q.join(session.qn, "userId", "user").should.be.exactly(q);
        q.joins[0].should.have.properties({
          mode: "none",
          type: "inner",
          target: session.qn,
          sourceColumn: "userId",
          targetColumn: "user"
        });
      });
    });

    describe("With callback", function() {
      it("join(table, column, callback)", function(done) {
        q.join(session.qn, "userId", function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(config.ops.joinUserSession);
          done();
        });
      });

      it("join(table, column, column, callback)", function(done) {
        q.join(session.qn, "userId", "userId", function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(config.ops.joinUserSession);
          done();
        });
      });
    });

    describe("With limit", function() {
      it("join(table, column).limit(count).find(callback)", function(done) {
        q.join(session.qn, "userId").limit(2).find(function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(config.ops.joinUserSession.slice(0, 2));
          done();
        });
      });
    });
  });

  describe("#joinoo()", function() {
    describe("Error handling", function() {
      it("joinoo()", function() {
        (function() {
          q.joinoo();
        }).should.throwError("Target table expected.");
      });

      it("joinoo(table)", function() {
        (function() {
          q.joinoo(session.qn);
        }).should.throwError("Source column name expected.");
      });
    });

    describe("No callback", function() {
      it("joinoo(table, column)", function() {
        q.joinoo(session.qn, "userId").should.be.exactly(q);
        q.joins[0].should.have.properties({
          mode: "1-1",
          type: "inner",
          target: session.qn,
          sourceColumn: "userId",
          targetColumn: "userId"
        });
      });

      it("joinoo(table, column, column)", function() {
        q.joinoo(session.qn, "userId", "user").should.be.exactly(q);
        q.joins[0].should.have.properties({
          mode: "1-1",
          type: "inner",
          target: session.qn,
          sourceColumn: "userId",
          targetColumn: "user"
        });
      });
    });

    describe("With callback", function() {
      it("joinoo(table, column, callback)", function(done) {
        q.joinoo(profile.qn , "userId", function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(config.ops.joinooUserProfile);
          done();
        });
      });

      it("joinoo(table, column, column, callback)", function(done) {
        q.joinoo(profile.qn, "userId", "userId", function(error, result) {
          should.assert(error === undefined);
          result.should.be.instanceOf(vdba.Result);
          result.rows.should.be.eql(config.ops.joinooUserProfile);
          done();
        });
      });
    });
  });
});