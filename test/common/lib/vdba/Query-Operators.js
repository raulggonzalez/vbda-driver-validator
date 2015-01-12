describe("vdba.<driver>.Query (Operators)", function() {
  var user = config.database.user;
  var users = user.rowsWithId;
  var profile = config.database.profile;
  var profiles = profile.rows;
  var session = config.database.session;
  var sessions  = session.rowsWithId;
  var drv, cx, db, p, s, u, q;

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
    db.createTable(profile.schema, profile.name, profile.columns, done);
  });

  before(function(done) {
    db.findTable(user.schema, user.name, function(error, table) {
      u = table;
      done();
    });
  });

  before(function(done) {
    db.findTable(profile.schema, profile.name, function(error, table) {
      p = table;
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

  before(function(done) {
    p.insert(profiles, done);
  });

  beforeEach(function() {
    q = p.query();
  });

  after(function(done) {
    db.dropTable(profile.schema, profile.name, done);
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

  describe("$contains", function() {
    it("$contains: text", function(done) {
      p.find({emails: {$contains: "user01@test.com"}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([profiles[0]]);
        done();
      });
    });

    it("$contains: integer", function(done) {
      s.find({clickedArticles: {$contains: 1}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql(sessions.slice(0, 2));
        done();
      });
    });
  });

  describe("$ncontains", function() {
    it("$ncontains: text", function(done) {
      p.find({emails: {$ncontains: "user01@test.com"}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql(profiles.slice(1));
        done();
      });
    });

    it("$ncontains: integer", function(done) {
      s.find({clickedArticles: {$ncontains: 1}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql(sessions.slice(2));
        done();
      });
    });
  });

  describe("$notContains", function() {
    it("$notContains: text", function(done) {
      p.find({emails: {$notContains: "user01@test.com"}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql(profiles.slice(1));
        done();
      });
    });

    it("$notContains: integer", function(done) {
      s.find({clickedArticles: {$notContains: 1}}, function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql(sessions.slice(2));
        done();
      });
    });
  });
});