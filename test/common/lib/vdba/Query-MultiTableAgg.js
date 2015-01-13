describe("vdba.<driver>.Query (Multi Table Aggregation Query)", function() {
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

  beforeEach(function() {
    q = u.query();
    q.join("session", "userId");
    q.group(["userId", "username"]);
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

  describe("One aggregation", function() {
    it("count(callback)", function(done) {
      q.count(function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([
          {userId: 1, username: "user01", count: 2},
          {userId: 2, username: "user02", count: 1},
          {userId: 3, username: "user03", count: 1}
        ]);
        done();
      });
    });
  });

  describe("Several aggregations", function() {
    it("count().sum(column).find(callback)", function(done) {
      q.count().sum("minutes").find(function(error, result) {
        should.assert(error === undefined);
        result.rows.should.be.eql([
          {userId: 1, username: "user01", count: 2, sum: 10.45},
          {userId: 2, username: "user02", count: 1, sum: 1.13},
          {userId: 3, username: "user03", count: 1, sum: null}
        ]);
        done();
      });
    });
  });
});