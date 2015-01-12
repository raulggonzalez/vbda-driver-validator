describe("vdba.<driver>.Table (DML)", function() {
  var drv, cx, db, u, user, users, s, session, sessions, p, profile, profiles;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
    user = config.database.user;
    users = user.rowsWithId;
    profile = config.database.profile;
    profiles = profile.rows;
    session = config.database.session;
    sessions = session.rowsWithId;
  });

  beforeEach(function(done) {
    drv.openConnection(config.connection.config, function(error, con) {
      cx = con;
      db = cx.database;
      done();
    });
  });

  beforeEach(function(done) {
    db.createTable(user.schema, user.name, user.columns, user.options, done);
  });

  beforeEach(function(done) {
    db.createTable(profile.schema, profile.name, profile.columns, profile.options, done);
  });

  beforeEach(function(done) {
    db.createTable(session.schema, session.name, session.columns, session.options, done);
  });

  beforeEach(function(done) {
    db.findTable(user.schema, user.name, function(error, tab) {
      u = tab;
      done();
    });
  });

  beforeEach(function(done) {
    db.findTable(profile.schema, profile.name, function(error, tab) {
      p = tab;
      done();
    });
  });

  beforeEach(function(done) {
    db.findTable(session.schema, session.name, function(error, tab) {
      s = tab;
      done();
    });
  });

  afterEach(function(done) {
    db.dropTable(profile.schema, profile.name, done);
  });

  afterEach(function(done) {
    db.dropTable(user.schema, user.name, done);
  });

  afterEach(function(done) {
    db.dropTable(session.schema, session.name, done);
  });

  afterEach(function(done) {
    cx.close(done);
  });

  describe("#insert()", function() {
    describe("Error handling", function() {
      it("insert()", function() {
        (function() {
          u.insert();
        }).should.throwError("Row(s) expected.");
      });
    });

    describe("One row", function() {
      it("insert(row)", function(done) {
        u.insert(users[0]);

        setTimeout(function() {
          u.count(function(error, count) {
            should.assert(error === undefined);
            count.should.be.eql(1);
            done();
          });
        }, 500);
      });

      it("insert(row, callback)", function(done) {
        u.insert(users[0], function(error) {
          should.assert(error === undefined);

          u.count(function(error, count) {
            should.assert(error === undefined);
            count.should.be.eql(1);
            done();
          });
        });
      });

      it("insert(row, callback) - With set<integer> column", function(done) {
        s.insert(sessions[0], function(error) {
          should.assert(error === undefined);

          s.findAll(function(error, result) {
            should.assert(error === undefined);
            result.rows.should.be.eql([sessions[0]]);
            done();
          });
        });
      });
    });

    describe("Several rows", function() {
      it("insert([], callback)", function(done) {
        u.insert([], function(error) {
          should.assert(error === undefined);

          u.count(function(error, count) {
            should.assert(error === undefined);
            count.should.be.eql(0);
            done();
          });
        });
      });

      it("insert(rows, callback)", function(done) {
        u.insert(users, function(error) {
          should.assert(error === undefined);

          u.count(function(error, count) {
            should.assert(error === undefined);
            count.should.be.eql(users.length);
            done();
          });
        });
      });
    });
  });

  describe("#truncate()", function() {
    beforeEach(function(done) {
      u.insert(users, done);
    });

    it("truncate()", function(done) {
      u.truncate();

      setTimeout(function() {
        u.count(function(error, count) {
          should.assert(error === undefined);
          count.should.be.eql(0);
          done();
        });
      }, 500);
    });

    it("truncate(callback)", function(done) {
      u.truncate(function(error) {
        should.assert(error === undefined);

        u.count(function(error, count) {
          should.assert(error === undefined);
          count.should.be.eql(0);
          done();
        });
      });
    });
  });

  describe("#remove()", function() {
    beforeEach(function(done) {
      u.insert(users, done);
    });

    it("remove()", function(done) {
      u.remove();

      setTimeout(function() {
        u.count(function(error, count) {
          should.assert(error === undefined);
          count.should.be.eql(0);
          done();
        });
      }, 500);
    });

    it("remove(callback)", function(done) {
      u.remove(function(error) {
        should.assert(error === undefined);

        u.count(function(error, count) {
          should.assert(error === undefined);
          count.should.be.eql(0);
          done();
        });
      });
    });

    it("remove(filter)", function(done) {
      u.remove({username: "user01"});

      setTimeout(function() {
        u.count(function(error, count) {
          should.assert(error === undefined);
          count.should.be.eql(users.length-1);
          done();
        });
      }, 500);
    });

    it("remove(filter, callback)", function(done) {
      u.remove({username: "user01"}, function(error) {
        should.assert(error === undefined);

        u.count(function(error, count) {
          should.assert(error === undefined);
          count.should.be.eql(users.length-1);
          done();
        });
      });
    });
  });
});