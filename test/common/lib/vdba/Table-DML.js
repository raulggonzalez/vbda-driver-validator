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

  describe("#update()", function() {
    var updatedRows;

    beforeEach(function() {
      updatedRows = [];

      for (var i = 0; i < users.length; ++i) {
        var item = util._extend({}, users[i]);
        item.password = "pwd";
        updatedRows.push(item);
      }
    });

    beforeEach(function(done) {
      u.insert(users, done);
    });

    beforeEach(function(done) {
      p.insert(profiles, done);
    });

    beforeEach(function(done) {
      s.insert(sessions, done);
    });

    describe("Error handling", function() {
      it("update()", function() {
        (function() {
          u.update();
        }).should.throwError("Column(s) to update expected.");
      });

      it("update(callback)", function() {
        (function() {
          u.update(function() {});
        }).should.throwError("Column(s) to update expected.");
      });
    });

    it("update(columns)", function(done) {
      u.update({password: "pwd"});

      setTimeout(function() {
        u.findAll(function(error, result) {
          should.assert(error === undefined);
          result.rows.should.be.eql(updatedRows);
          done();
        });
      }, 500);
    });

    it("update(columns, callback)", function(done) {
      u.update({password: "pwd"}, function(error) {
        should.assert(error === undefined);

        u.findAll(function(error, result) {
          should.assert(error === undefined);
          result.rows.should.be.eql(updatedRows);
          done();
        });
      });
    });

    describe("Update set<integer>", function() {
      describe("$set", function() {
        it("update(filter, {column: null}, callback)", function(done) {
          s.update({sessionId: 1}, {clickedArticles: null}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              should.assert(row.clickedArticles === null);
              done();
            });
          });
        });

        it("update(filter, {column: undefined}, callback)", function(done) {
          s.update({sessionId: 1}, {clickedArticles: undefined}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              should.assert(row.clickedArticles === null);
              done();
            });
          });
        });

        it("update(filter, {column: []}, callback)", function(done) {
          s.update({sessionId: 1}, {clickedArticles: []}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([]);
              done();
            });
          });
        });

        it("update(filter, {column: [...], callback)", function(done) {
          s.update({sessionId: 1}, {clickedArticles: [5, 4, 3, 2, 1]}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([5, 4, 3, 2, 1]);
              done();
            });
          });
        });

        it("update(filter, {column: {$set: []}}, callback)", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$set: []}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([]);
              done();
            });
          });
        });
      });

      describe("$add", function() {
        it("update(filter, {column: {$add: value}}, callback) - The first one with null array", function(done) {
          s.update({sessionId: 4}, {clickedArticles: {$add: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 4}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1]);
              done();
            });
          });
        });

        it("update(filter, {column: {$add: value}}, callback) - The first one with empty array", function(done) {
          s.update({sessionId: 3}, {clickedArticles: {$add: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 3}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1]);
              done();
            });
          });
        });


        it("update(filter, {column: {$add: value}}, callback) - The last one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$add: 6}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 3, 4, 5, 6]);
              done();
            });
          });
        });

        it("update(filter, {column: {$add: value}}, callback) - Value arleady exists", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$add: 3}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 3, 4, 5]);
              done();
            });
          });
        });
      });

      describe("$remove", function() {
        it("update(filter, {column: {$remove: value}}, callback) - The first one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$remove: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([2, 3, 4, 5]);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - A middle one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$remove: 3}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 4, 5]);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - The last one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$remove: 5}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 3, 4]);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - Unknown", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$remove: 12345}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 3, 4, 5]);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - The unique one", function(done) {
          s.update({sessionId: 2}, {clickedArticles: {$remove: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 2}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([]);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - If null", function(done) {
          s.update({sessionId: 4}, {clickedArticles: {$remove: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 4}, function(error, row) {
              should.assert(error === undefined);
              should.assert(row.clickedArticles === null);
              done();
            });
          });
        });
      });
    });

    describe("Update set<text>", function() {
      describe("$set", function() {
        it("update(filter, {column: null}, callback)", function(done) {
          p.update({userId: 6}, {emails: null}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 6}, function(error, row) {
              should.assert(error === undefined);
              should.assert(row.emails === null);
              done();
            });
          });
        });

        it("update(filter, {column: undefined}, callback)", function(done) {
          p.update({userId: 1}, {emails: undefined}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              should.assert(row.emails === null);
              done();
            });
          });
        });

        it("update(filter, {column: []}, callback)", function(done) {
          p.update({userId: 1}, {emails: []}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql([]);
              done();
            });
          });
        });

        it("update(filter, {column: [...], callback)", function(done) {
          p.update({userId: 1}, {emails: ["o@t.c", "t@t.c"]}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(["o@t.c", "t@t.c"]);
              done();
            });
          });
        });

        it("update(filter, {column: {$set: []}}, callback)", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$set: []}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([]);
              done();
            });
          });
        });
      });

      describe("$add", function() {
        it("update(filter, {column: {$add: value}}, callback) - The first one with null array", function(done) {
          p.update({userId: 7}, {emails: {$add: "u07@test.com"}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 7}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(["u07@test.com"]);
              done();
            });
          });
        });

        it("update(filter, {column: {$add: value}}, callback) - The first one with empty array", function(done) {
          p.update({userId: 4}, {emails: {$add: "u07@test.com"}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 4}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(["u07@test.com"]);
              done();
            });
          });
        });


        it("update(filter, {column: {$add: value}}, callback) - The last one", function(done) {
          p.update({userId: 1}, {emails: {$add: "new@test.com"}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails.concat(["new@test.com"]));
              done();
            });
          });
        });

        it("update(filter, {column: {$add: value}}, callback) - Value arleady exists", function(done) {
          p.update({userId: 1}, {emails: {$add: profiles[0].emails[0]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails);
              done();
            });
          });
        });
      });

      describe("$remove", function() {
        it("update(filter, {column: {$remove: value}}, callback) - The first one", function(done) {
          p.update({userId: 1}, {emails: {$remove: profiles[0].emails[0]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails.slice(1));
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - A middle one", function(done) {
          p.update({userId: 1}, {emails: {$remove: profiles[0].emails[1]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql([profiles[0].emails[0]].concat(profiles[0].emails[2]));
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - The last one", function(done) {
          p.update({userId: 1}, {emails: {$remove: profiles[0].emails[2]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails.slice(0, 2));
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - Unknown", function(done) {
          p.update({userId: 1}, {emails: {$remove: "unknown@test.com"}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - The unique one", function(done) {
          p.update({userId: 2}, {emails: {$remove: profiles[1].emails[0]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 2}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql([]);
              done();
            });
          });
        });

        it("update(filter, {column: {$remove: value}}, callback) - If null", function(done) {
          p.update({userId: 7}, {emails: {$remove: "user07@test.com"}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 7}, function(error, row) {
              should.assert(error === undefined);
              should.assert(row.emails === null);
              done();
            });
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