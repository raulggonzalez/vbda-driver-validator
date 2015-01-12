describe("vdba.<driver>.Table (DML - UPDATE)", function() {
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

    describe("text", function() {
      describe("$set", function() {
        it("update(filter, {column: value}, callback)", function(done) {
          u.update({userId: 1}, {password: "pwd"}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([updatedRows[0]]);
              done();
            });
          });
        });

        it("update(filter, {column: {$set: value}}}, callback)", function(done) {
          u.update({userId: 2}, {password: {$set: "pwd"}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 2}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([updatedRows[1]]);
              done();
            });
          });
        });
      });

      describe("$add", function() {
        it("update(filter, {column: {$add: value}}, callback)", function(done) {
          var row = util._extend({}, users[0]);
          row.username += "xxxxx";

          u.update({userId: 1}, {username: {$add: "xxxxx"}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });
    });

    describe("integer", function() {
      describe("$set", function() {
        it("update(filter, {column: value}, callback)", function(done) {
          var row = util._extend({}, users[0]);
          row.userId = 111;

          u.update({userId: 1}, {userId: 111}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 111}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });

        it("update(filter, {column: {$set: value}}, callback)", function(done) {
          var row = util._extend({}, users[0]);
          row.userId = 121;

          u.update({userId: 1}, {userId: {$set: 121}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 121}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$add", function() {
        it("update(filter, {column: {$add: value}}, callback)", function(done) {
          var row = util._extend({}, users[0]);
          row.userId = 101;

          u.update({userId: 1}, {userId: {$add: 100}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 101}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$inc", function() {
        it("update(filter, {column: {$inc: value}}, callback)", function(done) {
          var row = util._extend({}, users[0]);
          row.userId = 101;

          u.update({userId: 1}, {userId: {$inc: 100}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 101}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$dec", function() {
        it("update(filter, {column: {$dec: value}}, callback)", function(done) {
          var row = util._extend({}, users[0]);
          row.userId = -1;

          u.update({userId: 1}, {userId: {$dec: 2}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: -1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$mul", function() {
        it("update(filter, {column: {$mul: integer}}, callback)", function(done) {
          var row = util._extend({}, users[1]);
          row.userId = 200;

          u.update({userId: 2}, {userId: {$mul: 100}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 200}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });

        it("update(filter, {column: {$mul: real}}, callback)", function(done) {
          var row = util._extend({}, users[1]);
          row.userId = 200;

          u.update({userId: 2}, {userId: {$mul: 100.3}}, function(error) {
            should.assert(error === undefined);

            u.find({userId: 200}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });
    });

    describe("real", function() {
      describe("$set", function() {
        it("update(filter, {column: value}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes += 1;

          s.update({sessionId: 1}, {minutes: row.minutes}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });

        it("update(filter, {column: {$set: value}}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes += 1;

          s.update({sessionId: 1}, {minutes: {$set: row.minutes}}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$add", function() {
        it("update(filter, {column: {$add: value}}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes += 100;

          s.update({sessionId: 1}, {minutes: {$add: 100}}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$inc", function() {
        it("update(filter, {column: {$inc: value}}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes += 101;

          s.update({sessionId: 1}, {minutes: {$inc: 101}}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$dec", function() {
        it("update(filter, {column: {$dec: value}}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes -= 1;

          s.update({sessionId: 1}, {minutes: {$dec: 1}}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });

      describe("$mul", function() {
        it("update(filter, {column: {$mul: integer}}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes *= 200;

          s.update({sessionId: 1}, {minutes: {$mul: 200}}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });

        it("update(filter, {column: {$mul: real}}, callback)", function(done) {
          var row = util._extend({}, sessions[0]);
          row.minutes *= 200.25;

          s.update({sessionId: 1}, {minutes: {$mul: 200.25}}, function(error) {
            should.assert(error === undefined);

            s.find({sessionId: 1}, function(error, result) {
              should.assert(error === undefined);
              result.rows.should.be.eql([row]);
              done();
            });
          });
        });
      });
    });

    describe("set<integer>", function() {
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

      describe("$del", function() {
        it("update(filter, {column: {$del: value}}, callback) - The first one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$del: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([2, 3, 4, 5]);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - A middle one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$del: 3}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 4, 5]);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - The last one", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$del: 5}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 3, 4]);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - Unknown", function(done) {
          s.update({sessionId: 1}, {clickedArticles: {$del: 12345}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([1, 2, 3, 4, 5]);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - The unique one", function(done) {
          s.update({sessionId: 2}, {clickedArticles: {$del: 1}}, function(error) {
            should.assert(error === undefined);

            s.findOne({sessionId: 2}, function(error, row) {
              should.assert(error === undefined);
              row.clickedArticles.should.be.eql([]);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - If null", function(done) {
          s.update({sessionId: 4}, {clickedArticles: {$del: 1}}, function(error) {
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

    describe("set<text>", function() {
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

      describe("$del", function() {
        it("update(filter, {column: {$del: value}}, callback) - The first one", function(done) {
          p.update({userId: 1}, {emails: {$del: profiles[0].emails[0]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails.slice(1));
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - A middle one", function(done) {
          p.update({userId: 1}, {emails: {$del: profiles[0].emails[1]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql([profiles[0].emails[0]].concat(profiles[0].emails[2]));
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - The last one", function(done) {
          p.update({userId: 1}, {emails: {$del: profiles[0].emails[2]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails.slice(0, 2));
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - Unknown", function(done) {
          p.update({userId: 1}, {emails: {$del: "unknown@test.com"}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 1}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql(profiles[0].emails);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - The unique one", function(done) {
          p.update({userId: 2}, {emails: {$del: profiles[1].emails[0]}}, function(error) {
            should.assert(error === undefined);

            p.findOne({userId: 2}, function(error, row) {
              should.assert(error === undefined);
              row.emails.should.be.eql([]);
              done();
            });
          });
        });

        it("update(filter, {column: {$del: value}}, callback) - If null", function(done) {
          p.update({userId: 7}, {emails: {$del: "user07@test.com"}}, function(error) {
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
});