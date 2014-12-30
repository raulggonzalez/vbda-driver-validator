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

  describe("Tables", function() {
    describe("DQL", function() {
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

    describe("DDL", function() {
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
    });
  });

  describe("Indexes", function() {
    describe("DQL", function() {
      beforeEach(function(done) {
        db.createTable(user.name, user.columns, done);
      });

      beforeEach(function(done) {
        db.createIndex(user.name, user.index.name, user.index.columns, done);
      });

      afterEach(function(done) {
        db.dropTable(user.name, done);
      });

      describe("#hasIndex()", function() {
        describe("Error handling", function() {
          it("hasIndex()", function() {
            (function() {
              db.hasIndex();
            }).should.throwError("Index expected.");
          });

          it("hasIndex(name)", function() {
            (function() {
              db.hasIndex(user.index.name);
            }).should.throwError("Callback expected.");
          });
        });

        it("hasIndex(name, callback)", function(done) {
          db.hasIndex(user.index.name, function(error, exists) {
            should.assert(error === undefined);
            exists.should.be.eql(true);
            done();
          });
        });

        it("hasIndex('ix_unknown', callback)", function(done) {
          db.hasIndex("ix_unknown", function(error, exists) {
            should.assert(error === undefined);
            exists.should.be.eql(false);
            done();
          });
        });
      });

      describe("#findIndex()", function() {
        describe("Error handling", function() {
          it("findIndex()", function() {
            (function() {
              db.findIndex();
            }).should.throwError("Index name expected.");
          });

          it("findIndex(name)", function() {
            (function() {
              db.findIndex(user.index.name);
            }).should.throwError("Callback expected.");
          });
        });

        it("findIndex(name, callback)", function(done) {
          db.findIndex(user.index.name, function(error, ix) {
            should.assert(error === undefined);
            ix.should.be.instanceOf(vdba.Index);
            done();
          });
        });

        it("findIndex('ix_unknown', callback)", function(done) {
          db.findIndex("ix_unknown", function(error, ix) {
            should.assert(error === undefined);
            should.assert(ix === undefined);
            done();
          });
        });
      });
    });

    describe("DDL", function() {
      describe("#createIndex()", function() {
        beforeEach(function(done) {
          db.createTable(user.name, user.columns, done);
        });

        afterEach(function(done) {
          db.dropTable(user.name, done);
        });

        describe("Error handling", function() {
          beforeEach(function(done) {
            db.createIndex(user.name, user.index.name, user.index.columns, done);
          });

          it("createIndex()", function() {
            (function() {
              db.createIndex();
            }).should.throwError("Table expected.");
          });

          it("createIndex(table)", function() {
            (function() {
              db.createIndex(user.name);
            }).should.throwError("Index name expected.");
          });

          it("createIndex(table, index)", function() {
            (function() {
              db.createIndex(user.name, user.index.name);
            }).should.throwError(/^Indexing column(\(s\))? expected.$/)
          });

          it("createIndex(table, index, columns, callback) - Existing", function(done) {
            db.createIndex(user.name, user.index.name, user.index.columns, function(error) {
              error.should.be.instanceOf(Error);
              done();
            });
          });
        });

        it("createIndex(table, index, columns)", function(done) {
          db.createIndex(user.name, user.index.name, user.index.columns);

          setTimeout(function() {
            db.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          }, 500);
        });

        it("createIndex(table, index, columns, options)", function(done) {
          db.createIndex(user.name, user.index.name, user.index.columns, user.index.options);

          setTimeout(function() {
            db.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          }, 500);
        });

        it("createIndex(table, index, columns, callback)", function(done) {
          db.createIndex(user.name, user.index.name, user.index.columns, function(error) {
            should.assert(error === undefined);

            db.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });
        });

        it("createIndex(table, index, columns, options, callback)", function(done) {
          db.createIndex(user.name, user.index.name, user.index.columns, user.index.options, function(error) {
            should.assert(error === undefined);

            db.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });
        });
      });

      describe("#dropIndex()", function() {
        beforeEach(function(done) {
          db.createTable(user.name, user.columns, done);
        });

        beforeEach(function(done) {
          db.createIndex(user.name, user.index.name, user.index.columns, done);
        });

        afterEach(function(done) {
          db.dropTable(user.name, done);
        });

        describe("Error handling", function() {
          it("dropIndex()", function() {
            (function() {
              db.dropIndex();
            }).should.throwError("Index expected.");
          });
        });

        it("dropIndex(name)", function(done) {
          db.dropIndex(user.index.name);

          setTimeout(function() {
            db.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          }, 500);
        });

        it("dropIndex('ix_unknown')", function(done) {
          db.dropIndex("ix_unknown");
          setTimeout(done, 500);
        });

        it("dropIndex(name, callback)", function(done) {
          db.dropIndex(user.index.name, function(error) {
            should.assert(error === undefined);

            db.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });
        });

        it("dropIndex('ix_unknown', callback)", function(done) {
          db.dropIndex("ix_unknown", function(error) {
            should.assert(error === undefined);
            done();
          });
        });
      });
    });
  });
});