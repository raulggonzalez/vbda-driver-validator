describe("vdba.<driver>.Table", function() {
  var user = config.database.user;
  var drv, cx, db, tbl;

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
    db.findTable(user.schema, user.name, function(error, tab) {
      tbl = tab;
      done();
    });
  });

  afterEach(function(done) {
    db.dropTable(user.schema, user.name, done);
  });

  describe("Indexes", function() {
    describe("Type 2 Driver", function() {
      describe("DQL", function() {
        beforeEach(function(done) {
          db.createIndex(user.schema, user.name, user.index.name, user.index.columns, done);
        });

        describe("#hasIndex()", function() {
          describe("Error handling", function() {
            it("hasIndex()", function() {
              (function() {
                tbl.hasIndex();
              }).should.throwError("Index name expected.");
            });

            it("hasIndex(name)", function() {
              (function() {
                tbl.hasIndex(user.index.name);
              }).should.throwError("Callback expected.");
            });
          });

          it("hasIndex(name, callback)", function(done) {
            tbl.hasIndex(user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasIndex('unknown', callback)", function(done) {
            tbl.hasIndex("unknown", function(error, exists) {
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
                tbl.findIndex();
              }).should.throwError("Index name expected.");
            });

            it("findIndex(name)", function() {
              (function() {
                tbl.findIndex(user.index.name);
              }).should.throwError("Callback expected.");
            });
          });

          it("findIndex(name, callback)", function(done) {
            tbl.findIndex(user.index.name, function(error, ix) {
              should.assert(error === undefined);
              ix.should.be.instanceOf(vdba.Index);
              done();
            });
          });

          it("findIndex('ix_unknown', callback)", function(done) {
            tbl.findIndex("ix_unknown", function(error, ix) {
              should.assert(error === undefined);
              should.assert(ix === undefined);
              done();
            });
          });
        });
      });

      describe("DDL", function() {
        describe("#createIndex()", function() {
          describe("Error handling", function() {
            beforeEach(function(done) {
              tbl.createIndex(user.index.name, user.index.columns, done);
            });

            it("createIndex()", function() {
              (function() {
                tbl.createIndex();
              }).should.throwError("Index name expected.");
            });

            it("createIndex(name)", function() {
              (function() {
                tbl.createIndex(user.index.name);
              }).should.throwError(/^Indexing column(\(s\))? expected.$/);
            });

            it("createIndex(name, columns, callback) - Existing", function(done) {
              tbl.createIndex(user.index.name, user.index.columns, function(error) {
                error.should.be.instanceOf(Error);
                done();
              });
            });
          });

          it("createIndex(name, columns)", function(done) {
            tbl.createIndex(user.index.name, user.index.columns);

            setTimeout(function() {
              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            }, 500);
          });

          it("createIndex(name, columns, options)", function(done) {
            tbl.createIndex(user.index.name, user.index.columns, user.index.options);

            setTimeout(function() {
              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            }, 500);
          });

          it("createIndex(name, columns, callback)", function(done) {
            tbl.createIndex(user.index.name, user.index.columns, function(error) {
              should.assert(error === undefined);

              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            });
          });

          it("createIndex(name, columns, options, callback)", function(done) {
            tbl.createIndex(user.index.name, user.index.columns, user.index.options, function(error) {
              should.assert(error === undefined);

              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            });
          });
        });

        describe("#dropIndex()", function() {
          beforeEach(function(done) {
            db.createIndex(user.schema, user.name, user.index.name, user.index.columns, done);
          });

          describe("Error handling", function() {
            it("dropIndex()", function() {
              (function() {
                tbl.dropIndex();
              }).should.throwError("Index expected.");
            });
          });

          it("dropIndex(name)", function(done) {
            tbl.dropIndex(user.index.name);

            setTimeout(function() {
              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(false);
                done();
              });
            }, 500);
          });

          it("dropIndex(name, callback)", function(done) {
            tbl.dropIndex(user.index.name, function(error) {
              should.assert(error === undefined);

              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(false);
                done();
              });
            });
          });

          it("dropIndex('ix_unknown')", function(done) {
            tbl.dropIndex("ix_unknown");
            setTimeout(done, 500);
          });

          it("dropIndex('ix_unknown', callback)", function(done) {
            tbl.dropIndex(user.index.name, function(error) {
              should.assert(error === undefined);
              done();
            });
          });
        });
      });
    });
  });
});