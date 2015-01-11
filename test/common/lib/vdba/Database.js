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

  describe("Schemas", function() {
    describe("Type 1 Driver", function() {
      describe("#hasSchema()", function() {
        beforeEach(function(done) {
          db.createSchema(user.schema, done);
        });

        describe("Error handling", function() {
          it("hasSchema()", function() {
            (function() {
              db.hasSchema();
            }).should.throwError("Schema name expected.");
          });

          it("hasSchema(schema)", function() {
            (function() {
              db.hasSchema(user.schema);
            }).should.throwError("Callback expected.");
          });
        });

        it("hasSchema(schema, callback)", function(done) {
          db.hasSchema(user.schema, function(error, exists) {
            should.assert(error === undefined);
            exists.should.be.eql(true);
            done();
          });
        });
      });

      describe("#findSchema()", function() {
        it("findSchema(schema, callback)", function(done) {
          db.findSchema(user.schema, function(error, schema) {
            should.assert(error === undefined);
            schema.should.be.instanceOf(vdba.Schema);
            done();
          });
        });
      });
    });

    describe("Type 2 Driver", function() {
      describe("#createSchema()", function() {
        describe("Error handling", function() {
          it("createSchema()", function() {
            (function() {
              db.createSchema();
            }).should.throwError("Schema name expected.");
          });
        });

        it("createSchema(schema, callback)", function(done) {
          db.createSchema(user.schema, function(error) {
            should.assert(error === undefined);

            db.hasSchema(user.schema, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });
        });
      });

      describe("#dropSchema()", function() {
        describe("Error handling", function() {
          it("dropSchema()", function() {
            (function() {
              db.dropSchema();
            }).should.throwError("Schema name expected.");
          });
        });
      });
    });
  });

  describe("Tables", function() {
    describe("Type 1 Driver", function() {
      beforeEach(function(done) {
        db.createTable(user.schema, user.name, user.columns, done);
      });

      beforeEach(function(done) {
        db.createTable(session.schema, session.name, session.columns, done);
      });

      afterEach(function(done) {
        db.dropTable(user.schema, user.name, done);
      });

      afterEach(function(done) {
        db.dropTable(session.schema, session.name, done);
      });

      describe("#readTable()", function() {
        describe("Error handling", function() {
          it("readTable()", function() {
            (function() {
              db.readTable();
            }).should.throwError("Schema name expected.");
          });

          it("readTable(schema)", function() {
            (function() {
              db.readTable(user.schema);
            }).should.throwError("Table name expected.");
          });

          it("readTable(schema, table)", function() {
            (function() {
              db.readTable(user.schema, user.name);
            }).should.throwError("Callback expected.");
          });
        });

        it("readTable(schema, table, callback)", function(done) {
          db.readTable(user.schema, user.name, function(error, table) {
            should.assert(error === undefined);
            table.should.be.instanceOf(vdba.Table);
            table.schema.should.be.instanceOf(vdba.Schema);
            done();
          });
        });

        it("readTable(schema, 'unknown', callback)", function(done) {
          db.readTable(user.schema, "unknown", function(error, table) {
            should.assert(error === undefined);
            should.assert(table === undefined);
            done();
          });
        });

        it("readTable('unknown', table, callback)", function(done) {
          db.readTable("unknown", user.name, function(error, table) {
            should.assert(error === undefined);
            should.assert(table === undefined);
            done();
          });
        });

        it("readTable('unknown', 'unknown', callback)", function(done) {
          db.readTable("unknown", "unknown", function(error, table) {
            should.assert(error === undefined);
            should.assert(table === undefined);
            done();
          });
        });
      });

      describe("#findTable()", function() {
        describe("Error handling", function() {
          it("findTable()", function() {
            (function() {
              db.findTable();
            }).should.throwError("Schema name expected.");
          });

          it("findTable(schema)", function() {
            (function() {
              db.findTable(user.schema);
            }).should.throwError("Table name expected.");
          });

          it("findTable(schema, table)", function() {
            (function() {
              db.findTable(user.schema, user.name);
            }).should.throwError("Callback expected.");
          });
        });

        it("findTable(schema, table, callback)", function(done) {
          db.findTable(user.schema, user.name, function(error, tab) {
            should.assert(error === undefined);
            tab.should.be.instanceOf(vdba.Table);
            tab.database.should.be.exactly(db);
            tab.schema.should.be.instanceOf(vdba.Schema);
            tab.name.should.be.eql(user.name);
            tab.columns.should.be.instanceOf(Object);
            done();
          });
        });

        it("findTable(schema, 'unknown', callback)", function(done) {
          db.findTable(user.schema, "unknown", function(error, tab) {
            should.assert(error === undefined);
            should.assert(tab === undefined);
            done();
          });
        });

        it("findTable('unknown', table, callback)", function(done) {
          db.findTable("unknown", user.name, function(error, tab) {
            should.assert(error === undefined);
            should.assert(tab === undefined);
            done();
          });
        });
      });

      describe("#hasTable()", function() {
        describe("Error handling", function() {
          it("hasTable()", function() {
            (function() {
              db.hasTable();
            }).should.throwError("Schema name expected.");
          });

          it("hasTable(schema)", function() {
            (function() {
              db.hasTable(user.schema);
            }).should.throwError("Table name expected.");
          });

          it("hasTable(schema, table)", function() {
            (function() {
              db.hasTable(user.schema, user.name);
            }).should.throwError("Callback expected.");
          });
        });

        describe("Only table", function() {
          it("hasTable(schema, table, callback)", function(done) {
            db.hasTable(user.schema, user.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hastTable(schema, table, undefined, callback)", function(done) {
            db.hasTable(user.schema, user.name, undefined, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, 'unknown', callback)", function(done) {
            db.hasTable(user.schema, "unknown", function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable('unknown', table, callback)", function(done) {
            db.hasTable("unknown", user.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable('unknown', 'unknown', callback)", function(done) {
            db.hasTable("unknown", "unknown", function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });
        });

        describe("Table and columns", function() {
          it("hasTable(schema, table, {}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, {column: 'type'}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: "text"}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, {column: 'another'}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: "integer"}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, {unknown: type}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {unknown: "text"}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, {column: {type: 'type'}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: {type: "text"}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, {column: {type: 'another'}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: {type: "integer"}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, {column: {nullable: true}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: {nullable: true}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, {column: {nullable: false}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: {nullable: false}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, {column: {primaryKey: true}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {userId: {primaryKey: true}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, {column: {primaryKey: false}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {userId: {primaryKey: false}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, {column: {type: 'type', nullable: false}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: {type: "text", nullable: false}}, function(error, exists){
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, {column: {type: 'another', nullable: false}}, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: {type: "integer", nullable: false}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, {column: {type: 'type', nullable: !false}}, callback)", function(done)  {
            db.hasTable(user.schema, user.name, {username: {type: "text", nullable: true}}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasTable(schema, table, columns, callback)", function(done) {
            db.hasTable(user.schema, user.name, {username: "text", password: "text"}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasTable(schema, table, columns, callback) - w/ some unknown column", function(done) {
            db.hasTable(user.schema, user.name, {username: "text", unknown: "text"}, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });
        });
      });

      describe("#hasTables()", function() {
        describe("Error handling", function() {
          it("hasTables()", function() {
            (function() {
              db.hasTables();
            }).should.throwError("Schema name expected.");
          });

          it("hasTables()", function() {
            (function() {
              db.hasTables(user.schema);
            }).should.throwError("Table names expected.");
          });

          it("hasTables(schema, [])", function() {
            (function() {
              db.hasTables(user.schema, []);
            }).should.throwError("Table names expected.");
          });

          it("hasTables(schema, tables)", function() {
            (function() {
              db.hasTables(user.schema, [user.name, session.name]);
            }).should.throwError("Callback expected.");
          });
        });

        it("hasTables([e], callback)", function(done) {
          db.hasTables(user.schema, [user.name], function(error, exist) {
            should.assert(error === undefined);
            exist.should.be.eql(true);
            done();
          });
        });

        it("hasTables([u], callback)", function(done) {
          db.hasTables(user.schema, ["Unknown"], function(error, exist) {
            should.assert(error === undefined);
            exist.should.be.eql(false);
            done();
          });
        });

        it("hasTables([e, e], callback)", function(done) {
          db.hasTables(user.schema, [user.name, session.name], function(error, exist) {
            should.assert(error === undefined);
            exist.should.be.eql(true);
            done();
          });
        });

        it("hasTables([e, u], callback)", function(done) {
          db.hasTables(user.schema, [user.name, "Unknown"], function(error, exist) {
            should.assert(error === undefined);
            exist.should.be.eql(false);
            done();
          });
        });

        it("hasTables([u, u], callback)", function(done) {
          db.hasTables(user.schema, ["Unknown1", "Unknown2"], function(error, exist) {
            should.assert(error === undefined);
            exist.should.be.eql(false);
            done();
          });
        });

        it("hasTables([u, e], callback)", function(done) {
          db.hasTables(user.schema, ["Unknown", session.name], function(error, exist) {
            should.assert(error === undefined);
            exist.should.be.eql(false);
            done();
          });
        });
      });
    });

    describe("Type 2 Driver", function() {
      describe("#createTable()", function() {
        afterEach(function(done) {
          db.dropTable(user.schema, user.name, done);
        });

        describe("Error handling", function() {
          it("createTable()", function() {
            (function() {
              db.createTable();
            }).should.throwError("Schema name expected.");
          });

          it("createTable(schema)", function() {
            (function() {
              db.createTable(user.schema);
            }).should.throwError("Table name expected.");
          });
        });

        it("createTable(schema, table, columns)", function(done) {
          db.createTable(user.schema, user.name, user.columns);

          setTimeout(function() {
            db.hasTable(user.schema, user.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          }, 500);
        });

        it("createTable(schema, table, columns, callback)", function(done) {
          db.createTable(user.schema, user.name, user.columns, function(error) {
            should.assert(error === undefined);

            db.hasTable(user.schema, user.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });
        });

        it("createTable(schema, table, columns, callback) - Existing table", function(done) {
          db.createTable(user.schema, user.name, user.columns, function(error) {
            should.assert(error === undefined);

            db.createTable(user.schema, user.name, user.columns, function(error) {
              error.should.be.instanceOf(Error);
              done();
            });
          });
        });
      });

      describe("#dropTable()", function() {
        beforeEach(function(done) {
          db.createTable(user.schema, user.name, user.columns, done);
        });

        afterEach(function(done) {
          db.dropTable(user.schema, user.name, done);
        });

        describe("Error handling", function() {
          it("dropTable()", function() {
            (function() {
              db.dropTable();
            }).should.throwError("Schema name expected.");
          });

          it("dropTable(schema)", function() {
            (function() {
              db.dropTable(user.schema);
            }).should.throwError("Table name expected.");
          });
        });

        it("dropTable(schema, table)", function(done) {
          db.dropTable(user.schema, user.name);

          setTimeout(function() {
            db.hasTable(user.schema, user.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          }, 500);
        });

        it("dropTable(schema, 'Unknown')", function(done) {
          db.dropTable(user.schema, "Unknown");
          done();
        });

        it("dropTable(schema, table, callback)", function(done) {
          db.dropTable(user.schema, user.name, function(error) {
            should.assert(error === undefined);

            db.hasTable(user.schema, user.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });
        });

        it("dropTable('Unknown', table, callback)", function(done) {
          db.dropTable("Unknown", user.name, function(error) {
            should.assert(error === undefined);
            done();
          });
        });
      });
    });
  });

  describe("Indexes", function() {
    describe("Type 2 Driver", function() {
      describe("DQL", function() {
        beforeEach(function(done) {
          db.createTable(user.schema, user.name, user.columns, done);
        });

        beforeEach(function(done) {
          db.createIndex(user.schema, user.name, user.index.name, user.index.columns, done);
        });

        afterEach(function(done) {
          db.dropTable(user.schema, user.name, done);
        });

        describe("#hasIndex()", function() {
          describe("Error handling", function() {
            it("hasIndex()", function() {
              (function() {
                db.hasIndex();
              }).should.throwError("Schema expected.");
            });

            it("hasIndex(schema)", function() {
              (function() {
                db.hasIndex(user.schema);
              }).should.throwError("Index name expected.");
            });

            it("hasIndex(schema, index)", function() {
              (function() {
                db.hasIndex(user.schema, user.index.name);
              }).should.throwError("Callback expected.");
            });
          });

          it("hasIndex(schema, index, callback)", function(done) {
            db.hasIndex(user.schema, user.index.name, function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(true);
              done();
            });
          });

          it("hasIndex(schema, 'unknown', callback)", function(done) {
            db.hasIndex(user.schema, "unknown", function(error, exists) {
              should.assert(error === undefined);
              exists.should.be.eql(false);
              done();
            });
          });

          it("hasIndex('unknown', index, callback)", function(done) {
            db.hasIndex("unknown", user.index.name, function(error, exists) {
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
              }).should.throwError("Schema expected.");
            });

            it("findIndex(schema)", function() {
              (function() {
                db.findIndex(user.schema);
              }).should.throwError("Index name expected.");
            });

            it("findIndex(schema, name)", function() {
              (function() {
                db.findIndex(user.schema, user.index.name);
              }).should.throwError("Callback expected.");
            });
          });

          it("findIndex(schema, name, callback)", function(done) {
            db.findIndex(user.schema, user.index.name, function(error, ix) {
              should.assert(error === undefined);
              ix.should.be.instanceOf(vdba.Index);
              done();
            });
          });

          it("findIndex(schema, 'unknown', callback)", function(done) {
            db.findIndex(user.schema, "unknown", function(error, ix) {
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
            db.createTable(user.schema, user.name, user.columns, done);
          });

          afterEach(function(done) {
            db.dropTable(user.schema, user.name, done);
          });

          describe("Error handling", function() {
            beforeEach(function(done) {
              db.createIndex(user.schema, user.name, user.index.name, user.index.columns, done);
            });

            it("createIndex()", function() {
              (function() {
                db.createIndex();
              }).should.throwError("Schema expected.");
            });

            it("createIndex(schema)", function() {
              (function() {
                db.createIndex(user.schema);
              }).should.throwError("Table expected.");
            });

            it("createIndex(schema, table)", function() {
              (function() {
                db.createIndex(user.schema, user.name);
              }).should.throwError("Index name expected.");
            });

            it("createIndex(schema, table, index)", function() {
              (function() {
                db.createIndex(user.schema, user.name, user.index.name);
              }).should.throwError(/^Indexing column(\(s\))? expected.$/);
            });

            it("createIndex(schema, table, index, columns, callback) - Existing", function(done) {
              db.createIndex(user.schema, user.name, user.index.name, user.index.columns, function(error) {
                error.should.be.instanceOf(Error);
                done();
              });
            });
          });

          it("createIndex(schema, table, index, columns)", function(done) {
            db.createIndex(user.schema, user.name, user.index.name, user.index.columns);

            setTimeout(function() {
              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            }, 500);
          });

          it("createIndex(schema, table, index, columns, options)", function(done) {
            db.createIndex(user.schema, user.name, user.index.name, user.index.columns, user.index.options);

            setTimeout(function() {
              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            }, 500);
          });

          it("createIndex(schema, table, index, columns, callback)", function(done) {
            db.createIndex(user.schema, user.name, user.index.name, user.index.columns, function(error) {
              should.assert(error === undefined);

              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(true);
                done();
              });
            });
          });

          it("createIndex(schema, table, index, columns, options, callback)", function(done) {
            db.createIndex(user.schema, user.name, user.index.name, user.index.columns, user.index.options, function(error) {
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
            db.createTable(user.schema, user.name, user.columns, done);
          });

          beforeEach(function(done) {
            db.createIndex(user.schema, user.name, user.index.name, user.index.columns, done);
          });

          afterEach(function(done) {
            db.dropTable(user.schema, user.name, done);
          });

          describe("Error handling", function() {
            it("dropIndex()", function() {
              (function() {
                db.dropIndex();
              }).should.throwError("Schema expected.");
            });

            it("dropIndex(schema)", function() {
              (function() {
                db.dropIndex(user.schema);
              }).should.throwError("Index expected.");
            });
          });

          it("dropIndex(schema, index)", function(done) {
            db.dropIndex(user.schema, user.index.name);

            setTimeout(function() {
              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(false);
                done();
              });
            }, 500);
          });

          it("dropIndex(schema, 'unknown')", function(done) {
            db.dropIndex(user.schema, "unknown");
            setTimeout(done, 500);
          });

          it("dropIndex('unknown', index)", function(done) {
            db.dropIndex('unknown', user.index.name);
            setTimeout(done, 500);
          });

          it("dropIndex(schema, name, callback)", function(done) {
            db.dropIndex(user.schema, user.index.name, function(error) {
              should.assert(error === undefined);

              db.hasIndex(user.schema, user.index.name, function(error, exists) {
                should.assert(error === undefined);
                exists.should.be.eql(false);
                done();
              });
            });
          });

          it("dropIndex(schema, 'unknown', callback)", function(done) {
            db.dropIndex(user.schema, "unknown", function(error) {
              should.assert(error === undefined);
              done();
            });
          });

          it("dropIndex('unknown', index, callback)", function(done) {
            db.dropIndex("unknown", user.index.name, function(error) {
              should.assert(error === undefined);
              done();
            });
          });
        });
      });
    });
  });
});