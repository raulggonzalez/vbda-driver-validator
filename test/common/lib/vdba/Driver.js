//suite
describe("vdba.Driver", function() {
  describe("#getDriver()", function() {
    it("getDriver('Unknown')", function() {
      var drv = vdba.Driver.getDriver("Unknown");
      should.assert(drv === undefined);
    });

    it("getDriver(name)", function() {
      var drv = vdba.Driver.getDriver(config.driver.name);

      drv.should.be.instanceOf(vdba.Driver);
      drv.name.should.be.eql(config.driver.name);
    });

    it("getDriver(alias)", function() {
      var drv = vdba.Driver.getDriver(config.driver.name);

      for (var i = 0, aliases = drv.aliases; i < aliases.length; ++i) {
        vdba.Driver.getDriver(aliases[i]).should.be.exactly(drv);
      }
    });
  });

  describe("#createConnection()", function() {
    var drv;

    before(function() {
      drv = vdba.Driver.getDriver(config.driver.name);
    });

    describe("Error handling", function() {
      it("createConnection()", function() {
        (function() {
          drv.createConnection();
        }).should.throwError("Configuration expected.");
      });
    });

    it("createConnection(config)", function() {
      var cx = drv.createConnection(config.connection.config);
      cx.should.be.instanceOf(vdba.Connection);
      cx.connected.should.be.eql(false);
    });
  });

  describe("#openConnection()", function() {
    var drv;

    before(function() {
      drv = vdba.Driver.getDriver(config.driver.name);
    });

    describe("Error handling", function() {
      it("openConnection()", function() {
        (function() {
          drv.openConnection();
        }).should.throwError("Configuration expected.");
      });

      it("openConnection(config)", function() {
        (function() {
          drv.openConnection(config.connection.config);
        }).should.throwError("Callback expected.");
      });
    });

    it("openConnection(config, callback)", function(done) {
      drv.openConnection(config.connection.config, function(error, con) {
        should.assert(error === undefined);
        con.should.be.instanceOf(vdba.Connection);
        con.connected.should.be.eql(true);
        con.close(done);
      });
    });
  });
});