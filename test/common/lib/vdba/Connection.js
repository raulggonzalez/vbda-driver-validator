describe("vdba.<driver>.Connection", function() {
  var drv;

  before(function() {
    drv = vdba.Driver.getDriver(config.driver.name);
  });

  describe("Properties", function() {
    var cx;

    before(function(done) {
      drv.openConnection(config.connection.config, function(error, con) {
        cx = con;
        done();
      });
    });

    after(function(done) {
      cx.close(done);
    });

    it("server", function() {
      cx.server.should.be.instanceOf(vdba.Server);
    });

    it("database", function() {
      cx.database.should.be.instanceOf(vdba.Database);
      cx.database.name.should.be.eql(config.connection.config.database);
    });
  });

  describe("#open()", function() {
    var cx;

    beforeEach(function() {
      cx = drv.createConnection(config.connection.config);
    });

    afterEach(function(done) {
      cx.close(done());
    });

    describe("With closed connection", function() {
      it("open()", function(done) {
        cx.open();

        setTimeout(function() {
          cx.connected.should.be.eql(true);
          done();
        }, 500);
      });

      it("open(callback)", function(done) {
        cx.open(function(error) {
          should.assert(error === undefined);
          cx.connected.should.be.eql(true);
          done();
        });
      });
    });

    describe("With opened connection", function() {
      it("open()", function(done) {
        cx.open();

        setTimeout(function() {
          cx.connected.should.be.eql(true);
          done();
        }, 500);
      });

      it("open(callback)", function(done) {
        cx.open(function(error) {
          should.assert(error === undefined);
          cx.connected.should.be.eql(true);
          done();
        });
      });
    });
  });

  describe("#close()", function() {
    var cx;

    describe("With opened connection", function() {
      beforeEach(function(done) {
        drv.openConnection(config.connection.config, function(error, con) {
          cx = con;
          done();
        });
      });

      it("close()", function(done) {
        cx.close();

        setTimeout(function() {
          cx.connected.should.be.eql(false);
          done();
        }, 500);
      });

      it("close(callback)", function(done) {
        cx.close(function(error) {
          should.assert(error === undefined);
          cx.connected.should.be.eql(false);
          done();
        });
      });
    });

    describe("With closed connection", function() {
      beforeEach(function() {
        cx = drv.createConnection(config.connection.config);
      });

      it("close()", function(done) {
        cx.close();

        setTimeout(function() {
          cx.connected.should.be.eql(false);
          done();
        }, 500);
      });

      it("close(callback)", function(done) {
        cx.close(function(error) {
          should.assert(error === undefined);
          cx.connected.should.be.eql(false);
          done();
        });
      });
    });
  });
});