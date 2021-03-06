'use strict';
/* global require */

var debug = require('debug')('loopback-component-migrate');
var _ = require('lodash');

var loopback = require('loopback');
var lt = require('loopback-testing');

var chai = require('chai');
var expect = chai.expect;
var sinon = require('sinon');
chai.use(require('sinon-chai'));
require('mocha-sinon');

var path = require('path');
var SIMPLE_APP = path.join(__dirname, 'fixtures', 'simple-app');
var app = require(path.join(SIMPLE_APP, 'server/server.js'));

global.Promise = require('bluebird');

lt.beforeEach.withApp(app);

describe('loopback db migrate', function() {

  describe('initialization', function() {
    it('should attach a Migration model to the app', function() {
      expect(app.models.Migration).to.exist;
      expect(app.models.Migration).itself.to.respondTo('migrate');
    });
    it('should attach a MigrationMap model to the app', function() {
      expect(app.models.Migration).to.exist;
    });
    it('should provide a Migration.migrate() method', function() {
      expect(app.models.Migration).itself.to.respondTo('migrate');
    });
  });

  describe('migration', function() {
    // Set up a spy for each migration function.
    before(function() {
      var m1 = require(path.join(SIMPLE_APP, 'server/migrations/0001-initialize.js'));
      var m2 = require(path.join(SIMPLE_APP, 'server/migrations/0002-somechanges.js'));
      var m3 = require(path.join(SIMPLE_APP, 'server/migrations/0003-morechanges.js'));
      this.spies = {
        m1Up: sinon.spy(m1, 'up'),
        m1Down: sinon.spy(m1, 'down'),
        m2Up: sinon.spy(m2, 'up'),
        m2Down: sinon.spy(m2, 'down'),
        m3Up: sinon.spy(m3, 'up'),
        m3Down: sinon.spy(m3, 'down')
      };

      this.resetSpies = function() {
        _.forEach(this.spies, function(spy) {
          spy.reset();
        });
      };

      this.expectNoDown = function() {
        expect(this.spies.m1Down).not.to.have.been.called;
        expect(this.spies.m2Down).not.to.have.been.called;
        expect(this.spies.m3Down).not.to.have.been.called;
      };

      this.expectNoUp = function() {
        expect(this.spies.m1Up).not.to.have.been.called;
        expect(this.spies.m2Up).not.to.have.been.called;
        expect(this.spies.m3Up).not.to.have.been.called;
      };
    });

    // Reset all the spies after each test.
    afterEach(function() {
      this.resetSpies();
    });

    // Delete all data after each test.
    beforeEach(function(done) {
      Promise.all([
        app.models.Migration.destroyAll(),
        app.models.Migration.destroyAll()
      ])
      .then(function() {
        done();
      })
      .catch(done);
    });

    describe('migrate', function() {
      it('should set a property on app to indicate that migrations are running', function(done) {
        var self = this;
        expect(app.migrating).to.be.undefined;
        var promise = app.models.Migration.migrate();
        expect(app.migrating).to.be.true;
        promise.then(function() {
          expect(app.migrating).to.be.undefined;
          done();
        })
        .catch(done);
      });
    });

    describe('up', function() {
      it('should run all migration scripts', function(done) {
        var self = this;
        app.models.Migration.migrate()
          .then(function() {
            expect(self.spies.m1Up).to.have.been.called;
            expect(self.spies.m2Up).to.have.been.calledAfter(self.spies.m1Up);
            expect(self.spies.m3Up).to.have.been.calledAfter(self.spies.m2Up);
            self.expectNoDown();
            done();
          })
          .catch(done);
      });
      it('should run migrations up to the specificed point only', function(done) {
        var self = this;
        app.models.Migration.migrate('up', '0002-somechanges')
          .then(function() {
            expect(self.spies.m1Up).to.have.been.calledBefore(self.spies.m2Up);
            expect(self.spies.m2Up).to.have.been.calledAfter(self.spies.m1Up);
            expect(self.spies.m3Up).not.to.have.been.called;
            self.expectNoDown();
            done();
          })
          .catch(done);
      });
      it('should not rerun migrations that hae already been run', function(done) {
        var self = this;
        app.models.Migration.migrate('up', '0002-somechanges')
          .then(function() {
            self.resetSpies();
            return app.models.Migration.migrate('up');
          })
          .then(function() {
            expect(self.spies.m1Up).not.to.have.been.called;
            expect(self.spies.m2Up).not.to.have.been.called;
            expect(self.spies.m3Up).to.have.been.called;
            self.expectNoDown();
            done();
          })
          .catch(done);
      });
      it('should not rerun migrations that hae already been run', function(done) {
        var self = this;
          app.models.Migration.migrate('up')
            .then(function() {
              expect(self.spies.m1Up).to.have.been.called
              expect(self.spies.m2Up).to.have.been.calledAfter(self.spies.m1Up);
              expect(self.spies.m3Up).to.have.been.calledAfter(self.spies.m2Up);
              self.resetSpies();
            })
            .then(function() { app.models.Migration.migrate('up')})
            .then(function() {
              expect(self.spies.m1Up).not.to.have.been.called;
              expect(self.spies.m2Up).not.to.have.been.called;
              expect(self.spies.m3Up).not.to.have.been.called;
              self.expectNoDown();
              done();
            })
            .catch(done);
        });
    });

    describe('down', function() {
      it('should run all rollback scripts in reverse order', function(done) {
        var self = this;
        app.models.Migration.migrate('up')
          .then(function() {
            self.expectNoDown();
            self.resetSpies();
            return app.models.Migration.migrate('down');
          })
          .then(function() {
            expect(self.spies.m3Down).to.have.been.calledBefore(self.spies.m2Down);
            expect(self.spies.m2Down).to.have.been.calledAfter(self.spies.m3Down);
            expect(self.spies.m1Down).to.have.been.calledAfter(self.spies.m2Down);
            self.expectNoUp();
            done();
          })
          .catch(done);
      });
      it('should run rollbacks up to the specificed point only', function(done) {
        var self = this;
        app.models.Migration.migrate('up')
          .then(function() {
            self.expectNoDown();
            self.resetSpies();
            return app.models.Migration.migrate('down', '0001-initialize');
          })
          .then(function() {
            expect(self.spies.m3Down).to.have.been.called;
            expect(self.spies.m2Down).to.have.been.calledAfter(self.spies.m3Down);
            expect(self.spies.m1Down).not.to.have.been.called;
            self.expectNoUp();
            done();
          })
          .catch(done);
      });
      it('should not rerun rollbacks that hae already been run', function(done) {
        var self = this;
        app.models.Migration.migrate('up')
          .then(function() {
            return app.models.Migration.migrate('down', '0001-initialize');
          })
          .then(function() {
            self.resetSpies();
            return app.models.Migration.migrate('down');
          })
          .then(function() {
            expect(self.spies.m3Down).to.not.have.been.called;
            expect(self.spies.m2Down).to.not.have.been.called;
            expect(self.spies.m1Down).to.have.been.called;
            self.expectNoUp();
            done();
          })
          .catch(done);
      });
      it('should rollback a single migration that has not already run', function(done) {
        var self = this;
        app.models.Migration.migrate('up', '0002-somechanges')
          .then(function() {
            self.resetSpies();
            return app.models.Migration.migrate('down', '0003-morechanges');
          })
          .then(function() {
            expect(self.spies.m3Down).to.have.been.called;
            expect(self.spies.m2Down).to.not.have.been.called;
            expect(self.spies.m1Down).to.not.have.been.called;
            self.expectNoUp();
            done();
          })
          .catch(done);
      });
    });
  });

});
