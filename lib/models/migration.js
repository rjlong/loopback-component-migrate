'use strict';
/* global require */
/* global module */

var debug = require('debug')('loopback-component-migrate');
var path = require('path');
var fs = require('fs');
var assert = require('assert');
var utils = require('loopback-datasource-juggler/lib/utils');
var util = require('util');
var isNullOrUndefined = require('util').isNullOrUndefined;
var Promise = require('bluebird');

module.exports = function(Migration, options) {
  options = options || {};
  Migration.log = options.log || console;
  Migration.log = typeof Migration.log === 'string' ? require(Migration.log) : Migration.log;
  Migration.migrationsDir = options.migrationsDir || path.join(process.cwd(), 'server', 'migrations');
  debug('Migrations directory set to: %s', Migration.migrationsDir);

  Migration.handlePromiseAndCallback = function(err, result, cb) {
    if (cb) {
      cb(err, result);
    }

    if (!isNullOrUndefined(err)) {
      return Promise.reject(err);
    }

    return Promise.resolve(result);
  };

  Migration.startMigrating = function() {
    return Promise.resolve(Migration.app.migrating = true);
  };

  Migration.stopMigrating = function() {
    return Promise.resolve(delete Migration.app.migrating);
  };

  const mapScriptObjName = (scriptObj) => scriptObj.name;
  
  /**
   * Remote Method: Run pending migrations.
   *
   * @param {String} [to] Name of the migration script to migrate to.
   * @param {Function} [cb] Callback function.
   */
  Migration.migrateTo = function(to, cb) {
    to = to || '';
    assert(typeof to === 'string', 'The to argument must be a string, not ' + typeof to);
    return Migration.migrate('up', to, cb);
  };

  /**
   * Remote Method: Rollback migrations.
   *
   * @param {String} [to] Name of migration script to rollback to.
   * @param {Function} [cb] Callback function.
   */
  Migration.rollbackTo = function(to, cb) {
    to = to || '';
    assert(typeof to === 'string', 'The to argument must be a string, not ' + typeof to);
    return Migration.migrate('down', to, cb);
  };

  /**
   * Run migrations (up or down).
   *
   * @param {String} [upOrDown] Direction (up or down)
   * @param {String} [to] Name of migration script to migrate/rollback to.
   * @param {Function} [cb] Callback function.
   */
  Migration.migrate = function(upOrDown, to, cb) {
    if (typeof to === 'function') {
      to = '';
    }
    upOrDown = upOrDown || 'up';
    to = to || '';

    assert(typeof upOrDown === 'string', 'The upOrDown argument must be a string, not ' + typeof upOrDown);
    assert(typeof to === 'string', 'The to argument must be a string, not ' + typeof to);

    if (Migration.app.migrating) {
      const msg = 'Unable to start migrations: already running';
      Migration.log.warn(msg);
      return Promise.reject(msg)
    }

    Migration.hrstart = process.hrtime();

    return Migration.startMigrating()
      .then(() => Migration.findScriptsToRun(upOrDown, to))
      .then(scriptsToRun => {
        scriptsToRun = scriptsToRun || [];

        if (scriptsToRun.length) {
          Migration.log.info('Running migrations: \n', scriptsToRun);

          return scriptsToRun.reduce((current, localScriptName) => {
            return current.then(() => Migration.runScript(localScriptName, upOrDown));
          }, new Promise.resolve());
          
        } else {
          Migration.log.info('No new migrations to run.');
          Migration.emit('complete');
        }
      })
      .then(() => Migration.finish(null, cb))
      .catch(function(err) {
        Migration.log.info('ERROR', err);
        return Migration.finish(err, cb);
      });
  };

  Migration.finish = function(err, cb) {
    if (err) {
      Migration.log.error('Migrations did not complete. An error was encountered:', err);
      Migration.emit('error', err);
    } else {
      Migration.log.info('All migrations have run without any errors.');
      Migration.emit('complete');
    }
    delete Migration.app.migrating;
    var hrend = process.hrtime(Migration.hrstart);
    Migration.log.info('Total migration time was %ds %dms', hrend[0], hrend[1] / 1000000);
    return Migration.stopMigrating()
      .then(() => Migration.handlePromiseAndCallback(err, null, cb));
  };

  Migration.findScriptsToRun = function(upOrDown, to, cb) {
    upOrDown = upOrDown || 'up';
    to = to || '';

    debug('findScriptsToRun direction:%s, to:%s', upOrDown, to ? to : 'undefined');

    // Add .js to the script name if it wasn't provided.
    if (to && to.substring(to.length - 3, to.length) !== '.js') {
      to = to + '.js';
    }

    var scriptsToRun = [];
    var order = upOrDown === 'down' ? 'name DESC' : 'name ASC';
    var filters = {
      order: order
    };

    if (to) {
      var where;
      // DOWN: find only those that are greater than the 'to' point in descending order.
      if (upOrDown === 'down') {
        where = { name: { gte: to }};
      }
      // UP: find only those that are less than the 'to' point in ascending order.
      else {
        where = { name: { lte: to }};
      }
      filters.where = where;
    }
    debug('fetching migrations from db using filter %j', filters);
    return Migration.find(filters)
      .then(scriptsAlreadyRan => {
        scriptsAlreadyRan = scriptsAlreadyRan.map(script => mapScriptObjName(script));
        debug('scriptsAlreadyRan: %j', scriptsAlreadyRan);

        // Find rollback scripts.
        if (upOrDown === 'down') {

          // If the requested rollback script has not already run return just the requested one if it is a valid script.
          // This facilitates rollback of failed migrations.
          if (to && scriptsAlreadyRan.indexOf(to) === -1) {
            debug('requested script has not already run - returning single script as standalone rollback script');
            scriptsToRun = [to];
            return scriptsToRun
          }

          // Remove the last item since we don't want to roll back the requested script.
          if (scriptsAlreadyRan.length && to) {
            scriptsAlreadyRan.pop();
            debug('remove last item. scriptsAlreadyRan: %j', scriptsAlreadyRan);
          }
          scriptsToRun = scriptsAlreadyRan;

          debug('Found scripts to run: %j', scriptsToRun);
          return scriptsToRun;
        } else {
          // Find migration scripts.
          // get all local scripts and filter for only .js files
          var candidateScripts = fs.readdirSync(Migration.migrationsDir).filter(fileName => fileName.substring(fileName.length - 3, fileName.length) === '.js');
          debug('Found %s candidate scripts: %j', candidateScripts.length, candidateScripts);

          // filter out those that come after the requested to value.
          if (to) {
            candidateScripts = candidateScripts.filter(function(fileName) {
              var inRange = fileName <= to;
              debug('checking wether %s is in range (%s <= %s): %s', fileName, fileName, to, inRange);
              return inRange;
            });
          }

          // filter out those that have already ran
          candidateScripts = candidateScripts.filter(function(fileName) {
            debug('checking wether %s has already run', fileName);
            var alreadyRan = scriptsAlreadyRan.indexOf(fileName) !== -1;
            debug('checking wether %s has already run: %s', fileName, alreadyRan);
            return !alreadyRan;
          });

          scriptsToRun = candidateScripts;
          debug('Found scripts to run: %j', scriptsToRun);
          return scriptsToRun;
        }
      })
      .then(res => Migration.handlePromiseAndCallback(null, res, cb))
      .catch(err => {
        Migration.log.error('Error retrieving migrations:');
        Migration.log.error(err.stack);
        return Migration.handlePromiseAndCallback(err, null, cb);
      });
  };

  Migration.runScript = function(localScriptName, upOrDown) {
    const migrationStartTime = process.hrtime();
    Migration.log.info(localScriptName, 'running.');
    return new Promise((resolve, reject) => {
      try {
        require(path.join(Migration.migrationsDir, localScriptName))[upOrDown](Migration.app, function (err) {
          if (err) {
            return reject(err)
          }
          return resolve();
        })
      }
      catch (err) {
        return reject(err);
    }
    })
    .then(() => {
      if (upOrDown === 'up') {
        return Migration.create({name: localScriptName, runDtTm: new Date()});
      }
      return Migration.destroyAll({name: localScriptName});
    })
    .then(() => {
      var migrationEndTime = process.hrtime(migrationStartTime);
      Migration.log.info('%s finished sucessfully. Migration time was %ds %dms', localScriptName, migrationEndTime[0], migrationEndTime[1] / 1000000);
    })
    .catch(err => {
      Migration.log.error(localScriptName, 'error:');
      Migration.log.error(err.stack);
      return Promise.reject(err);
    });
  };
  
  return Migration;
};
