#!/usr/bin/env node

var util = require('util');
var http = require('http');
var url = require('url');

var couchDB;
try {
    couchDB = url.parse(process.argv[2]);
}
catch (e) {
    console.error('error: source must be a valid url');
    process.exit();
}

if (process.argv.length == 3) {
    // Reclining use: simple reduce instruction stored within the design doc.
    getReduces(couchDB, function(err, data) {
        if (err) throw err;
        data.forEach(function(v) {
            var target = {
                hostname: couchDB.hostname,
                port: couchDB.port,
                path: '/' + v.target
            };
            ensureDB(target, function(err) {
                if (err) return console.error(err);
                var source = {
                    hostname: couchDB.hostname,
                    port: couchDB.port,
                    path: util.format('%s/%s/_view/%s?group=true',
                        couchDB.pathname, v.id, v.view)
                };
                updateDestination(source, target, function(err) {
                    if (err) console.error(err);
                })
            });
        });
    });
}
else if (process.argv.length == 4) {
    // Sitting up: explicity declare the source URI.
    var destDB;
    try {
        destDB = url.parse(process.argv[3]);
    }
    catch (e) {
        console.error('error: destination must be a valid url');
        process.exit();
    }

    ensureDB(destDB, function(err) {
        if (err) return console.error(err);
        updateDestination(couchDB, destDB, function(err) {
            if (err) console.error(err);
        })
    });
}
else {
    console.error('error: bad arguments');
    process.exit();
}


// Find reduces to save.
function getReduces(couchDB, next) {

    var getDesignDoc = function(docname, callback) {
        http.get({
            hostname: couchDB.hostname,
            port: couchDB.port,
            path: couchDB.pathname + '/' + docname
        }, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load design doc'));

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() { callback(null, JSON.parse(body)) })
        }).on('error', function(e) { callback(e) })
    };

    var getDesignDocs = function(callback) {
        http.get({
            hostname: couchDB.hostname,
            port: couchDB.port,
            path: url.format({
                pathname: couchDB.pathname + '/_all_docs',
                query: { startkey: '"_design"', endkey:'"_design0"' }
            })
        }, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load design docs'));

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() { callback(null, JSON.parse(body)) })
        }).on('error', function(e) { callback(e) })
    };

    getDesignDocs(function(err, docs) {
        if (err) return next(err);

        var wait = 0;
        var data = [];
        function after(err, body) {
            wait--;
            if (err) return console.error(err);
            if (body.views) {
                Object.keys(body.views).forEach(function(v) {
                    if (body.views[v].dbcopy) {
                        data.push({
                            id: body._id,
                            view: v,
                            target: body.views[v].dbcopy
                        });
                    }
                });
            }
            if (wait == 0) next(null, data);
        }

        docs.rows.forEach(function(v) {
            wait++;
            getDesignDoc(v.id, after)
        });
    });
}

// Ensure a database exists.
function ensureDB(db, next) {
    var getDB = function(callback) {
        http.get(db, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load db'));
            res.on('end', function() { callback(null) })
        }).on('error', function(e) { callback(e) })
    };

    var createDB = function(callback) {
        console.log('Creating %s', db.path);
        var req = http.request({
            method: 'PUT',
            hostname: db.hostname,
            port: db.port,
            path: db.path
        }, function(res) {
            if (res.statusCode != 201)
                return callback(new Error('Could not be created'));
            res.on('end', function() { callback(null) })
        })
        req.on('error', function(e) { callback(e) })
        req.end();
    };

    var getDesign = function(callback) {
        http.get({
            hostname: db.hostname,
            port: db.port,
            path: db.path + '/_design/chaise',
        }, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load design doc'));
            res.on('end', function() { callback(null) })
        }).on('error', function(e) { callback(e) })

    }
    var createDesign = function(callback) {
        var ddoc = {
           "_id": "_design/chaise",
           "language": "javascript",
           "views": {
               "time": {
                   "map": "function(doc) {\n  emit(doc.timestamp, {_id: doc._id, _rev: doc._rev});\n}"
               }
           }
        };

        var req = http.request({
            method: 'PUT',
            hostname: db.hostname,
            port: db.port,
            path: db.path + '/_design/chaise'
        }, function(res) {
            if (res.statusCode != 201)
                return callback(new Error('Could not be created'));
            res.on('end', function() {
                callback(null)
            });
        });
        req.on('error', function(e) { callback(e) })
        req.write(JSON.stringify(ddoc));
        req.end();
    };

    getDB(function(err) {
        if (err) {
            // No database, create it and the design doc.
            return createDB(function(err) {
                if (err) return next(err);
                createDesign(next);
            });
        }
        // Found the database, but we may need to make the design doc.
        getDesign(function(err) {
            if (!err) return next();
            createDesign(next);
        });
    });
}

// Takes results from a view and write then to a db.
function updateDestination(source, target, next) {
    var ts = +(new Date);
    // TODO make this streaming.

    var prepare = function(data) {
        return data.rows.map(function(v) {
            var key = v.key
            if (typeof key !== 'string' && key.join !== undefined) {
                key = key.join('/');
            }
            v._id = "r/" + key;
            v.timestamp = ts;
            return v;
        });
    };

    var retrieve = function(callback) {
        http.get(source, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load source data'));

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() { callback(null, JSON.parse(body)) })
        }).on('error', function(e) { callback(e) })
    };

    var getRevisions = function(data, callback) {
        var req = http.request({
            method: 'POST',
            hostname: target.hostname,
            port: target.port,
            path: target.path + '/_all_docs',
            headers: {'Content-Type': 'application/json'}
        }, function(res) {
            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() {
                if (res.statusCode != 200 || !body)
                    return callback(new Error('Bad response'));
                callback(null, JSON.parse(body));
            });
        });
        req.on('error', function(e) { callback(e) })
        req.write(JSON.stringify(data));
        req.end();
    };

    var getObsolete = function(callback) {
        var req = http.request({
            method: 'GET',
            hostname: target.hostname,
            port: target.port,
            path: util.format('%s/_design/chaise/_view/time?endkey=%s', target.path, ts - 1),
            headers: {'Content-Type': 'application/json'}
        }, function(res) {
            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() {
                if (res.statusCode != 200 || !body)
                    return callback(new Error('Bad response'));
                callback(null, JSON.parse(body));
            });
        });
        req.on('error', function(e) { callback(e) })
        req.end();
    };

    var insert = function(data, callback) {
        var req = http.request({
            method: 'POST',
            hostname: target.hostname,
            port: target.port,
            path: target.path + '/_bulk_docs',
            headers: {'Content-Type': 'application/json'}
        }, function(res) {
            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() {
                if (!body) return callback(new Error('Bad response'));
                body = JSON.parse(body);
                if (res.statusCode != 201) {
                    if (body.error)
                        return callback(new Error(body.error +', '+ body.reason));
                    return callback(new Error('Could not update documents'));
                }

                var errors = [];
                body.forEach(function(v) { if (v.error) errors.push(v) });
                callback(errors.length ? new Error('Completed with errors') : null);
            });

        })
        req.on('error', function(e) { callback(e) })
        req.write(JSON.stringify({docs: data}));
        req.end();
    };

    var cleanup = function(callback) {
        getObsolete(function(err, docs) {
            if (err) return callback(err);
            var data = docs.rows.map(function(v) {
                v.value._deleted = true;
                return v.value;
            });
            insert(data, function(err) {
                callback(err);
            });
        });
    };

    retrieve(function(err, data) {
        if (err) return next(err);
        data = prepare(data);

        var ids = data.map(function(v) { return v._id; });
        getRevisions({"keys": ids}, function(err, docs) {
            if (err) return next(err);
            if (docs.rows.length) {
                var idMap = {};
                docs.rows.forEach(function(v) {
                    if (!v.error) idMap[v.id] = v.value.rev
                });
                data = data.map(function(v) { v._rev = idMap[v._id]; return v; });
            }
            insert(data, function(err) {
                if (err) return next(err);
                cleanup(next);
            });
        });
    });
}
