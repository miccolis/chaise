#!/usr/bin/env node

var util = require('util');
var http = require('http');
var url = require('url');

var couchDB
try {
    couchDB = url.parse(process.argv[2]);
}
catch (e) {
    console.error('Error: argument must be a valid URL');
    process.exit();
}

getReduces(couchDB, function(err, data) {
    if (err) throw err;
    data.forEach(function(v) {
        var source = {
            host: couchDB.hostname,
            port: couchDB.port,
            path: util.format('%s/%s/_view/%s?group=true',
                couchDB.pathname, v.id, v.view) 
        };
        var target = {
            host: couchDB.hostname,
            port: couchDB.port,
            path: '/' + v.target
        };
        ensureDB(target, function(err) {
            updateDestination(source, target, function(err) {
                if (err) console.error(err);
                console.log(arguments);
            })
        });
    });
});

// Find reduces to save.
function getReduces(couchDB, next) {

    var getDesignDoc = function(docname, callback) {
        http.get({
            host: couchDB.hostname,
            port: couchDB.port,
            path: couchDB.pathname + '/' + docname
        }, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load doc'));

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() { callback(null, JSON.parse(body)) })
        }).on('error', function(e) { callback(e) })
    }

    var getDesignDocs = function(callback) {
        http.get({
            host: couchDB.hostname,
            port: couchDB.port,
            path: url.format({
                pathname: couchDB.pathname + '/_all_docs',
                query: { startkey: '"_design"', endkey:'"_design0"' }
            })
        }, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load docs'));

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() { callback(null, JSON.parse(body)) })
        }).on('error', function(e) { callback(e) })
    }

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
        http.get({
            host: db.host,
            port: db.port,
            path: db.path
        }, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load db'));
            res.on('end', function() { callback(null) })
        }).on('error', function(e) { callback(e) })
    }

    var createDB = function(callback) {
        var uri = {
            method: 'PUT',
            host: db.host,
            port: db.port,
            path: db.path
        };

        var req = http.request(uri, function(res) {
            if (res.statusCode != 201)
                return callback(new Error('Could not be created'));
            res.on('end', function() { callback(null) })
        })
        req.on('error', function(e) { callback(e) })
        req.end();
    }

    getDB(function(err) {
        if (err) {
            console.log('Creating %s', db.path);
            createDB(next)
        }
        next();
    });
}

// Takes results from a view and write then to a db.
function updateDestination(source, target, next) {
    // TODO make this streaming.

    var retrieve = function(callback) {
        http.get(source, function(res) {
            if (res.statusCode != 200)
                return callback(new Error('Could not load doc'));

            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { body += chunk });
            res.on('end', function() { callback(null, JSON.parse(body).rows) })
        }).on('error', function(e) { callback(e) })
    }

    var insert = function(data, callback) {
        data = data.map(function(v) {
            v._id = v.key.toString();;
            return v;
        });

        var uri = {
            method: 'POST',
            host: target.host,
            port: target.port,
            path: target.path + '/_bulk_docs',
            headers: {'Content-Type': 'application/json'}
        };

        var req = http.request(uri, function(res) {
            if (res.statusCode != 201)
                return callback(new Error('Could not update documents'));
            res.on('end', function() { callback(null) })
        })
        req.on('error', function(e) { callback(e) })
        req.write(JSON.stringify({docs: data}));
        req.end();
    }

    retrieve(function(err, data) {
        if (err) return next(err);
        insert(data, next);
    });
}
