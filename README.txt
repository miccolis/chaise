Chaise
------

A simple script, intended to be run periodically by cron, which stores the
ouput of CouchDB MapReduces in a CouchDB database.

This program can be used either sitting, or reclining.

RECLINING

Add a `dbcopy` key to a design document view. The value of this key is the name
of the target database.

For example:

    {
       "_id": "_design/stats",
       "language": "javascript",
       "views": {
           "summation": {
               "map": "function(doc) {\n  emit(doc.letter, doc.data);\n}",
               "reduce": "_sum",
               "dbcopy": "chaise_dest"
           },
       }
    }

Then run the command:

    ./index.js "http://localhost:5984/<dbname>"


SITTING

If you need specific arguments passed to the source view, or if your destination
is a different server you can pass a little more specificity to Chaise and also
provide it with a destination.

In this case you wouldn't change anything in your database. Rather you'd just
run a command like:

    ./index.js "http://localhost:5984/<dbname>/_design/<foo>/_view/<bar>" \
    "http://localhost:5984/chaise_dest"


CREDITS

Inspired by: http://support.cloudant.com/customer/portal/articles/359310-chained-mapreduce-views

See also: https://github.com/afters/Couch-Incarnate
