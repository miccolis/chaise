Chaise
------

A simple script, intended to be run periodically by cron, which stores the
ouput of CouchDB MapReduces in a CouchDB database.

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

Inspired by: http://support.cloudant.com/customer/portal/articles/359310-chained-mapreduce-views

See also: https://github.com/afters/Couch-Incarnate
