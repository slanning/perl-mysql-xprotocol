// mysqlsh -u test_user -h localhost --js xproto < play/xproto.js
// assumes password: test_pass
// and you created the collection: myDb.createCollection(coll_name);
// and did the myColl.add(...) calls (need to use existsInDatabase I think)

// http://dev.mysql.com/doc/x-devapi-userguide/en/devapi-users-introduction.html
// http://dev.mysql.com/worklog/task/?id=8338

var mysqlx = require('mysqlx').mysqlx;

var db_name = 'xproto';
var coll_name = 'test_collection';

// XSESSION

// alternative URL format: mysqlx.getSession('test_user:test_pass@localhost:33060')
var mySession = mysqlx.getSession( {    // gets an XSession
    host: 'localhost',
    // port: 33060,
    dbUser: 'test_user',
    dbPassword: 'test_pass'
} );
// getNodeSession

// BASESESSION methods
// s = getSchemas(); for (schema in s) { print(schema + '\n') }
// getSchema('schema')
// getDefaultSchema()
// createSchema('schema)
// dropSchema('schema')
// dropCollection('schema', 'collection')
// dropTable('schema', 'table')
// getUri()
// close()

// XSESSION has a simpler sub-set of SQL interface,
// meant to be able in the future to scale to multiple nodes, replication, etc..

// NODESESSION allows full SQL but only on one server
// sql("select * from foo").execute()
// res = sql("set myvar = ?").bind(10).execute(); row = res.fetchOne(); print(row[0])
// quoteName
// setCurrentSchema('schema')
// getCurrentSchema


// DATABASE objects: Schema, Collection, Table
// DATABASE object methods
// getSession()
// getSchema()
// getName()
// existsInDatabase()

var myDb = mySession.getSchema(db_name);

// SCHEMA methods
// myDb.createCollection(coll_name);    createCollection('coll', true) will reuse an existing collection
// getCollections()
// getTables()
// getCollection('coll')   getCollection('coll', true) to validate that the collection exists
// getTable('table')
// getCollectionAsTable('coll')

// DOCUMENT: COLLECTION or TABLE
// doc->'$.some.field.like[3].this'
// var customers = db.getCollectionAsTable('customers');
// customers.insert('doc').values('{"_id":"001", "name": "mike", "last_name": "connor"}').execute();
// var result = customers.select(["doc->'$.name'", "doc->'$.last_name'"]).where("doc->'$._id' = '001'").execute();
// var record = result.fetchOne();
// print "Last Name : %s\n"  % record[1]

// adding DocumentOrJSON (could be dbDoc instead of JSON, but probably not in Perl (only JSON in js/python/nodejs)
// var res = myColl.add({ "col1":1, "col2":"2"});     res.getLastDocumentId()
//myColl.add([ { "col1":2, "col2":"3"}, { "col1":3, "col9":"9"}, { "col1":3, "col2":"3"} ]);
// if you have a unique id, you can .add({ _id: 'your id', ...}) instead of relying on the default document ID
// also var res = col.add(...).execute(); res.getDocumentId(); (most recently added ID)
// also var res = col.add(...)..add(...)execute(); ids = res.getDocumentIds(); ids.forEach(function(id) { print(id) }); (or ids[1])

var myColl = myDb.getCollection(coll_name);

// named :params are only allowed in CRUD ops (one .bind() per param); name must not begin with a digit
// ? placeholders only in (nodesession) .sql() strings
myDocs = myColl.find('col1 = :param').bind('param', 3).execute();

// COLLECTION methods
// http://dev.mysql.com/doc/x-devapi-userguide/en/collection-crud-function-overview.html  for syntax diagrams
// add   (binding doesn't work for add)
// find(SearchConditionStr)  .fields(ProjectedDocumentExprStr) .groupBy(SearchExprStrList) .having(SearchConditionStr) .sort(SortExprStrList) .limit(NumberOfRows) .offset(NumberOfRows) .bind(PlaceholderValues) .execute()
// modify("name = :name").set(".age", 37).bind(params).execute();
// or modify('name = :param').set('parent_name',mysqlx.expr(':param')).bind('param', juniors[index].name).execute();
// modify(SearchConditionStr)   .set(CollectionField, ExprOrLiteral) | .unset(CollectionFields) | .arrayInsert(CollectionField, ExprOrLiteral) | .arrayAppend(CollectionField, ExprOrLiteral) .arrayDelete(CollectionField) .sort .limit .bind .execute
// var rm = remove('name = :param1 AND age = :param2')    // later: rm.bind...execute()
// remove(SearchConditionStr)  .sort .limit .bind .execute
// createIndex
// dropIndex
// getIndexes()
// newDoc
// count()

// http://dev.mysql.com/doc/x-devapi-userguide/en/working-with-data-sets.html
print(myDocs.fetchOne());    // there's also fetchAll(); "Connectors implementing the X DevAPI can offer more advanced iteration patterns"
print(myDocs.fetchOne());    // "When the last data item has been read and fetchOne() is called again a NULL value is returned."

// http://dev.mysql.com/doc/x-devapi-userguide/en/fetching-all-data-items-at-once.html
// "Asynchronous query executions return control to caller once a query has been issued and prior to receiving any reply from the server. Calling fetchAll() to read the data items produced by an asynchronous query execution may block the caller. fetchAll() cannot return control to the caller before reading results from the server is finished."

// http://dev.mysql.com/doc/x-devapi-userguide/en/working-with-sql-result-sets.html
// "Use the hasData() method to learn whether a SqlResult is a data set or a result" (the dataset may be empty)
// store procedure something... nextResult (or nextDataSet?)

// RowResult.getColumns() --> metadata
// Column[0].databaseName
// Column[0].tableName = NULL
// Column[0].tableLabel = NULL
// Column[0].columnName = NULL
// Column[0].columnLabel = "a"
// Column[0].type = BIGINT
// Column[0].length = 3
// Column[0].fractionalDigits = 0
// Column[0].numberSigned = TRUE
// Column[0].collationName = "binary"
// Column[0].characterSetName = "binary"
// Column[0].padded = FALSE
// Column[1].....

// EXPRESSIONS
// Boolean: find, remove
// Value: modify, update   expr('counter + 1')  basically to avoid it being a literal string


mySession.close();


// if TABLE (instead of COLLECTION)
// http://dev.mysql.com/doc/x-devapi-userguide/en/sql-crud-functions.html  for syntax diagrams
// insert(['col1', 'col2', etc...]).values('blah', mysqlx.dateValue(2000, 5, 27), 16).execute()
// or insert(['id','name']).values(1, 'Mike').values(2, 'Jack').execute()
// .insert(TableFields) .values(ExprOrLiterals) .execute()
// AUTO_INCREMENT: res = .insert; res.getFirstAutoIncrementValue() or when chaining: res.getAutoIncrementValues()
// select(['_id', 'name', 'age']).where('name like :param').orderBy(['name']).bind('param', 'm%').execute();
// .select .where .groupBy .having .orderBy .limit .offset .bind .execute
// async (not in mysql shell, but in nodejs): ...execute(function(row){ ... })
// update .set(TableField, ExprOrLiteral) .where .orderBy .limit .bind .execute
// delete .where .orderBy .limit .bind .execute
// dropIndex
// getIndexes
// count



// TRANSACTIONS

// sess.startTransaction();
// try {
//     myColl.add(....)
//     ...
//     var reply = sess.commit();
//     if (reply.warningCount()) {
//         var warnings = reply.getWarnings();
//         for (index in warnings){
//             var warning = warnings[index];
//             print ('Type ['+ warning.Level + '] (Code ' + warning.Code + '): ' + warning.Message + '\n');
//         }
//     }
// }
// catch(err) {
//     var reply = sess.rollback();
//
//     if (reply.warningCount) {
//         var warnings = reply.getWarnings();
//         for (index in warnings) {
//             var warning = warnings[index];
//             print ('Type ['+ warning.Level + '] (Code ' + warning.Code + '): ' + warning.Message + '\n');
//         }
//     }
//
//     print ('Data could not be inserted: ' + err.message);
// }
// finally {
//     // example: sess.close();
// }

// sess.setFetchWarnings(false)   to disable warnings


// RESULT SETS
// http://dev.mysql.com/doc/x-devapi-userguide/en/result-set-classes.html
// BaseResult: Result | DocResult | RowResult<--SqlResult


// EBNF of the API - pretty diagrams of syntax
// starting at http://dev.mysql.com/doc/x-devapi-userguide/en/mysql-x-crud-ebnf-definitions.html
// and http://dev.mysql.com/doc/x-devapi-userguide/en/mysql-x-expressions-ebnf-definitions.html
