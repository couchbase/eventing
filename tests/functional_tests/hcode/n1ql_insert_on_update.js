function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO `hello-world` ( KEY, VALUE  ) VALUES ( UUID() ,'Hello world' );
    query.execQuery();
}
