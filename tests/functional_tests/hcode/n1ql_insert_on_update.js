function OnUpdate(doc,meta) {
    var docId = meta.id;
    INSERT INTO `hello-world` ( KEY, VALUE  ) VALUES ( UUID() ,'Hello world' );
}
