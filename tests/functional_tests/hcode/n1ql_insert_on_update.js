function OnUpdate(doc,meta) {
    let docId = meta.id;
    INSERT INTO `hello-world` ( KEY, VALUE  ) VALUES ( UUID() ,'Hello world' );
}
