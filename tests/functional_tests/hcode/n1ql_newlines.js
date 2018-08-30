function OnUpdate(doc, meta) {
    var docId = meta.id;

    // This comment is supposed to cause problem with N1QL compilation
    INSERT
        INTO            `hello-world`
                (KEY, VALUE)                VALUES ( UUID() ,'Hello world' );}