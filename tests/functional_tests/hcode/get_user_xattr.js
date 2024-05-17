function OnUpdate(doc, meta) {
    meta = {"id": meta.id};

    couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.upsert("my_key", "hello world", {"xattrs": true}),
        couchbase.MutateInSpec.upsert("user_xattrs", {"testField" : "insert"}, {"xattrs": true})
    ]);

    couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.upsert("user_xattrs.arrayTest", [], {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayAppend("user_xattrs.arrayTest", 2, {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayPrepend("user_xattrs.arrayTest", 1, {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayInsert("user_xattrs.arrayTest[0]", 0, {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayAddUnique("user_xattrs.arrayTest", 3, {"create_path": true, "xattrs": true})
    ]);


    // Test fetching a single subdocument path
    var result = couchbase.lookupIn(dst_bucket, meta, [
        couchbase.LookupInSpec.get("my_key", {"xattrs": true})
    ]);
    if(!result.success) {
        log(result);
        return;
    }
    var val = result.doc[0].value;
    var success = result.doc[0].success;
    if (val !== "hello world" || success === false) {
        log(result);
        return;
    }


    // Test fetching multiple subdocument paths
    result = couchbase.lookupIn(dst_bucket, meta, [
        couchbase.LookupInSpec.get("user_xattrs.arrayTest", {"xattrs": true}),
        couchbase.LookupInSpec.get("user_xattrs.testField", {"xattrs": true})
    ]);
    if(!result.success) {
        log(result);
        return;
    }
    var array = result.doc[0].value;
    success = result.doc[0].success;
    if (array.length !== 4 || success === false) {
        log(result);
        return;
    }
    for(var i=0; i<4; i++) {
        if(array[i] !== i) {
            log(result);
            return;
        }
    }
    var test_field = result.doc[1].value;
    success = result.doc[1].success;
    if (test_field !== "insert" || success === false) {
        log(result);
        return;
    }


    // Test a subdocument path not present in the xattrs
    result = couchbase.lookupIn(dst_bucket, meta, [
        couchbase.LookupInSpec.get("random_key", {"xattrs": true})
    ])
    if(!result.success) {
        log(result);
        return;
    }
    val = result.doc[0].value;
    success = result.doc[0].success;
    if (val !== undefined || success === true) {
        log(result);
        return;
    }

    delete dst_bucket[meta.id];
}