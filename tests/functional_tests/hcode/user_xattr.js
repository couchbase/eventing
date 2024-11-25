function OnUpdate(doc, meta, xattr) {
    log(xattr, meta);
    if (xattr.user_xattrs.testField != "replace") {
        return
    }

    var arr = xattr.user_xattrs.arrayTest;
    for(var index = 0; index < 4; index++) {
        if (arr[index] != index) {
            return
        }
    }
    delete dst_bucket[meta.id]
}

function OnDelete(meta, option) {
    meta = {"id": meta.id};
    log(meta)

    dst_bucket[meta.id] = {};
    var result = couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.insert("user_xattrs", {"testField" : "insert"}, {"xattrs": true})
    ]);
    if (!result.success) {
        log(result);
        return;
    }

    result = couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.upsert("user_xattrs", {"testField" : "upsert"}, {"xattrs": true})
    ]);
    if (!result.success) {
        log(result);
        return;
    }

    result = couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.replace("user_xattrs", {"testField" : "replace"}, {"xattrs": true})
    ]);
    if (!result.success) {
        log(result);
        return;
    }

    result = couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.upsert("user_xattrs.arrayTest", [], {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayAppend("user_xattrs.arrayTest", 2, {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayPrepend("user_xattrs.arrayTest", 1, {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayInsert("user_xattrs.arrayTest[0]", 0, {"create_path": true, "xattrs": true}),
        couchbase.MutateInSpec.arrayAddUnique("user_xattrs.arrayTest", 3, {"create_path": true, "xattrs": true})
    ]);
    if (!result.success) {
        log(result);
        return;
    }
}
