function OnUpdate(doc, meta) {
    if (meta.datatype !== "binary") {
        return;
    }

    // test for insert operation
    var {
        success
    } = couchbase.insert(dst_bucket, {
        "id": meta.id + "_binary"
    }, doc);
    if (!success) {
        return;
    }

    if (!getAndCheck(meta.id, doc, dst_bucket)) {
        return;
    }
    delete dst_bucket[meta.id + "_binary"]
    delete dst_bucket[meta.id];
}

function OnDelete(meta, options) {
    var arr = new Uint8Array([1, 0, 2, 3]);
    src_bucket[meta.id + "_binary"] = arr.buffer;

    var returned_doc = src_bucket[meta.id + "_binary"];

    if (!(returned_doc instanceof ArrayBuffer)) {
        return;
    }

    if (!equal(returned_doc, arr)) {
        return;
    }

    dst_bucket[meta.id] = "success";
}

function getAndCheck(id, doc, bucket) {
    var res = couchbase.get(bucket, {
        "id": id + "_binary"
    });
    if (!res.success) {
        return false;
    }

    if (!(res.doc instanceof ArrayBuffer) || res.meta.datatype !== "binary") {
        return false;
    }

    if (!equal(doc, res.doc)) {
        return false;
    }
    return true;
}

function equal(buf1, buf2) {
    if (buf1.byteLength != buf2.byteLength) return false;
    var dv1 = new Uint8Array(buf1);
    var dv2 = new Uint8Array(buf2);
    for (var i = 0; i != buf1.byteLength; i++) {
        if (dv1[i] != dv2[i]) return false;
    }
    return true;
}