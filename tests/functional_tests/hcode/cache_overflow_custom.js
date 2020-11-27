function OnUpdate(doc, meta) {
  try {
    var n = 6; // objects needed to overflow default cache
    var bigobj = {
      "junk": '1'.repeat(15 * 1024 * 1024),
      "stamp": "old"
    };

    // create n big docs
    for (var i=0; i<n; i++) {
      var key = "big-" + i.toString() + "-" + meta.id;
      dst_bucket[key] = bigobj;
    }

    // prime the cache
    for (var i=0; i<n; i++) {
      var key = "big-" + i.toString() + "-" + meta.id;
      couchbase.get(dst_bucket, {id: key}, {"cache": true});
    }

    // invalidates cache as it does not know about N1QL
    var spec = "big-%-" + meta.id;
    UPDATE `hello-world` SET stamp = "new" WHERE META().id LIKE $spec;

    // read the docs via cache. we must see only stale docs as cache is large
    var ok = true;
    for (var i=0; i<n; i++) {
      var key = "big-" + i.toString() + "-" + meta.id;
      var obj = couchbase.get(dst_bucket, {id: key}, {"cache": true});
      if (obj.doc.stamp === "new") {
        ok = false;
        break;
      }
    }

    // if we see new docs, cache overflowed
    if (!ok) {
      log("Test failed, saw new docs");
      return;
    }

    // saw no new docs, and is good as we set a big cache size
    dst_bucket[meta.id] = "pass";
  } catch (err) {
    log(err);
  }
}
