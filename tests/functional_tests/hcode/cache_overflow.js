function OnUpdate(doc, meta) {
  try {
    var n = 6; // objects needed to overflow cache
    var bigobj = {
      "junk": '1'.repeat(15 * 1024 * 1024),
      "stamp": "old"
    };

    // create n big docs
    for (var i=0; i<n; i++) {
      var key = "big-" + i.toString() + "-" + meta.id;
      dst_bucket[key] = bigobj;
    }

    // prime the cache - it will overflow as default max size is 64MB
    for (var i=0; i<n; i++) {
      var key = "big-" + i.toString() + "-" + meta.id;
      couchbase.get(dst_bucket, {id: key}, {"cache": true});
    }

    // invalidates cache as it does not know about N1QL
    var spec = "big-%-" + meta.id;
    UPDATE `hello-world` SET stamp = "new" WHERE META().id LIKE $spec;

    // read the docs via cache. we must see misses
    var ok = false;
    for (var i=0; i<n; i++) {
      var key = "big-" + i.toString() + "-" + meta.id;
      var obj = couchbase.get(dst_bucket, {id: key}, {"cache": true});
      if (obj.doc.stamp === "new") {
        ok = true;
        break;
      }
    }

    // if everything is old, cache did not overflow
    if (!ok) {
      log("Test failed, all 90MB docs cached despite cache size of 64MB");
      return;
    }

    // if we see some new docs, cache overflowed and all is good
    dst_bucket[meta.id] = "pass";
  } catch (err) {
    log(err);
  }
}
