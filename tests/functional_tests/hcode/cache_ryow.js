function OnUpdate(doc, meta) {
  dst_bucket[meta.id] = {"state": "original"};
  for (var i=0; i<10; i++) {
    couchbase.get(dst_bucket, meta, {"cache": true});
  }
  dst_bucket[meta.id] = {"state": "intermediate"};
  for (var i=0; i<10; i++) {
    var obj = couchbase.get(dst_bucket, meta, {"cache": true});
    if (obj.doc.state != "intermediate") {
      log("cache test failed. expected state=intermediate but saw", obj);
      delete dst_bucket[meta.id];
      return;
    }
  }
  var mod = {"state": "final"};
  couchbase.set(dst_bucket, meta, mod);
  for (var i=0; i<10; i++) {
    var obj = couchbase.get(dst_bucket, meta, {"cache": true});
    if (obj.doc.state != "final") {
      log("cache test failed. expected state=final but saw", obj);
      delete dst_bucket[meta.id];
      return;
    }
  }
}
