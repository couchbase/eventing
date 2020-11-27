function OnUpdate(doc, meta) {
  var start_long = new Date();
  for (var i=0; i<10000; i++) {
    couchbase.get(src_bucket, meta);
  }
  var long = (new Date()) - start_long;

  var start_short = new Date();
  for (var i=0; i<10000; i++) {
    couchbase.get(src_bucket, meta, {"cache": true});
  }
  var short = (new Date()) - start_short;

  log("Long", long, "Short", short, "Ratio", long/short);

  if (long/short > 2) {
    dst_bucket[meta.id] = 'success';
  }
}
