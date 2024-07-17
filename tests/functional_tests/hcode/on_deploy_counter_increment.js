function OnUpdate(doc, meta) {}

function OnDeploy(action) {
  var { doc } = couchbase.increment(src_bucket, { "id": "counter" });
  var id = "counter_" + doc.count.toString(10);
  dst_bucket[id] = 'success';
}
