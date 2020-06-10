function OnUpdate(doc, meta) {
  var {doc} = couchbase.decrement(src_bucket, {"id": "counter"});
  var suffix = 1024+doc.count;
  var id = "counter_"+suffix.toString(10);
  dst_bucket[id] = 'success';
}
