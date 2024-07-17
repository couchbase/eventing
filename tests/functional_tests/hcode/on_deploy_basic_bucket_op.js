function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  dst_bucket["1"] = 'hello world';
  var doc_read = dst_bucket["1"];
  if (doc_read !== 'hello world') {
    delete dst_bucket["1"]
  }
}
