function OnUpdate(doc, meta) {
  var expiry = new Date();
  expiry.setSeconds(expiry.getSeconds() + 10);

  var req = {"id": meta.id+"_Handler", "expiry_date": expiry};
  couchbase.insert(dst_bucket, req, 'success');
}

function OnDelete(meta, options) {
  if(options.expired) {
    var id = meta.id.slice(0, -8);
    delete dst_bucket[id];
  }
}
