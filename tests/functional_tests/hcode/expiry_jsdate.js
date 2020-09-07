function OnUpdate(doc, meta) {
  log(meta);
  var dcpExpiry = meta.expiration;
  var jsExpiry = meta.expiry_date;
  if (jsExpiry.getTime() === dcpExpiry * 1000) {
    delete src_bucket[meta.id];
  }
}
