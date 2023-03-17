function OnUpdate(doc, meta) {
  if (!doc.count) {
    doc = { "count": 1, "id": meta.id };
    meta.id = meta.id + "_test";
    couchbase.insert(src, meta, doc, { "self_recursion": true });
    return;
  }

  if (doc.count < 3) {
    doc.count++
    couchbase.upsert(src, meta, doc, { "self_recursion": true });
    return;
  }

  if (doc.count < 6) {
    doc.count++;
    couchbase.replace(src, meta, doc, { "self_recursion": true });
    return;
  }

  couchbase.delete(src, { "id": meta.id });
  couchbase.delete(src, { "id": doc.id });
}
