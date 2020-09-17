function OnUpdate(doc, meta) {
    log('docId', meta.id);
    var docId = meta.id;
    var upsertq = UPSERT INTO `bucket-1` (KEY, VALUE) VALUES ($docId, "Hello from handler 3");
    upsertq.close();
}
function OnDelete(meta) {
}
