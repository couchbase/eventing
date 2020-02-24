function OnUpdate(doc, meta) {
    log('docId', meta.id);
    var docId = meta.id;
    var upsertq = UPSERT INTO `bucket-2` (KEY, VALUE) VALUES ($docId, "Hello from handler 1");
    upsertq.close();
}
function OnDelete(meta) {
}