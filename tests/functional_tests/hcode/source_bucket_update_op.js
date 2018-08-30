function OnUpdate(doc, meta) {
    log('document', doc);
    src_bucket[meta.id + "11111111"] = doc;
}
function OnDelete(meta) {
    delete src_bucket[meta.id + "11111111"];
}
