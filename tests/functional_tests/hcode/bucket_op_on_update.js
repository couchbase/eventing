function OnUpdate(doc, meta) {
    log('document', doc);
    dst_bucket[meta.id] = 'hello world';
}
function OnDelete(doc) {
}