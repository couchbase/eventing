function OnUpdate(doc, meta, xattrs) {
    if(!xattr.test_xattr) {
        return;
    }

    log('document', doc);
    dst_bucket[meta.id] = 'hello world';
}
function OnDelete(meta) {
}
