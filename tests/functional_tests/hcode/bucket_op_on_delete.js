function OnDelete(meta) {
    log('metadata', meta);
    dst_bucket[meta.id + "abc"] = 'hello world'
}
