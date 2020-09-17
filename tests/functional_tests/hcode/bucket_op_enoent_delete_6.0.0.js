function OnUpdate(doc, meta) {
    try {
        delete dst_bucket['some-non-existent-key'];
    } catch (e) {
        dst_bucket[meta.id] = 'success';
        log('error:', e);
    }
}
