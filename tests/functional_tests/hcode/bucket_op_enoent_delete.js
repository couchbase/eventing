function OnUpdate(doc, meta) {
    try {
        delete dst_bucket['some-non-existent-key'];
        dst_bucket[meta.id] = 'success';
    } catch (e) {
        log('error:', e);
    }
}