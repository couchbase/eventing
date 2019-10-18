function OnUpdate(doc, meta) {
    try {
        var value = dst_bucket['some-non-existent-key'];
        if (value === undefined) {
            dst_bucket[meta.id] = 'success';
        }
    } catch (e) {
        log('error:', e);
    }
}