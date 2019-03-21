function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'application/malformed-json'
        }
    };

    try {
        curl('GET', localhost, request);
        log('Must not reach here');
    } catch (e) {
        dst_bucket[meta.id] = JSON.stringify(e);
    }
}