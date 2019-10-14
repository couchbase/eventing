function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        destination[meta.id] = 00000000000000000000000000000101;
    } catch (e) {
        log(e);
    }
}
