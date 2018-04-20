function OnUpdate(doc,meta) {
    expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, expiry, meta.id);
}
function timerCallback(docid) {
    dst_bucket[docid] = 'from timerCallback';
}

function OnDelete(meta) {
    log('deleting document', meta.id);
    delete dst_bucket[meta.id]; // DELETE operation
}
