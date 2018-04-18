function OnUpdate(doc,meta) {
    expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, expiry, meta.id);
}
function NDtimerCallback(docid) {
    dst_bucket[docid] = 'from NDtimerCallback';
}
