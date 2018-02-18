function OnUpdate(doc, meta) {
    //log('creating document on dst : ', meta.id);
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, meta.id, expiry);
}

function OnDelete(meta) {
    //log('deleting document: ', meta.id);
    var expiry = Math.round((new Date()).getTime() / 1000) + 30;
    cronTimer(NDtimerCallback, meta.id, expiry);
}

function timerCallback(docid) {
    dst_bucket2[docid] = 'from timerCallback';
}

function NDtimerCallback(docid) {
    delete dst_bucket2[docid];
}
