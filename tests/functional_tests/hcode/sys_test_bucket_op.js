function OnUpdate(doc, meta) {
    //log('creating document on dst : ', meta.id);
    dst[meta.id] = "from bucket op";
}

function OnDelete(meta) {
    //log('deleting document: ', meta.id);
    delete dst[meta.id];
}
