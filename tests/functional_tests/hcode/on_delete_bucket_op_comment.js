function OnUpdate(doc, meta) {
    let doc_id = meta.id; // GET operation
    log('creating document for : ', doc);
    dst_bucket[doc_id] = {'doc_id' : doc_id}; // SET operation
}

// This is intentionally left blank


























function OnDelete(meta) {
    log('deleting document', meta.id);
    // delete dst_bucket[meta.id]; // DELETE operation
}
