function OnDelete(doc) {
    log('document', doc);
    dst_bucket[doc.id] = 'hello world'
}