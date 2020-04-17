function OnUpdate(meta, doc) {
}

function OnDelete(meta, options) {
    if(options.expired) {
        log('metadata: ', meta, ' exipired');
        dst_bucket[meta.id + "expired"] = 'hello world'
    }
}
