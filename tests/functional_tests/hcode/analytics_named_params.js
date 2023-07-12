function OnUpdate(doc, meta) {
    var count = 0;
    const limit = 4;

    let query = couchbase.analyticsQuery('SELECT * FROM default LIMIT $limit;', {"limit": limit});
    for (let row of query) {
        ++count;
    }

    if (count === limit) {
        dst_bucket[meta.id] = 'yes';
    }
}
