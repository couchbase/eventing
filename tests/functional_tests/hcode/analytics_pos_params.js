function OnUpdate(doc, meta) {
    let count = 0;
    const limit = 5;

    let query = couchbase.analyticsQuery('SELECT * FROM default LIMIT $1;', [limit]);
    for (let row of query) {
        ++count;
    }

    if (count === limit) {
        dst_bucket[meta.id] = 'yes';
    }
}
