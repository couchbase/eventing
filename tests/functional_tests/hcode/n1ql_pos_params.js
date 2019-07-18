function OnUpdate(doc, meta) {
    let count = 0,
        limit = 5;

    let query = N1QL('SELECT * FROM default LIMIT $1;', [limit]);
    for (let row of query) {
        ++count;
    }

    if (count === limit) {
        dst_bucket[meta.id] = 'yes';
    }
}