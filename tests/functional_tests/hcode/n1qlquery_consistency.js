function OnUpdate(doc, meta) {
    try {
        var limit = 5;
        var query = new N1qlQuery('SELECT * FROM `default` LIMIT $limit', {
            namedParams: {'$limit': limit},
            consistency: 'request'
        });

        var result = query.execQuery()
        var count = 0;
        for (var row of result) {
            ++count;
        }

        if (count == 5) {
            dst_bucket[meta.id] = 'pass';
        }
    } catch (e) {
        log('error', e);
    }
}
