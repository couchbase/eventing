function OnUpdate(doc, meta) {
    try {
        var result = new N1qlQuery('SELECT * FROM `hello-world` LIMIT 5', {
            namedParams: {},
            consistency: 'request'
        });

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
