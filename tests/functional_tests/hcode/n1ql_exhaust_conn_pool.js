function OnUpdate(doc, meta) {
    const lcbConnPoolSize = 5;
    let count = 0;

    try {
        for (let i = 0; i < lcbConnPoolSize + 10; ++i) {
            let query = SELECT * FROM default;
            for (let row of query) {
                ++count;
                break;
            }
        }
    } catch (e) {
        if (e.message === 'Connection pool maximum capacity reached' && count === lcbConnPoolSize) {
            dst_bucket[meta.id] = 'yes';
        } else {
            log (e, count);
        }
    }
}