function OnUpdate(doc, meta) {
    try {
        let query1 = SELECT * FROM default;
        let query2 = SELECT * FROM default;
        let query3 = SELECT * FROM default;
        let query4 = SELECT * FROM default;
        let query5 = SELECT * FROM default;
        let query6 = SELECT * FROM default;
        let query7 = SELECT * FROM default;
        let query8 = SELECT * FROM default;
        let query9 = SELECT * FROM default;
        let query10 = SELECT * FROM default;
        let query11 = SELECT * FROM default;
    } catch (e) {
        if (e.message === 'Connection pool maximum capacity reached') {
            dst_bucket[meta.id] = 'Success';
            log(e)
        } else {
            log(e)
        }
    }
}