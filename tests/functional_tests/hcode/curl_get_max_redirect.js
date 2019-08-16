function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'application/json',
            'Redirect-Max': 'true'
        }
    };

    try {
        curl('GET', localhost, request);
    } catch (e) {
        var expected = {
            "message":"Unable to perform the request: Number of redirects hit maximum amount"
        }
        if(e['message'] === expected['message']) {
            dst_bucket[meta.id] = e;
        }
    }
}
