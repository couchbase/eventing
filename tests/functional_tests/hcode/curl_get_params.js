function OnUpdate(doc, meta) {
    var request = {
        path: '/url-params',
        params: {
            'key': 'value',
            1: 2,
            'array': ['yes', 'this', 'is', 'an', 'array']
        }
    };

    try {
        var response = curl('GET', localhost, request);
        log(response);
        if (!verifyResponse(response)) {
            throw 'inconsistent response';
        }
        dst_bucket[meta.id] = JSON.stringify(response);
    } catch (e) {
        log('error', e);
    }
}

function verifyResponse(response) {
    var expected = {
        status: 200
    };

    if (response.status !== expected.status) {
        return false;
    }
    return true;
}