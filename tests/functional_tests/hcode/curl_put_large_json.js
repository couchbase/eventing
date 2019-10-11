function OnUpdate(doc, meta) {
    var size = 5 * 1024 * 1024;
    var empty = GetSomeJSONData(0);
    var data = GetSomeJSONData(size);
    var request = {
        body: data,
        headers: {
            'Body-Size' : JSON.stringify(empty).length + size
        }
    };

    try {
        var response = curl('PUT', localhost, request);
        log(response);
        if (!verifyResponse(response)) {
            throw 'inconsistent response';
        }

        dst_bucket[meta.id] = JSON.stringify(response);
    } catch (e) {
        log(e);
    }
}

function GetSomeJSONData(size) {
    return {
        'key': '1'.repeat(size)
    };
}

function verifyResponse(response) {
    var expected = {
        status: 200,
    };

    if (response.status !== expected.status) {
        return false;
    }
    return true;
}
