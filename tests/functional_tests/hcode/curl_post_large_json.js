function OnUpdate(doc, meta) {
    var size = 5 * 1024 * 1024;
    var data = GetSomeJSONData(size);
    var request = {
        body: data,
        headers: {
            'Body-Size' : JSON.stringify(data).length
        }
    };

    try {
        var response = curl('POST', localhost, request);
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
    var s = '';
    // 5 MB
    for (var i = 0; i < size; ++i) {
        s += '1';
    }
    return {
        'key': s
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