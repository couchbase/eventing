function OnUpdate(doc, meta) {
    var request = {
        body: GetSomeJSONData()
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

function GetSomeJSONData() {
    return {
        'key': 'here comes some value from Couchbase Function'
    };
}

function verifyResponse(response) {
    var expected = {
        status: 200,
        headers: {
            "Content-Type": " application/json; charset=utf-8\r\n"
        },
        body: {
            is_body_consistent: true
        }
    };

    if (response.status !== expected.status) {
        return false;
    }
    if (response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    if (response.body.is_body_consistent !== expected.body.is_body_consistent) {
        return false;
    }
    return true;
}