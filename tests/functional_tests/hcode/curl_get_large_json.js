function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'application/json'
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
    var size = parseInt(response.headers['Value-Size']);
    var value = response.body.key;
    if (size !== value.length) {
        return false;
    }

    for (var i = 0; i < size; ++i) {
        if (value[i] !== '1') {
            return false;
        }
    }
    return true;
}