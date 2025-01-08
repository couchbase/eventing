function OnUpdate(doc, meta) {
  var request = {
    headers: {
      'Accept': 'image/png'
    },
    timeout: "abc"
  };

  try {
    var response = curl('GET', localhost, request);
    throw 'non integer check failed';
  } catch (e) {
    if (e.message != "'timeout' should be a positive integer") {
      throw e;
    }
  }

  request.timeout = -1;
  try {
    var response = curl('GET', localhost, request);
    throw 'positive integer check failed';
  } catch (e) {
    if (e.message != "'timeout' should be a positive integer") {
      throw e;
    }
  }

  request.timeout = 10;
  try {
    var response = curl('GET', localhost, request);
    throw 'expected timeout';
  } catch (e) {
    dst_bucket[meta.id] = {};
  }
}
