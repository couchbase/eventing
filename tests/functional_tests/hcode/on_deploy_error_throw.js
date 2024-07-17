function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  var meta = { "id": "1" };
  TestErrorType(KVError, meta.id + 'KVError');
  TestErrorType(N1QLError, meta.id + 'N1QLError');
  TestErrorType(EventingError, meta.id + 'EventingError');
}

function TestErrorType(T, id) {
  try {
    throw new T();
  } catch (e) {
    if (e instanceof Error && e instanceof T) {
      dst_bucket[id] = 'Custom error instance';
    }
  }
}
