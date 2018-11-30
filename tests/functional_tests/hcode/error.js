function OnUpdate(doc, meta) {
    TestErrorType(KVError, meta.id + 'KVError');
    TestErrorType(N1QLError, meta.id + 'N1QLError');
    TestErrorType(EventingError, meta.id + 'EventingError');
}

function TestErrorType(T, id) {
    try {
        throw new T();
    } catch (e) {
        if(e instanceof Error && e instanceof T) {
            dst_bucket[id] = 'Custom error instance';
        }
    }
}