function OnUpdate(doc, meta) {
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 5);

    createTimer(Callback, fireAt, meta.id);
}

function Callback() {
    dst_bucket[uuidv4()] = "hello world";
}

function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    let r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}
