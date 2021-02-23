function OnUpdate(doc, meta) {
  var id = meta.id;
  log("Starting for", id);
  try {
    const lcbConnPoolSize = 10;
    for (var i=0; i< 2 * lcbConnPoolSize; i++) {
      LeakHandles(id);
    }
    INSERT INTO `hello-world`(KEY,VALUE) VALUES($id,'Hello world');
  }
  catch (e) {
    log ("Error for", id, e);
  }
  log("Finished for", id);
}

function LeakHandles(id) {
  var res = SELECT * FROM `default`;
  var count = 0;
  for (var row of res) {
    count++;
    if (count > 5) {
       log("Leaking handle for", id);
       return;
    }
  }
}
