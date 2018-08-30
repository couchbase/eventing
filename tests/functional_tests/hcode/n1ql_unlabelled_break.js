function OnUpdate(doc, meta) {
	var lim = 1,
	count = 0;

	// Unlabeled break
	var res1 = SELECT * FROM default LIMIT $lim;
	for(var row1 of res1) {
		var res2 = SELECT * FROM default LIMIT $lim;
		for(var row2 of res2) {
			var docId = meta.id + (++count);
			INSERT INTO `hello-world` (KEY, VALUE) VALUES ($docId, 'Hello world');
			break;
		}
	}
}

function OnDelete(meta) {
}
