function OnUpdate(doc, meta) {
	var lim = 1,
	count = 0;

	// Labeled break
	var res1 = SELECT * FROM default LIMIT $lim;
	for(var row1 of res1) {
		var res2 = SELECT * FROM default LIMIT $lim;
		x: for(var row2 of res2) {
			var res3 = SELECT * FROM default LIMIT $lim;
			for(var row3 of res3) {
				var docId = meta.id + (++count);
				var ins = INSERT INTO `hello-world` (KEY, VALUE) VALUES ($docId, 'Hello world');
				ins.execQuery();
				break x;
			}
		}
	}
}

function OnDelete(meta) {
}
