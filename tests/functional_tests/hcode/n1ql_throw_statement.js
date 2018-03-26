function OnUpdate(doc, meta) {
	var lim = 1,
	count = 0;

	// Throw
	var res1 = SELECT * FROM default LIMIT $lim;
	for(var row1 of res1) {
		var res2 = SELECT * FROM default LIMIT $lim;
			try{
				for(var row2 of res2) {
					var res3 = SELECT * FROM default LIMIT $lim;
					for(var row3 of res3) {
						var docId = meta.id + (++count);
						INSERT INTO `hello-world` (KEY, VALUE) VALUES ($docId, 'Hello world');
						throw 'Error';
					}
				}
			} catch(e) {
			}
	}
}

function OnDelete(meta) {
}
