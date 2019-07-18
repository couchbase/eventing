function OnUpdate(doc, meta) {
	let lim = 2,
	count = 0;

	// Unlabeled break
	let res1 = SELECT * FROM default LIMIT $lim;
	for(let row1 of res1) {
		let res2 = SELECT * FROM default LIMIT $lim;
		for(let row2 of res2) {
			let docId = meta.id + (++count);
			INSERT INTO `hello-world` (KEY, VALUE) VALUES ($docId, 'Hello world');

    		res2.close();
			break;
		}
	}
}

function OnDelete(meta) {
}
