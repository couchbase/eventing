function OnUpdate(doc, meta) {
	let lim = 2,
	count = 0;

	// Throw
	let res1 = SELECT * FROM default LIMIT $lim;
	for(let row1 of res1) {
		let res2 = SELECT * FROM default LIMIT $lim;
        try{
            for(let row2 of res2) {
                var res3 = SELECT * FROM default LIMIT $lim;
                for(let row3 of res3) {
                    let docId = meta.id + (++count);
                    INSERT INTO `hello-world` (KEY, VALUE) VALUES ($docId, 'Hello world');
                    throw 'Error';
                }
            }
        } catch(e) {
            res3.close();
        }
	}
}

function OnDelete(meta) {
}
