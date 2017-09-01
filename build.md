# Building Couchbase server with Functions
### Dependencies
Currently, eventing only works on Mac OS as necessary dependencies
are still being added to deps mechanism.

### Clone and build eventing toy 
```bash
$ repo init -u git://github.com/couchbase/manifest.git -m toy/toy-eventing.xml -g all
$ repo sync --jobs=20
$ make -j8
```

Make sure that the number of open files under ulimit is atleast 5000.
It can be set by -
```bash
$ echo "ulimit -n 5000" > ~/.bashrc or ~/.bash_profile or ~/.zshrc
```

### Starting the server and cluster setup
```bash
$ cd install/bin
$ ./couchbase-server
```

### Add admin user
Open the console and in security, add an `eventing` user with admin role 

### Add sample bucket
Add the `beer-sample` bucket from `Settings > Sample buckets`.
Under the Query tab execute –
```sql
CREATE PRIMARY INDEX ON `beer-sample`;
```

### Logs
```bash
$ tail -f install/var/lib/couchbase/eventing.log
```

# Samples
Please create the following buckets before trying the sample applications -
1) default
2) eventing
3) hello-world

The sample can be found in the [samples](https://github.com/couchbase/eventing/tree/master/samples) directory. You can import them using the `import` functionality under the Functions tab. The imported functions will be disabled by default. You need to `deploy` them to spin them up into action.

# Creating a function
Let us try creating a function. Consider the following example -<br/>
We want to monitor the `beer-sample` bucket for those beers whose `abv > 20` and add those into the `default` bucket.

> Please make sure that you have added beer-sample bucket and have created a primary index on it.

Under Functions tab, click on `Create`. Set `Source bucket` to `beer-sample` and `Metadata bucket` to `eventing`. Give a name and enter the `RBAC Credentials`. Click `Continue`.

You will now be taken to the editor page where you can define the logic of your function. Replace the text with the content below.
```javascript
function OnUpdate(doc, meta) {
    var bucket = '`beer-sample`';
    var abvLim = 20;

    // Select all the beers whose abv value > 20.
    var res =   SELECT name, abv
                FROM :bucket
                WHERE abv > :abvLim;

    for(var row of res) {
		var name = row.name;
		var data = JSON.stringify(row);

		// Upsert these rows into abv_bucket.
		var ins = UPSERT INTO default (KEY, VALUE) VALUES (':name', :data);
		ins.execQuery();
	}
}

function OnDelete(msg){
}
```
Go back to the Functions tab and hit `Deploy`.
