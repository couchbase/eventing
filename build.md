# Building Couchbase server with Eventing
### Dependencies
We need to install the latest version of `libuv` and `libcouchbase >= 2.7.5`
#### For Mac OS X
```bash
$ brew upgrade
$ brew install libuv
$ brew install libcouchbase
```

If you have already installed them, then upgrade them to the latest
```bash
$ brew upgrade libuv
$ brew upgrade libcouchbase
```

### Clone and build Couchbase server
```bash
$ repo init -u git://github.com/couchbase/manifest.git -m toy/toy-eventing.xml
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
$ cd ns_server
$ ./cluster_run –n1
$ ./cluster_connect –n1
```

### Set Memory-Optimized Global Secondary Indexes
```bash
$ curl -X POST http://localhost:9102/settings -u Administrator:asdasd -d '{"indexer.settings.storage_mode": "memory_optimized"}'
```

### Add admin user
Add `eventing` user as admin with password `asdasd`
```bash
$ curl -u Administrator:asdasd -v -XPUT -d "password=asdasd&roles=admin" http://localhost:9000/settings/rbac/users/local/eventing
```

### Add sample bucket
Add the `beer-sample` bucket from `Settings > Sample buckets`.
Under the Query tab execute –
```sql
CREATE PRIMARY INDEX ON `beer-sample`;
```

Now you can goto Eventing and create and deploy apps.
>Please remember to add user in the application settings for each app that you create.

### Logs
From ns_server directory,
```bash
$ tail -f logs/n_0/eventing.log
```

---

# Sample application
Let us consider the following example -<br/>
We want to monitor the `beer-sample` bucket for those beers whose `abv > 20` and add those into another bucket called `abv_bucket`.

Please do the following before proceeding with this example –
1) Create a bucket `abv_bucket`.
2) Under the Query tab and execute –
```sql
CREATE PRIMARY INDEX ON abv_bucket;
```

Under 'Eventing' tab, create a new application.

### Handler
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
		var ins = UPSERT INTO abv_bucket (KEY, VALUE) VALUES (':name', :data);
		ins.execQuery();
	}
}

function OnDelete(msg){
}
```

### Deployment plan
```json
{
"buckets": [
  {
   "alias": "abv_bucket",
   "bucket_name": "abv_bucket"
  }
],
"metadata_bucket": "eventing",
"source_bucket": "beer-sample"
}
```

### Settings
Make sure that you’ve added user `eventing` with password `asdasd`.
Set the log level to `Trace` incase you want to see the log messages in `eventing.log`.