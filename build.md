# Building Couchbase server with Functions
### Dependencies
The dependencies are `libuv`, `v8 5.9.211` and `libcouchbase >= 2.7.5`
#### For Mac OS X
##### libuv
```bash
$ brew upgrade
$ brew install libuv
```

If you have already installed libuv, then upgrade it to the latest
```bash
$ brew upgrade libuv
```
Download the following dependencies and put them to `~/.cbdepscache/`

##### v8

https://drive.google.com/open?id=0B4VRo9qU8CtkNWtoc0NGQmUtUEE <br>
https://drive.google.com/open?id=0B4VRo9qU8CtkS190UDBqSlhWTWM

##### icu56
https://drive.google.com/open?id=0B4VRo9qU8CtkYmpfRjdNQWE1OVE <br>
https://drive.google.com/open?id=0B4VRo9qU8CtkUER1QlBhcUhwRU0

##### libcouchbase
https://drive.google.com/open?id=0B4VRo9qU8CtkQlVMZmhFTHR5cGM <br>
https://drive.google.com/open?id=0B4VRo9qU8CtkUUJ4M0tMRDJUWlk

### Clone and build Couchbase server
```bash
$ repo init -u git://github.com/couchbase/manifest.git -m toy/toy-eventing.xml -g all
$ repo sync --jobs=20
$ BUILD_ENTERPRISE=ON make -j8
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

### Logs
From ns_server directory,
```bash
$ tail -f logs/n_0/eventing.log
```

---

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