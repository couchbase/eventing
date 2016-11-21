Go2C
====

Instead of using traditional CGO way to have Golang and C/C++ interact with each other, project is trying to have the communication
over local tcp port. There are advantages of this approach over CGO(though initial code complexity will be bit more, but it will
pay in long run):

* If CGO binding crashes, then it will take down all other OS processes spawned by Golang runtime. Main motivation for me to go
  down this path instead of CGO was because of a project where I was embedding Google V8 JS Runtime. Intention was to run each
  V8::Isolate under different OS process and if during executing some arbitrary user supplied JS code, if one V8::Isolate
  crashes - it shouldn’t take down other innocent OS processes running under their own V8::Isolate. Processing pipeline looked
  like:

  Golang(sends messages from some source) => C/C++ V8 binding(executes user supplied JS code against each of those messages)

* Debugging the C/C++ binding gets easier, as now one could hook in gdb, valgrind, asan, tsan etc easily. This isn’t trivial to
  do with traditional CGO way.


Run the project:
===============

Prerequisites - libuv, rapidjson, jemalloc(could use just system malloc as well)

$ go get -u github.com/abhi-bit/Go2C
$ make
