#!/bin/bash

install_name_tool -change libinspector.dylib  $HOME/.cbdepscache/lib/libinspector.dylib client.bin
install_name_tool -change @rpath/libcouchbase.dylib $HOME/.cbdepscache/lib/libcouchbase.dylib client.bin
install_name_tool -change @rpath/libicui18n.56.1.dylib $HOME/.cbdepscache/lib/libicui18n.56.1.dylib client.bin
install_name_tool -change @rpath/libicuuc.56.1.dylib $HOME/.cbdepscache/lib/libicuuc.56.1.dylib client.bin
install_name_tool -change @rpath/libjemalloc.2.dylib $HOME/.cbdepscache/lib/libjemalloc.2.dylib client.bin
install_name_tool -change @rpath/libv8.dylib $HOME/.cbdepscache/lib/libv8.dylib client.bin
install_name_tool -change @rpath/libv8_libplatform.dylib $HOME/.cbdepscache/lib/libv8_libplatform.dylib client.bin
install_name_tool -change @rpath/libv8_libbase.dylib $HOME/.cbdepscache/lib/libv8_libbase.dylib client.bin

install_name_tool -change @rpath/libjemalloc.2.dylib $HOME/.cbdepscache/lib/libjemalloc.2.dylib eventing

install_name_tool -change @rpath/libicui18n.56.1.dylib $HOME/.cbdepscache/lib/libicui18n.56.1.dylib $HOME/.cbdepscache/lib/libinspector.dylib
install_name_tool -change @rpath/libicuuc.56.1.dylib $HOME/.cbdepscache/lib/libicuuc.56.1.dylib $HOME/.cbdepscache/lib/libinspector.dylib
install_name_tool -change @rpath/libv8.dylib $HOME/.cbdepscache/lib/libv8.dylib $HOME/.cbdepscache/lib/libinspector.dylib
install_name_tool -change @loader_path/libv8_libbase.dylib $HOME/.cbdepscache/lib/libv8_libbase.dylib $HOME/.cbdepscache/lib/libinspector.dylib
install_name_tool -change @loader_path/libv8_libplatform.dylib  $HOME/.cbdepscache/lib/libv8_libplatform.dylib $HOME/.cbdepscache/lib/libinspector.dylib

install_name_tool -change @loader_path/libv8_libbase.dylib $HOME/.cbdepscache/lib/libv8_libbase.dylib $HOME/.cbdepscache/lib/libv8_libplatform.dylib

install_name_tool -change @loader_path/libicui18n.dylib $HOME/.cbdepscache/lib/libicui18n.dylib $HOME/.cbdepscache/lib/libv8.dylib
install_name_tool -change @loader_path/libicuuc.dylib $HOME/.cbdepscache/lib/libicuuc.dylib $HOME/.cbdepscache/lib/libv8.dylib
install_name_tool -change @loader_path/libv8_libbase.dylib $HOME/.cbdepscache/lib/libv8_libbase.dylib $HOME/.cbdepscache/lib/libv8.dylib
install_name_tool -change @loader_path/libv8_libplatform.dylib $HOME/.cbdepscache/lib/libv8_libplatform.dylib $HOME/.cbdepscache/lib/libv8.dylib
