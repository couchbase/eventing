#!/bin/bash
install_name_tool -change @loader_path/libv8.dylib /Users/$USER/.cbdepscache/lib/libv8.dylib client.bin
install_name_tool -change @rpath/libjemalloc.2.dylib /Users/$USER/.cbdepscache/lib/libjemalloc.2.dylib client.bin
install_name_tool -change @loader_path/libicui18n.dylib /Users/$USER/.cbdepscache/lib/libicui18n.dylib ~/.cbdepscache/lib/libv8.dylib
install_name_tool -change @loader_path/libicuuc.dylib /Users/$USER/.cbdepscache/lib/libicuuc.dylib ~/.cbdepscache/lib/libv8.dylib
