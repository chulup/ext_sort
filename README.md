Do not forget to add environment variable PKG_CONFIG_PATH=<path_to_seastar>/build/release to CMake invocation!

Improvements:

1. Add real available memory test
2. Add options to use temp file on another disk and perform first sort step in multiple threads
3. Implement in-place sort or at least do not take twice the space necessary
