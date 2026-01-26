# Examples CMakeLists.txt

# Basic example
add_executable(basic_example basic_example.cpp)
target_link_libraries(basic_example spdk_kv)

# Async example
add_executable(async_example async_example.cpp)
target_link_libraries(async_example spdk_kv)

# C API example (compiled as C++ due to header dependencies)
set_source_files_properties(c_api_example.c PROPERTIES LANGUAGE CXX)
add_executable(c_api_example c_api_example.c)
target_link_libraries(c_api_example spdk_kv)

# Benchmark
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark spdk_kv)
