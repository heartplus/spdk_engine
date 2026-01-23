# Examples CMakeLists.txt

# Basic example
add_executable(basic_example basic_example.cpp)
target_link_libraries(basic_example PRIVATE spdk_kv)

# Benchmark example
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark PRIVATE spdk_kv)

# C API example (compiled as C++ to link with C++ library)
add_executable(c_api_example c_api_example.cpp)
target_link_libraries(c_api_example PRIVATE spdk_kv)

# Async example
add_executable(async_example async_example.cpp)
target_link_libraries(async_example PRIVATE spdk_kv)
