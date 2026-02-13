cmake_minimum_required(VERSION 3.14)

project(spdk_kv_engine
    VERSION 0.1.0
    DESCRIPTION "SPDK-based KV Engine"
    LANGUAGES CXX
)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(SPDK_KV_SOURCES
    src/engine.cpp
    src/checkpoint.cpp
    src/recovery.cpp
    src/compaction.cpp
    src/spdk_env.cpp
    src/io_submitter.cpp
    src/alloc_log.cpp
)

# Main library
add_library(spdk_kv STATIC ${SPDK_KV_SOURCES})
target_include_directories(spdk_kv PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Shared library
add_library(spdk_kv_shared SHARED ${SPDK_KV_SOURCES})
target_include_directories(spdk_kv_shared PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(spdk_kv_shared PROPERTIES OUTPUT_NAME spdk_kv)

# SPDK integration
find_package(PkgConfig REQUIRED)
pkg_check_modules(SPDK REQUIRED spdk_bdev spdk_env_dpdk)

# Find additional SPDK dependencies
find_package(OpenSSL REQUIRED)
pkg_check_modules(UUID REQUIRED uuid)
find_library(ISAL_LIB isal HINTS /usr/local/lib)
find_library(ISAL_CRYPTO_LIB isal_crypto HINTS /usr/local/lib)

# Static library
target_include_directories(spdk_kv PUBLIC ${SPDK_INCLUDE_DIRS})
target_link_libraries(spdk_kv PUBLIC
    ${SPDK_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    ${UUID_LIBRARIES}
    ${ISAL_LIB}
    ${ISAL_CRYPTO_LIB}
    numa
    pthread
    dl
    rt
)

# Shared library
target_include_directories(spdk_kv_shared PUBLIC ${SPDK_INCLUDE_DIRS})
target_link_libraries(spdk_kv_shared PUBLIC
    ${SPDK_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    ${UUID_LIBRARIES}
    ${ISAL_LIB}
    ${ISAL_CRYPTO_LIB}
    numa
    pthread
    dl
    rt
)

# Examples
add_subdirectory(examples)

# Install
install(TARGETS spdk_kv spdk_kv_shared
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/spdk_kv
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

# Package configuration
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/spdk_kv-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

# Summary
message(STATUS "")
message(STATUS "SPDK KV Engine Configuration:")
message(STATUS "  Version:     ${PROJECT_VERSION}")
message(STATUS "  Build type:  ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  SPDK:        ON")
message(STATUS "")
