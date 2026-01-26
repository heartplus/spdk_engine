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

# Option for SPDK integration (enabled by default)
option(WITH_SPDK "Build with SPDK support" ON)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(SPDK_KV_SOURCES
    src/engine.cpp
    src/mem_index.cpp
    src/append_buffer.cpp
    src/io_submitter.cpp
    src/checkpoint.cpp
    src/recovery.cpp
    src/compaction.cpp
    src/spdk_env.cpp
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

# SPDK integration (optional)
if(WITH_SPDK)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        # Get all SPDK flags from pkg-config
        pkg_check_modules(SPDK QUIET spdk_bdev spdk_blob spdk_blob_bdev spdk_nvme spdk_env_dpdk)
    endif()

    if(SPDK_FOUND)
        message(STATUS "SPDK found, enabling SPDK support")
        message(STATUS "  SPDK CFLAGS: ${SPDK_CFLAGS}")
        message(STATUS "  SPDK LDFLAGS: ${SPDK_LDFLAGS}")

        # Additional system libraries required by SPDK
        set(SPDK_SYSTEM_LIBS
            ssl crypto
            uuid
            isal isal_crypto
            numa pthread dl rt
            aio
        )

        # Use pkg-config flags directly
        target_compile_definitions(spdk_kv PUBLIC WITH_SPDK)
        target_compile_options(spdk_kv PUBLIC ${SPDK_CFLAGS})
        target_link_libraries(spdk_kv PUBLIC ${SPDK_LDFLAGS} ${SPDK_SYSTEM_LIBS})

        target_compile_definitions(spdk_kv_shared PUBLIC WITH_SPDK)
        target_compile_options(spdk_kv_shared PUBLIC ${SPDK_CFLAGS})
        target_link_libraries(spdk_kv_shared PUBLIC ${SPDK_LDFLAGS} ${SPDK_SYSTEM_LIBS})

        set(SPDK_ENABLED TRUE)
    else()
        message(STATUS "SPDK not found, building in simulation mode")
        set(SPDK_ENABLED FALSE)
        set(WITH_SPDK OFF)
    endif()
else()
    set(SPDK_ENABLED FALSE)
endif()

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
message(STATUS "  SPDK:        ${SPDK_ENABLED}")
message(STATUS "")
