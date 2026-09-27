cmake_minimum_required(VERSION 3.20)
project(market_data_engine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
list(APPEND CMAKE_PREFIX_PATH /opt/homebrew /usr/local)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED)
find_package(nlohmann_json 3.9 REQUIRED)

# ---------------------------------------------------------------------------
# Optional analytical-storage dependencies.
#
# Each is a feature flag rather than a hard requirement: without them the
# engine still collects and replays data, it just does not produce the
# columnar dataset. That keeps the core buildable on a machine that has no
# Arrow, and keeps CI honest about what is actually exercised.
# ---------------------------------------------------------------------------
option(MDE_WITH_PARQUET "Write normalized datasets as Parquet (needs Arrow)" ON)
option(MDE_WITH_DUCKDB  "Build the DuckDB catalog and query tool" ON)
option(MDE_WITH_ZSTD    "Compress the raw event archive with zstd" ON)

if(MDE_WITH_PARQUET)
  find_package(Arrow CONFIG)
  find_package(Parquet CONFIG)
  if(NOT Arrow_FOUND OR NOT Parquet_FOUND)
    message(WARNING "Arrow/Parquet not found -- building without Parquet storage")
    set(MDE_WITH_PARQUET OFF)
  endif()
endif()

if(MDE_WITH_DUCKDB)
  find_package(DuckDB CONFIG QUIET)
  if(NOT DuckDB_FOUND)
    find_library(MDE_DUCKDB_LIB NAMES duckdb HINTS /opt/homebrew/lib /usr/local/lib)
    find_path(MDE_DUCKDB_INCLUDE NAMES duckdb.hpp HINTS /opt/homebrew/include /usr/local/include)
    if(NOT MDE_DUCKDB_LIB OR NOT MDE_DUCKDB_INCLUDE)
      message(WARNING "DuckDB not found -- building without the DuckDB catalog")
      set(MDE_WITH_DUCKDB OFF)
    endif()
  endif()
endif()

if(MDE_WITH_ZSTD)
  find_package(zstd CONFIG QUIET)
  if(NOT zstd_FOUND)
    find_library(MDE_ZSTD_LIB NAMES zstd HINTS /opt/homebrew/lib /usr/local/lib)
    find_path(MDE_ZSTD_INCLUDE NAMES zstd.h HINTS /opt/homebrew/include /usr/local/include)
    if(NOT MDE_ZSTD_LIB OR NOT MDE_ZSTD_INCLUDE)
      message(WARNING "libzstd not found -- raw archive will be written uncompressed")
      set(MDE_WITH_ZSTD OFF)
    endif()
  endif()
endif()

message(STATUS "market-data-engine features: "
               "parquet=${MDE_WITH_PARQUET} duckdb=${MDE_WITH_DUCKDB} zstd=${MDE_WITH_ZSTD}")

option(MDE_BUILD_TESTS "Build the unit test suite" ON)
option(MDE_WERROR "Treat warnings as errors" OFF)

# Strict warnings everywhere. The project is kept warning-clean.
add_library(mde_flags INTERFACE)
target_compile_options(mde_flags INTERFACE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Wshadow;-Wconversion;-Wsign-conversion>
  $<$<AND:$<BOOL:${MDE_WERROR}>,$<CXX_COMPILER_ID:GNU,Clang,AppleClang>>:-Werror>
)

# ---------------------------------------------------------------------------
# Core library: everything venue-agnostic and unit-testable.
# ---------------------------------------------------------------------------
add_library(mde_core
  src/order_book.cpp
  src/market_registry.cpp
  src/http_client.cpp
  src/exchange/polymarket_parser.cpp
  src/exchange/polymarket_adapter.cpp
  src/storage/raw_archive.cpp
  src/storage/parquet_writer.cpp
  src/storage/duckdb_catalog.cpp
  src/storage/storage_manager.cpp
  src/order_book_manager.cpp
  src/engine.cpp
  src/replay.cpp
  src/event_codec.cpp
  src/recorder.cpp
  src/metrics.cpp
  src/config.cpp
  src/logging.cpp
)
target_include_directories(mde_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mde_core
  PUBLIC  nlohmann_json::nlohmann_json Threads::Threads
  PRIVATE mde_flags Boost::headers OpenSSL::SSL OpenSSL::Crypto
)
# Beast/Asio headers are third-party; -Wconversion on our own code should not
# be defeated by having to silence theirs.
target_include_directories(mde_core SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})

if(MDE_WITH_PARQUET)
  target_compile_definitions(mde_core PUBLIC MDE_WITH_PARQUET)
  target_link_libraries(mde_core PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
endif()

if(MDE_WITH_ZSTD)
  target_compile_definitions(mde_core PUBLIC MDE_WITH_ZSTD)
  if(zstd_FOUND)
    target_link_libraries(mde_core PRIVATE zstd::libzstd_shared)
  else()
    target_include_directories(mde_core SYSTEM PRIVATE ${MDE_ZSTD_INCLUDE})
    target_link_libraries(mde_core PRIVATE ${MDE_ZSTD_LIB})
  endif()
endif()

if(MDE_WITH_DUCKDB)
  target_compile_definitions(mde_core PUBLIC MDE_WITH_DUCKDB)
  if(DuckDB_FOUND)
    target_link_libraries(mde_core PRIVATE duckdb)
  else()
    target_include_directories(mde_core SYSTEM PRIVATE ${MDE_DUCKDB_INCLUDE})
    target_link_libraries(mde_core PRIVATE ${MDE_DUCKDB_LIB})
  endif()
endif()

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
add_executable(market_data_engine src/main.cpp)
target_link_libraries(market_data_engine PRIVATE mde_core mde_flags)

add_executable(market_data_replay tools/replay.cpp)
target_link_libraries(market_data_replay PRIVATE mde_core mde_flags)

if(MDE_WITH_DUCKDB)
  add_executable(market_data_query tools/query_db.cpp)
  target_link_libraries(market_data_query PRIVATE mde_core mde_flags)
endif()

add_executable(market_data_discover tools/discover.cpp)
target_link_libraries(market_data_discover PRIVATE mde_core mde_flags)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(MDE_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.17.0
    GIT_SHALLOW TRUE
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  # GoogleTest is third-party; keep its own diagnostics out of our build log so
  # a warning-clean build stays a meaningful signal.
  foreach(gtest_target gtest gtest_main gmock gmock_main)
    if(TARGET ${gtest_target})
      target_compile_options(${gtest_target} PRIVATE
        $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-w>)
      get_target_property(gtest_inc ${gtest_target} INTERFACE_INCLUDE_DIRECTORIES)
      if(gtest_inc)
        target_include_directories(${gtest_target} SYSTEM INTERFACE ${gtest_inc})
      endif()
    endif()
  endforeach()

  enable_testing()
  include(GoogleTest)

  add_executable(mde_tests
    tests/test_order_book.cpp
    tests/test_market_registry.cpp
    tests/test_polymarket_parser.cpp
    tests/test_market_events.cpp
    tests/test_order_book_manager.cpp
    tests/test_event_bus.cpp
    tests/test_config.cpp
    tests/test_recorder.cpp
    tests/test_parquet_writer.cpp
    tests/test_storage_integration.cpp
    tests/test_storage_manager.cpp
    tests/test_replay.cpp
  )
  target_compile_definitions(mde_tests PRIVATE
    MDE_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
  target_link_libraries(mde_tests PRIVATE mde_core mde_flags GTest::gtest_main)
  gtest_discover_tests(mde_tests)
endif()
