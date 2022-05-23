set(
    SHARED_SOURCES
    base_test.cpp
    base_test.hpp
    lib/sql/sqlite_testrunner/sqlite_testrunner.cpp
    lib/sql/sqlite_testrunner/sqlite_testrunner.hpp
    testing_assert.cpp
    testing_assert.hpp
)

set(
    HYRISE_UNIT_TEST_SOURCES
    ${SHARED_SOURCES}
    gtest_case_template.cpp
    gtest_main.cpp
    lib/all_parameter_variant_test.cpp
    lib/all_type_variant_test.cpp
    
    lib/storage/gd_segment_test.cpp
    
    lib/storage/any_segment_iterable_test.cpp
    lib/storage/chunk_encoder_test.cpp
    lib/storage/chunk_test.cpp
    lib/storage/encoded_segment_test.cpp
    lib/storage/encoding_test.hpp
    lib/storage/index/multi_segment_index_test.cpp
    lib/storage/index/single_segment_index_test.cpp
    lib/storage/iterables_test.cpp
    lib/storage/materialize_test.cpp
    lib/storage/pos_lists/entire_chunk_pos_list_test.cpp
    lib/storage/prepared_plan_test.cpp
    lib/storage/reference_segment_test.cpp
    lib/storage/segment_access_counter_test.cpp
    lib/storage/segment_accessor_test.cpp
    lib/storage/segment_iterators_test.cpp
    lib/storage/storage_manager_test.cpp
    lib/storage/table_column_definition_test.cpp
    lib/storage/table_key_constraint_test.cpp
    lib/storage/table_test.cpp
    lib/storage/value_segment_test.cpp
    testing_assert.cpp
    testing_assert.hpp
    utils/constraint_test_utils.hpp
)

set (
    SYSTEM_TEST_SOURCES
    ${SHARED_SOURCES}
    benchmarklib/synthetic_table_generator_test.cpp
    benchmarklib/tpcc/tpcc_test.cpp
    benchmarklib/tpcds/tpcds_db_generator_test.cpp
    benchmarklib/tpch/tpch_db_generator_test.cpp
    gtest_main.cpp
    lib/concurrency/stress_test.cpp
    lib/server/server_test_runner.cpp
    lib/sql/sqlite_testrunner/sqlite_testrunner_encodings.cpp
    lib/utils/plugin_test_utils.cpp
    lib/utils/plugin_test_utils.hpp
    plugins/mvcc_delete_plugin_system_test.cpp
)

# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    gmock
    SQLite::SQLite3
    hyriseMvccDeletePlugin  # So that we can test member methods without going through dlsym
)

# This warning does not play well with SCOPED_TRACE
add_compile_options(-Wno-used-but-marked-unused)

# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")

# Build special sanitizer version of googletest
include_directories(../../third_party/googletest/googletest/)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure hyriseTest
add_executable(hyriseTest ${HYRISE_UNIT_TEST_SOURCES})
add_dependencies(hyriseTest hyriseSecondTestPlugin hyriseTestPlugin hyriseMvccDeletePlugin hyriseTestNonInstantiablePlugin)
target_link_libraries(hyriseTest hyrise ${LIBRARIES})

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
target_compile_options(hyriseTest PUBLIC -Xclang -fno-pch-timestamp)
endif()
if(NOT ${CMAKE_VERSION} VERSION_LESS "3.16.0")
    target_precompile_headers(hyriseTest PRIVATE
        <gtest/gtest.h>
        [["all_parameter_variant.hpp"]]
        [["storage/create_iterable_from_segment.hpp"]]
        [["storage/table.hpp"]]
        [["types.hpp"]]
    )
endif()

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest PUBLIC hyrise hyriseBenchmarkLib ${LIBRARIES})
target_link_libraries_system(hyriseSystemTest pqxx_static)
target_compile_options(hyriseSystemTest PRIVATE -DPQXX_HIDE_EXP_OPTIONAL)
