set(MY_PROJ_NAME "pegasus_replica_server")

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.

set(TEMP_SRC "")
file("GLOB" TEMP_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/../*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/../*.c"
    )
set(MY_PROJ_SRC ${TEMP_SRC})
list(REMOVE_ITEM MY_PROJ_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../main.cpp)
list(REMOVE_ITEM MY_PROJ_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../available_detector.cpp)
list(REMOVE_ITEM MY_PROJ_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../info_collector.cpp)
list(REMOVE_ITEM MY_PROJ_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../info_collector_app.cpp)
list(REMOVE_ITEM MY_PROJ_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../result_writer.cpp)
list(REMOVE_ITEM MY_PROJ_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../hotspot_partition_calculator.cpp)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../")

set(MY_PROJ_LIBS
    dsn_replica_server
    dsn_replication_common
    dsn.block_service.local
    dsn.block_service.fds
    dsn.block_service
    dsn.failure_detector
    dsn_utils
    RocksDB::rocksdb
    pegasus_reporter
    pegasus_base
    pegasus_client_static
    event
    galaxy-fds-sdk-cpp
    PocoNet
    PocoFoundation
    PocoNetSSL
    PocoJSON
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

add_definitions(-Wno-attributes)

SET(CMAKE_INSTALL_RPATH ".")
SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

dsn_add_executable()
dsn_install_executable()
