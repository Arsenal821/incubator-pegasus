set(MY_PROJ_NAME "pegasus_meta_server")

set(MY_PROJ_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/../args.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/../brief_stat.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/../server_utils.cpp"
    )

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

message(STATUS "For ${MY_PROJ_NAME} ZOOKEEPER_LIBRARIES=${ZOOKEEPER_LIBRARIES}")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../")

set(MY_PROJ_LIBS
    dsn_meta_server
    dsn.failure_detector
    dsn.replication.zookeeper_provider
    dsn_utils
    dsn.block_service
    pegasus_reporter
    pegasus_base
    zookeeper_mt
    event
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

add_definitions(-Wno-attributes)

SET(CMAKE_INSTALL_RPATH ".")
SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

dsn_add_executable()
dsn_install_executable()
