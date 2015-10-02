cmake_minimum_required(VERSION 2.8.12)
project(TellDB)

# Set default install paths
set(CMAKE_INSTALL_DIR cmake CACHE PATH "Installation directory for CMake files")
set(INCLUDE_INSTALL_DIR include CACHE PATH "Installation directory for header files")
set(LIB_INSTALL_DIR lib CACHE PATH "Installation directory for libraries")

# Set the TellDB directory
set(TellDB_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE PATH "Path to the TellDB binaries and configuration")

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Set compile options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")

# Find dependencies
find_package(Boost REQUIRED)
find_package(Crossbow COMPONENTS Allocator InfinIO Logger REQUIRED)
find_package(BdTree REQUIRED)
find_package(TellStore REQUIRED)
find_package(Jemalloc REQUIRED)

# Documentation
find_package(Doxygen)
if (DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
    add_custom_target(doc
        ${DOXYGEN_EXECUTABLE}
        ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation")
endif()

set(TELLDB_SRCS
    src/TellDB.cpp
    src/Transaction.cpp
    src/TransactionCache.cpp
    src/Field.cpp
    src/TableCache.cpp
    src/Tuple.cpp
)

set(TELLDB_COMMON_HDR
    telldb/TellDB.hpp
    telldb/Transaction.hpp
    telldb/Field.hpp
    telldb/Tuple.hpp
    telldb/Types.hpp
)
add_library(telldb SHARED ${TELLDB_SRCS} ${TELLDB_COMMON_HDR})
target_include_directories(telldb PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(telldb PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(telldb PRIVATE ${Crossbow_LIBRARIES})
target_link_libraries(telldb PUBLIC tellstore-client)

add_subdirectory(benchmark)
