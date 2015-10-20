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
find_package(Crossbow COMPONENTS Allocator REQUIRED)
find_package(BdTree REQUIRED)
find_package(TellStore REQUIRED)
find_package(Jemalloc REQUIRED)


set(TELLDB_SRCS
    src/TellDB.cpp
    src/Transaction.cpp
    src/TransactionCache.cpp
    src/Field.cpp
    src/TableCache.cpp
    src/Tuple.cpp
    src/Exceptions.cpp
    src/BdTreeBackend.cpp
    src/BdTreeBackend.hpp
    src/Indexes.cpp
    src/Indexes.hpp
    src/RemoteCounter.cpp
    src/RemoteCounter.hpp
    src/TableData.hpp
)

set(TELLDB_COMMON_HDR
    telldb/TellDB.hpp
    telldb/Transaction.hpp
    telldb/Field.hpp
    telldb/Tuple.hpp
    telldb/Types.hpp
    telldb/Exceptions.hpp
    telldb/Iterator.hpp
)
add_library(telldb SHARED ${TELLDB_SRCS} ${TELLDB_COMMON_HDR})
target_include_directories(telldb PRIVATE ${Crossbow_INCLUDE_DIRS})
target_include_directories(telldb PUBLIC $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${INCLUDE_INSTALL_DIR}>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)
target_link_libraries(telldb PRIVATE crossbow_allocator)
target_link_libraries(telldb PUBLIC tellstore-client)
target_link_libraries(telldb PRIVATE bdtree)

# Documentation
if(BUILD_GLOBAL_DOCS)
    set(TELL_PUBLIC_HDRS "${TELL_PUBLIC_HDRS} ${CMAKE_CURRENT_SOURCE_DIR}/telldb" PARENT_SCOPE)
else()
    find_package(Doxygen)
    if(DOXYGEN_FOUND)
        configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
        add_custom_target(doc
            ${DOXYGEN_EXECUTABLE}
            ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "Generating API documentation")
    endif()
endif()

add_subdirectory(benchmark)

# Install the library
install(TARGETS telldb EXPORT TellDBTargets
        ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
        LIBRARY DESTINATION ${LIB_INSTALL_DIR}
        PUBLIC_HEADER DESTINATION ${INCLUDE_INSTALL_DIR}/telldb)

# Install the cmake target
install(EXPORT TellDBTargets DESTINATION ${CMAKE_INSTALL_DIR})

# Create cmake config file
configure_file(TellDBConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/TellDBConfig.cmake @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/TellDBConfig.cmake DESTINATION ${CMAKE_INSTALL_DIR})

