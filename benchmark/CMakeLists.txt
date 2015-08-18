###################
# TellDB benchmark
###################
set(BENCHMARK_SRCS
    BdTreeBackend.cpp
    RemoteCounter.cpp
)

set(BENCHMARK_PRIVATE_HDR
    BdTreeBackend.hpp
    RemoteCounter.hpp
    TableData.hpp
)

# Add TellDB benchmark executable
add_executable(telldb-benchmark main.cpp ${BENCHMARK_SRCS} ${BENCHMARK_PRIVATE_HDR})

# Link against Boost
target_include_directories(telldb-benchmark PRIVATE ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(telldb-benchmark PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(telldb-benchmark PRIVATE crossbow_allocator crossbow_infinio crossbow_logger)

# Link against Bd-Tree
target_link_libraries(telldb-benchmark PRIVATE bdtree)

# Link against TellStore
target_link_libraries(telldb-benchmark PRIVATE tellstore-client)

# Link against Jemalloc
target_include_directories(telldb-benchmark PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(telldb-benchmark PRIVATE ${Jemalloc_LIBRARIES})
