include_directories(${Crossbow_INCLUDE_DIRS})
add_executable(basic_test basic_test.cpp)
target_link_libraries(basic_test telldb)
