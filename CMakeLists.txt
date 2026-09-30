cmake_minimum_required(VERSION 3.10)
project(raft_election CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

add_library(raft_core STATIC
    src/raft_rpc.cpp
    src/raft_node.cpp
)
target_include_directories(raft_core PUBLIC include)
target_link_libraries(raft_core PUBLIC Threads::Threads)

add_executable(raft_node src/main.cpp)
target_link_libraries(raft_node PRIVATE raft_core)

add_executable(raft_client src/raft_client.cpp)
target_link_libraries(raft_client PRIVATE raft_core)

# --- Tests -------------------------------------------------------------
enable_testing()

function(add_raft_test name)
    add_executable(${name} tests/${name}.cpp)
    target_include_directories(${name} PRIVATE include tests)
    target_link_libraries(${name} PRIVATE raft_core)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_raft_test(test_rpc)
add_raft_test(test_raft_node)
add_raft_test(test_election)
