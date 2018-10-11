cmake_minimum_required(VERSION 3.12)
project(ext_sort LANGUAGES CXX)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CXX_STANDARD_REQUIRED ON)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake/Modules)

find_package(Boost REQUIRED
    system
)
find_package(Seastar REQUIRED)

set(SOURCES
    src/main.cpp
    src/helpers.h
    src/helpers.cpp
)
add_executable(ext_sort ${SOURCES})

set_property(TARGET ext_sort PROPERTY CXX_STANDARD 17)
target_link_libraries(ext_sort
    Seastar::Seastar
    Boost::system
)



target_compile_options(ext_sort
    PRIVATE
        -Wall
        -Werror
        -Wno-unused-variable
        -DDEBUG
        -g
    )
