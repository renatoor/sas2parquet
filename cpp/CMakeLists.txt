cmake_minimum_required(VERSION 3.16)
include(ExternalProject)
set(CMAKE_VERBOSE_MAKEFILE, TRUE)
# set(CMAKE_CXX_FLAGS "-O3 -Wall -Wextra")

project(sas2parquet)

include_directories("${CMAKE_SOURCE_DIR}/include")

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_subdirectory(lib)

add_executable(main main.cpp)
target_link_libraries(main PRIVATE Arrow::arrow_shared Parquet::parquet_shared readstat)
