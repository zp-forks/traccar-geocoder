cmake_minimum_required(VERSION 3.16)
project(traccar-geocoder-build LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(EXPAT REQUIRED)
find_package(s2 REQUIRED)

add_executable(build-index src/build_index.cpp)

target_include_directories(build-index PRIVATE
    /opt/homebrew/include
)

target_link_libraries(build-index PRIVATE
    s2::s2
    ZLIB::ZLIB
    BZip2::BZip2
    EXPAT::EXPAT
    Threads::Threads
)
