cmake_minimum_required(VERSION 3.16.3)
project(SpannerBenchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find Google Cloud Spanner C++ SDK
find_package(google_cloud_cpp_spanner REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)

add_executable(benchmark_ycsb_spanner benchmark_ycsb_spanner.cpp)

target_link_libraries(benchmark_ycsb_spanner
    PRIVATE
    google-cloud-cpp::spanner
    gflags
    glog::glog
    pthread
)
