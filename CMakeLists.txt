cmake_minimum_required(VERSION 3.20)
project(shopping_sessions LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)

add_executable(shopping_sessions src/main.cpp)

target_compile_options(shopping_sessions PRIVATE
    -O3
    -DNDEBUG
    -pthread
)

target_link_libraries(shopping_sessions PRIVATE
    Arrow::arrow_shared
    Parquet::parquet_shared
    nlohmann_json::nlohmann_json
    pthread
)
