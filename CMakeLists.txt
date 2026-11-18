cmake_minimum_required(VERSION 3.16)

project(MiniKVInfra
	VERSION 0.1.0
	LANGUAGES CXX 

)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(minikv_server
	apps/minikv_server.cpp
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(minikv_server PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

enable_testing()

