# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.24)
cmake_policy(SET CMP0135 NEW)

project(iceberg-c)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# force the compiler to support these standards
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CARGO_DIST_DIR "${PROJECT_SOURCE_DIR}/target/debug")
if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_BUILD_TYPE "--release")
    set(CARGO_DIST_DIR "${PROJECT_SOURCE_DIR}/target/release")
endif()

set(ICEBERG_STATIC_LIB "${CARGO_DIST_DIR}/libiceberg_c${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(ICEBERG_SHARED_LIB "${CARGO_DIST_DIR}/libiceberg_c${CMAKE_SHARED_LIBRARY_SUFFIX}")
message(NOTICE "-- Iceberg C static lib: ${ICEBERG_STATIC_LIB}")
message(NOTICE "-- Iceberg C shared lib: ${ICEBERG_SHARED_LIB}")

# custom target for cargo build
add_custom_target(cargo_build
    COMMAND cargo build ${CARGO_BUILD_TYPE}
    BYPRODUCTS  ${ICEBERG_STATIC_LIB} ${ICEBERG_SHARED_LIB}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

# cmake target for static lib
add_library(iceberg_c_static INTERFACE)
target_link_libraries(iceberg_c_static INTERFACE ${ICEBERG_STATIC_LIB})
target_include_directories(iceberg_c_static INTERFACE include)
add_dependencies(iceberg_c_static cargo_build)

# cmake target for shared lib
add_library(iceberg_c_shared INTERFACE)
target_link_libraries(iceberg_c_shared INTERFACE ${ICEBERG_SHARED_LIB})
target_include_directories(iceberg_c_shared INTERFACE include)
add_dependencies(iceberg_c_shared cargo_build)
