/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


#ifndef _ICEBERG_H
#define _ICEBERG_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/**
 * Proxy id for "last added" items, including schema, partition spec, sort order.
 */
#define TableMetadataBuilder_LAST_ADDED -1

typedef enum PrimitiveLiteralTag {
  Boolean,
  Int,
  Long,
  Float,
  Double,
  String,
  Binary,
  Int128,
  UInt128,
  AboveMax,
  BelowMin,
} PrimitiveLiteralTag;

typedef struct BinarySlice {
  const uint8_t *ptr;
  uintptr_t len;
} BinarySlice;

typedef union PrimitiveLiteralUnion {
  bool boolean;
  int32_t int;
  int64_t long;
  float float;
  double double;
  i128 int128;
  u128 uint128;
  const char *string;
  struct BinarySlice binary;
} PrimitiveLiteralUnion;

typedef struct PrimitiveLiteral {
  enum PrimitiveLiteralTag tag;
  union PrimitiveLiteralUnion data;
} PrimitiveLiteral;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

void iceberg_spec_primitive_literal_free(struct PrimitiveLiteral *literal);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* _ICEBERG_H */
