#pragma once

#include <base/types.h>
#include <string_view>
#include <vector>

namespace DB::Upsert
{

/// Explicit barriers propagate errors to the commit coordinator; destructors are not durability barriers.
void syncDirectory(const String & path);
/// Join every file barrier and propagate errors before any directory or commit barrier.
void syncFiles(const std::vector<String> & paths);
void createDurableFile(const String & path, std::string_view contents);
String readBoundedFile(const String & path, size_t max_bytes);

}
