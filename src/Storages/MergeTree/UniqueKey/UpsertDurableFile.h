#pragma once

#include <base/types.h>
#include <string_view>

namespace DB::Upsert
{

/// Explicit barriers propagate errors to the commit coordinator; destructors are not durability barriers.
void syncDirectory(const String & path);
void createDurableFile(const String & path, std::string_view contents);
String readBoundedFile(const String & path, size_t max_bytes);

}
