// Copyright 2008 Google Inc.  All rights reserved.
//
// Thin compatibility wrapper around absl::Substitute.
// All new code should use absl::Substitute / absl::SubstituteAndAppend directly.

#pragma once

#include <string>

#include "absl/strings/substitute.h"

namespace strings {

using ::absl::Substitute;
using ::absl::SubstituteAndAppend;

} // namespace strings
