// Copyright 2008 Google Inc. All Rights Reserved.
//
// Migrated to base/macros.h. This file is a backward-compatible redirect.
// New code should include "base/macros.h" directly.

#pragma once

#include "base/macros.h"
#include "butil/macros.h"
#include "gutil/port.h"

#ifndef SWIG
#define ABSTRACT = 0
#endif

// base::LinkerInitialized - kept here for backward compatibility with gutil code.
namespace base {
enum LinkerInitialized { LINKER_INITIALIZED };
}
