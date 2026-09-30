// Tiny header-only test helper: no external test framework dependency.
// A test binary calls CHECK(...) for each assertion; if any fails the
// process exits with a non-zero status (picked up by CTest).
#pragma once

#include <cstdlib>
#include <iostream>
#include <string>

inline int g_failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::cerr << "CHECK FAILED: " << #cond << " at " << __FILE__   \
                       << ":" << __LINE__ << std::endl;                    \
            g_failures++;                                                  \
        } else {                                                           \
            std::cerr << "  ok: " << #cond << std::endl;                   \
        }                                                                  \
    } while (0)

#define TEST_MAIN_RETURN() return g_failures == 0 ? 0 : 1
