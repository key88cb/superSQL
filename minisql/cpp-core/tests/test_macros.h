#ifndef TEST_MACROS_H
#define TEST_MACROS_H

#include <iostream>
#include <string>
#include <vector>

extern int total_tests;
extern int passed_tests;

#define ASSERT_TRUE(condition) \
    do { \
        total_tests++; \
        if (condition) { \
            passed_tests++; \
        } else { \
            std::cerr << "FAIL: " << #condition << " @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        } \
    } while (0)

#define ASSERT_FALSE(condition) ASSERT_TRUE(!(condition))

#define ASSERT_GE(val1, val2) \
    do { \
        total_tests++; \
        if ((val1) >= (val2)) { \
            passed_tests++; \
        } else { \
            std::cerr << "FAIL: " << #val1 << " >= " << #val2 << " (actual: " << (val1) << ", expected >=: " << (val2) << ") @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        } \
    } while (0)

#define ASSERT_LE(val1, val2) \
    do { \
        total_tests++; \
        if ((val1) <= (val2)) { \
            passed_tests++; \
        } else { \
            std::cerr << "FAIL: " << #val1 << " <= " << #val2 << " (actual: " << (val1) << ", expected <=: " << (val2) << ") @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        } \
    } while (0)

#define ASSERT_EQ(val1, val2) \
    do { \
        total_tests++; \
        if ((val1) == (val2)) { \
            passed_tests++; \
        } else { \
            std::cerr << "FAIL: " << #val1 << " == " << #val2 << " (actual: " << (val1) << ", expected: " << (val2) << ") @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        } \
    } while (0)

#define TEST_REPORT() \
    do { \
        std::cout << "TEST RESULTS: " << passed_tests << "/" << total_tests << " PASSED" << std::endl; \
        if (passed_tests < total_tests) return 1; \
        return 0; \
    } while (0)

#endif
