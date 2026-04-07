// Copyright 2002 and onwards Google Inc.
// Self-contained version moved from base/gutil/bits.h.
#pragma once
#include <cstdint>
#include "base/gutil/integral_types.h"

class Bits {
public:
    static int CountOnesInByte(unsigned char n);
    static int CountOnes(uint32 n) {
        n -= ((n >> 1) & 0x55555555);
        n = ((n >> 2) & 0x33333333) + (n & 0x33333333);
        return (((n + (n >> 4)) & 0xF0F0F0F) * 0x1010101) >> 24;
    }
    static int CountTrailingZeros32(uint32_t n);
    static int CountTrailingZerosNonZero32(uint32_t n);
    static int CountTrailingZeros64(uint64_t n);
    static int CountTrailingZerosNonZero64(uint64_t n);
    static inline int CountOnes64(uint64 n) {
#if defined(__x86_64__)
        n -= (n >> 1) & 0x5555555555555555ULL;
        n = ((n >> 2) & 0x3333333333333333ULL) + (n & 0x3333333333333333ULL);
        return (((n + (n >> 4)) & 0xF0F0F0F0F0F0F0FULL) * 0x101010101010101ULL) >> 56;
#else
        return CountOnes(n >> 32) + CountOnes(n & 0xffffffff);
#endif
    }
    static inline int CountOnes64withPopcount(uint64 n) {
#if defined(__x86_64__) && defined __GNUC__
        int64 count = 0;
        asm("popcnt %1,%0" : "=r"(count) : "rm"(n) : "cc");
        return count;
#else
        return CountOnes64(n);
#endif
    }
    static uint8 ReverseBits8(uint8 n);
    static uint32 ReverseBits32(uint32 n);
    static uint64 ReverseBits64(uint64 n);
    static int Count(const void* m, int num_bytes);
    static int Difference(const void* m1, const void* m2, int num_bytes);
    static int CappedDifference(const void* m1, const void* m2, int num_bytes, int cap);
    static int Log2Floor(uint32 n);
    static int Log2Floor64(uint64 n);
    static int Log2FloorNonZero(uint32 n);
    static int Log2FloorNonZero64(uint64 n);
    static int Log2Ceiling(uint32 n);
    static int Log2Ceiling64(uint64 n);
    static int FindLSBSetNonZero(uint32 n);
    static int FindLSBSetNonZero64(uint64 n);
    static int FindMSBSetNonZero(uint32 n) { return Log2FloorNonZero(n); }
    static int FindMSBSetNonZero64(uint64 n) { return Log2FloorNonZero64(n); }
    static int Log2Floor_Portable(uint32 n);
    static int Log2FloorNonZero_Portable(uint32 n);
    static int FindLSBSetNonZero_Portable(uint32 n);
    static int Log2Floor64_Portable(uint64 n);
    static int Log2FloorNonZero64_Portable(uint64 n);
    static int FindLSBSetNonZero64_Portable(uint64 n);
    template <class T> static bool BytesContainByte(T bytes, uint8 c);
    template <class T> static bool BytesContainByteLessThan(T bytes, uint8 c);
    template <class T> static bool BytesAllInRange(T bytes, uint8 lo, uint8 hi);
private:
    static const char num_bits[];
    static const unsigned char bit_reverse_table[];
    Bits(const Bits&) = delete;
    const Bits& operator=(const Bits&) = delete;
};

template <class T>
struct BitPattern {
    static const T half_ones = (static_cast<T>(1) << (sizeof(T) * 4)) - 1;
    static const T l = (sizeof(T) == 1) ? 1 : (half_ones / 0xff * (half_ones + 2));
    static const T h = ~(l * 0x7f);
};

#if defined(__GNUC__) && ((__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || __GNUC__ >= 4)
inline int Bits::Log2Floor(uint32 n) { return n == 0 ? -1 : 31 ^ __builtin_clz(n); }
inline int Bits::Log2FloorNonZero(uint32 n) { return 31 ^ __builtin_clz(n); }
inline int Bits::FindLSBSetNonZero(uint32 n) { return __builtin_ctz(n); }
inline int Bits::Log2Floor64(uint64 n) { return n == 0 ? -1 : 63 ^ __builtin_clzll(n); }
inline int Bits::Log2FloorNonZero64(uint64 n) { return 63 ^ __builtin_clzll(n); }
inline int Bits::FindLSBSetNonZero64(uint64 n) { return __builtin_ctzll(n); }
#elif defined(_MSC_VER)
#include "base/gutil/bits-internal-windows.h"
#else
#include "base/gutil/bits-internal-unknown.h"
#endif

inline int Bits::CountOnesInByte(unsigned char n) { return num_bits[n]; }
inline uint8 Bits::ReverseBits8(unsigned char n) {
    n = ((n >> 1) & 0x55) | ((n & 0x55) << 1);
    n = ((n >> 2) & 0x33) | ((n & 0x33) << 2);
    return ((n >> 4) & 0x0f) | ((n & 0x0f) << 4);
}
inline uint32 Bits::ReverseBits32(uint32 n) {
    n = ((n >> 1) & 0x55555555) | ((n & 0x55555555) << 1);
    n = ((n >> 2) & 0x33333333) | ((n & 0x33333333) << 2);
    n = ((n >> 4) & 0x0F0F0F0F) | ((n & 0x0F0F0F0F) << 4);
    n = ((n >> 8) & 0x00FF00FF) | ((n & 0x00FF00FF) << 8);
    return (n >> 16) | (n << 16);
}
inline uint64 Bits::ReverseBits64(uint64 n) {
#if defined(__x86_64__)
    n = ((n >> 1) & 0x5555555555555555ULL) | ((n & 0x5555555555555555ULL) << 1);
    n = ((n >> 2) & 0x3333333333333333ULL) | ((n & 0x3333333333333333ULL) << 2);
    n = ((n >> 4) & 0x0F0F0F0F0F0F0F0FULL) | ((n & 0x0F0F0F0F0F0F0F0FULL) << 4);
    n = ((n >> 8) & 0x00FF00FF00FF00FFULL) | ((n & 0x00FF00FF00FF00FFULL) << 8);
    n = ((n >> 16) & 0x0000FFFF0000FFFFULL) | ((n & 0x0000FFFF0000FFFFULL) << 16);
    return (n >> 32) | (n << 32);
#else
    return ReverseBits32(n >> 32) | (static_cast<uint64>(ReverseBits32(n & 0xffffffff)) << 32);
#endif
}
inline int Bits::Log2FloorNonZero_Portable(uint32 n) { return Log2Floor(n); }
inline int Bits::Log2Floor64_Portable(uint64 n) {
    const uint32 topbits = static_cast<uint32>(n >> 32);
    if (topbits == 0) return Log2Floor(static_cast<uint32>(n));
    return 32 + Log2FloorNonZero(topbits);
}
inline int Bits::Log2FloorNonZero64_Portable(uint64 n) {
    const uint32 topbits = static_cast<uint32>(n >> 32);
    if (topbits == 0) return Log2FloorNonZero(static_cast<uint32>(n));
    return 32 + Log2FloorNonZero(topbits);
}
inline int Bits::FindLSBSetNonZero64_Portable(uint64 n) {
    const uint32 bottombits = static_cast<uint32>(n);
    if (bottombits == 0) return 32 + FindLSBSetNonZero(static_cast<uint32>(n >> 32));
    return FindLSBSetNonZero(bottombits);
}
template <class T>
inline bool Bits::BytesContainByteLessThan(T bytes, uint8 c) {
    T l = BitPattern<T>::l; T h = BitPattern<T>::h;
    return c <= 0x80 ? ((h & (bytes - l * c) & ~bytes) != 0) : ((((bytes - l * c) | (bytes ^ h)) & h) != 0);
}
template <class T>
inline bool Bits::BytesContainByte(T bytes, uint8 c) {
    return Bits::BytesContainByteLessThan<T>(bytes ^ (c * BitPattern<T>::l), 1);
}
template <class T>
inline bool Bits::BytesAllInRange(T bytes, uint8 lo, uint8 hi) {
    T l = BitPattern<T>::l; T h = BitPattern<T>::h;
    if (lo > hi) return false;
    if (hi - lo < 128) { T x = bytes - l * lo; T y = bytes + l * (127 - hi); return ((x | y) & h) == 0; }
    return !Bits::BytesContainByteLessThan(bytes + (255 - hi) * l, lo + (255 - hi));
}
inline int Bits::CountTrailingZerosNonZero32(uint32_t n) { return Bits::FindLSBSetNonZero(n); }
inline int Bits::CountTrailingZeros32(uint32_t n) { return n == 0 ? 32 : Bits::CountTrailingZerosNonZero32(n); }
inline int Bits::CountTrailingZerosNonZero64(uint64_t n) { return Bits::FindLSBSetNonZero64(n); }
inline int Bits::CountTrailingZeros64(uint64_t n) { return n == 0 ? 64 : Bits::CountTrailingZerosNonZero64(n); }
