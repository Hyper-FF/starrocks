// Copyright 2005 Google Inc.
// Self-contained endian utilities moved from base/gutil/endian.h.
#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>

#include "base/compiler_util.h"
#include "base/gutil/int128.h"
#include "base/gutil/integral_types.h"

#if defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#ifndef bswap_16
#define bswap_16(x) OSSwapInt16(x)
#endif
#ifndef bswap_32
#define bswap_32(x) OSSwapInt32(x)
#endif
#ifndef bswap_64
#define bswap_64(x) OSSwapInt64(x)
#endif
#include <machine/endian.h>
#ifndef __BYTE_ORDER
#define __BYTE_ORDER BYTE_ORDER
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __BIG_ENDIAN BIG_ENDIAN
#endif
#elif defined(__GLIBC__) || defined(__linux__)
#include <byteswap.h>
#include <endian.h>
#else
#include <endian.h>
#endif

#if !defined(IS_LITTLE_ENDIAN) && !defined(IS_BIG_ENDIAN)
#if defined(__BYTE_ORDER)
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define IS_LITTLE_ENDIAN
#elif __BYTE_ORDER == __BIG_ENDIAN
#define IS_BIG_ENDIAN
#endif
#elif defined(__LITTLE_ENDIAN__)
#define IS_LITTLE_ENDIAN
#elif defined(__BIG_ENDIAN__)
#define IS_BIG_ENDIAN
#endif
#endif

inline uint16_t UnalignedLoad16(const void* p) { uint16_t t; memcpy(&t, p, sizeof t); return t; }
inline uint32_t UnalignedLoad32(const void* p) { uint32_t t; memcpy(&t, p, sizeof t); return t; }
inline uint64_t UnalignedLoad64(const void* p) { uint64_t t; memcpy(&t, p, sizeof t); return t; }
inline void UnalignedStore16(void* p, uint16_t v) { memcpy(p, &v, sizeof v); }
inline void UnalignedStore32(void* p, uint32_t v) { memcpy(p, &v, sizeof v); }
inline void UnalignedStore64(void* p, uint64_t v) { memcpy(p, &v, sizeof v); }

inline uint64_t gbswap_64(uint64_t host_int) {
#if defined(__GNUC__) && defined(__x86_64__) && !defined(__APPLE__)
    if (__builtin_constant_p(host_int)) { return __bswap_constant_64(host_int); }
    else { uint64_t result; __asm__("bswap %0" : "=r"(result) : "0"(host_int)); return result; }
#elif defined(bswap_64)
    return bswap_64(host_int);
#else
    return static_cast<uint64_t>(bswap_32(static_cast<uint32_t>(host_int >> 32))) |
           (static_cast<uint64_t>(bswap_32(static_cast<uint32_t>(host_int))) << 32);
#endif
}

inline unsigned __int128 gbswap_128(unsigned __int128 host_int) {
    return static_cast<unsigned __int128>(bswap_64(static_cast<uint64_t>(host_int >> 64))) |
           (static_cast<unsigned __int128>(bswap_64(static_cast<uint64_t>(host_int))) << 64);
}

inline uint32_t bswap_24(uint32_t x) {
    return ((x & 0x0000ffULL) << 16) | ((x & 0x00ff00ULL)) | ((x & 0xff0000ULL) >> 16);
}

#ifdef IS_LITTLE_ENDIAN
inline uint16 ghtons(uint16 x) { return bswap_16(x); }
inline uint32 ghtonl(uint32 x) { return bswap_32(x); }
inline uint64 ghtonll(uint64 x) { return gbswap_64(x); }
#elif defined(IS_BIG_ENDIAN)
inline uint16 ghtons(uint16 x) { return x; }
inline uint32 ghtonl(uint32 x) { return x; }
inline uint64 ghtonll(uint64 x) { return x; }
#else
#error "Unsupported bytesex"
#endif

inline uint16 gntohl(uint16 x) { return ghtonl(x); }
inline uint32 gntohs(uint32 x) { return ghtons(x); }
inline uint64 gntohll(uint64 x) { return ghtonll(x); }
#if !defined(__APPLE__)
inline uint64 htonll(uint64 x) { return ghtonll(x); }
inline uint64 ntohll(uint64 x) { return htonll(x); }
#endif

class LittleEndian {
public:
#ifdef IS_LITTLE_ENDIAN
    static uint16 FromHost16(uint16 x) { return x; }
    static uint16 ToHost16(uint16 x) { return x; }
    static uint32 FromHost32(uint32 x) { return x; }
    static uint32 ToHost32(uint32 x) { return x; }
    static uint64 FromHost64(uint64 x) { return x; }
    static uint64 ToHost64(uint64 x) { return x; }
    static unsigned __int128 FromHost128(unsigned __int128 x) { return x; }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return x; }
    static bool IsLittleEndian() { return true; }
#elif defined(IS_BIG_ENDIAN)
    static uint16 FromHost16(uint16 x) { return bswap_16(x); }
    static uint16 ToHost16(uint16 x) { return bswap_16(x); }
    static uint32 FromHost32(uint32 x) { return bswap_32(x); }
    static uint32 ToHost32(uint32 x) { return bswap_32(x); }
    static uint64 FromHost64(uint64 x) { return gbswap_64(x); }
    static uint64 ToHost64(uint64 x) { return gbswap_64(x); }
    static bool IsLittleEndian() { return false; }
#endif
    static uint16 Load16(const void* p) { return ToHost16(UnalignedLoad16(p)); }
    static void Store16(void* p, uint16 v) { UnalignedStore16(p, FromHost16(v)); }
    static uint32 Load32(const void* p) { return ToHost32(UnalignedLoad32(p)); }
    static void Store32(void* p, uint32 v) { UnalignedStore32(p, FromHost32(v)); }
    static uint64 Load64(const void* p) { return ToHost64(UnalignedLoad64(p)); }
    static uint64 Load64VariableLength(const void* const p, int len) {
        assert(len >= 1 && len <= 8);
        const char* const buf = static_cast<const char* const>(p);
        uint64 val = 0; --len;
        do { val = (val << 8) | buf[len]; } while (--len >= 0);
        return val;
    }
    static void Store64(void* p, uint64 v) { UnalignedStore64(p, FromHost64(v)); }
    static uint128 Load128(const void* p) {
        return uint128(ToHost64(UnalignedLoad64(reinterpret_cast<const uint64*>(p) + 1)), ToHost64(UnalignedLoad64(p)));
    }
    static void Store128(void* p, const uint128& v) {
        UnalignedStore64(p, FromHost64(Uint128Low64(v)));
        UnalignedStore64(reinterpret_cast<uint64*>(p) + 1, FromHost64(Uint128High64(v)));
    }
    static uint128 Load128VariableLength(const void* p, int len) {
        if (len <= 8) return uint128(Load64VariableLength(p, len));
        return uint128(Load64VariableLength(static_cast<const char*>(p) + 8, len - 8), Load64(p));
    }
    static uword_t LoadUnsignedWord(const void* p) { return sizeof(uword_t) == 8 ? Load64(p) : Load32(p); }
    static void StoreUnsignedWord(void* p, uword_t v) { if (sizeof(v) == 8) Store64(p, v); else Store32(p, v); }
};

class BigEndian {
public:
#ifdef IS_LITTLE_ENDIAN
    static uint16 FromHost16(uint16 x) { return bswap_16(x); }
    static uint16 ToHost16(uint16 x) { return bswap_16(x); }
    static uint32 FromHost24(uint32 x) { return bswap_24(x); }
    static uint32 ToHost24(uint32 x) { return bswap_24(x); }
    static uint32 FromHost32(uint32 x) { return bswap_32(x); }
    static uint32 ToHost32(uint32 x) { return bswap_32(x); }
    static uint64 FromHost64(uint64 x) { return gbswap_64(x); }
    static uint64 ToHost64(uint64 x) { return gbswap_64(x); }
    static unsigned __int128 FromHost128(unsigned __int128 x) { return gbswap_128(x); }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return gbswap_128(x); }
    static bool IsLittleEndian() { return true; }
#elif defined(IS_BIG_ENDIAN)
    static uint16 FromHost16(uint16 x) { return x; }
    static uint16 ToHost16(uint16 x) { return x; }
    static uint32 FromHost24(uint32 x) { return x; }
    static uint32 ToHost24(uint32 x) { return x; }
    static uint32 FromHost32(uint32 x) { return x; }
    static uint32 ToHost32(uint32 x) { return x; }
    static uint64 FromHost64(uint64 x) { return x; }
    static uint64 ToHost64(uint64 x) { return x; }
    static uint128 FromHost128(uint128 x) { return x; }
    static uint128 ToHost128(uint128 x) { return x; }
    static bool IsLittleEndian() { return false; }
#endif
    static uint16 Load16(const void* p) { return ToHost16(UnalignedLoad16(p)); }
    static void Store16(void* p, uint16 v) { UnalignedStore16(p, FromHost16(v)); }
    static uint32 Load32(const void* p) { return ToHost32(UnalignedLoad32(p)); }
    static void Store32(void* p, uint32 v) { UnalignedStore32(p, FromHost32(v)); }
    static uint64 Load64(const void* p) { return ToHost64(UnalignedLoad64(p)); }
    static uint64 Load64VariableLength(const void* const p, int len) {
        assert(len >= 1 && len <= 8);
        uint64 val = Load64(p); uint64 mask = 0; --len;
        do { mask = (mask << 8) | 0xff; } while (--len >= 0);
        return val & mask;
    }
    static void Store64(void* p, uint64 v) { UnalignedStore64(p, FromHost64(v)); }
    static uint128 Load128(const void* p) {
        return uint128(ToHost64(UnalignedLoad64(p)), ToHost64(UnalignedLoad64(reinterpret_cast<const uint64*>(p) + 1)));
    }
    static void Store128(void* p, const uint128& v) {
        UnalignedStore64(p, FromHost64(Uint128High64(v)));
        UnalignedStore64(reinterpret_cast<uint64*>(p) + 1, FromHost64(Uint128Low64(v)));
    }
    static uint128 Load128VariableLength(const void* p, int len) {
        if (len <= 8) return uint128(Load64VariableLength(static_cast<const char*>(p) + 8, len));
        return uint128(Load64VariableLength(p, len - 8), Load64(static_cast<const char*>(p) + 8));
    }
    static uword_t LoadUnsignedWord(const void* p) { return sizeof(uword_t) == 8 ? Load64(p) : Load32(p); }
    static void StoreUnsignedWord(void* p, uword_t v) { if (sizeof(uword_t) == 8) Store64(p, v); else Store32(p, v); }
};

typedef BigEndian NetworkByteOrder;
