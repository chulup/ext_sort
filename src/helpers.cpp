#include "helpers.h"
#include <utility>
#include <filesystem>
#include <fmt/format.h>
#include <core/memory.hh>
#include <core/reactor.hh>
#include <core/temporary_buffer.hh>
#include <core/print.hh>

using namespace seastar;

void print_mem_stats() {
    const auto stats = memory::stats();
    const auto total_free = stats.free_memory();
    seastar::print("free mem: %s, thread_count: %u\n", pp_number(total_free), smp::count);
    seastar::print("allocated: %s\n", pp_number(stats.allocated_memory()));
    seastar::print("mallocs: %llu; live objects: %llu\n", stats.mallocs(), stats.live_objects());
}


uint64_t get_max_buffer_size() {
#if TEST_MEMORY_LIMITS
    print_mem_stats();
#endif

    const uint64_t increment = 512 * 1024 * 1024; // 512M
    const uint64_t decrement = 64 * 1024 * 1024; // 64M

    uint64_t current = increment;

#if TEST_MEMORY_LIMITS
    seastar::print("Allocating buffers starting from size %s with increment %s\n", pp_number(current), pp_number(increment));
#endif
    while (true) {
        try {
            auto buf = temporary_buffer<char>::aligned(4096, current);
#if TEST_MEMORY_LIMITS
            seastar::print("Allocated buffer of size %s\n", pp_number(buf.size()));
#endif
        } catch (const std::bad_alloc &) {
#if TEST_MEMORY_LIMITS
            seastar::print("Got bad_alloc on size %s\n", pp_number(current));
#endif
            break;
        }
        current += increment;
    }
#if TEST_MEMORY_LIMITS
    seastar::print("Allocating buffers starting from size %s with decrement %s\n", pp_number(current), pp_number(decrement));
#endif
    while (true) {
        try {
            auto buf = temporary_buffer<char>::aligned(4096, current);
#if TEST_MEMORY_LIMITS
            seastar::print("Allocated buffer of size %s\n", pp_number(buf.size()));
#endif
            break;
        } catch (const std::bad_alloc &) {
#if TEST_MEMORY_LIMITS
            seastar::print("Got bad_alloc on size %s\n", pp_number(current));
#endif
        }
        current -= decrement;
    }

    print("Maximum buffer size is %s\n", pp_number(current));

    return current;
}

std::string pp_number(uint64_t number) {
    static std::vector<char> prefixes = {
        'b',
        'K',
        'M',
        'G',
        'T',
        'P',
        'E',
    };
    double test = number;
    size_t order = 0;
    while (test > 1000) {
        test /= 1000;
        order ++;
    }
    return fmt::format("{:.1f} {}", test, prefixes[order]);
}


future<file> open_temp_file(const sstring &path) {
    std::filesystem::path fs_path{path};
    if (fs_path.has_filename()) {
        fs_path = fs_path.parent_path();
    }
    return open_file_dma(fs_path.c_str(), open_flags::rw | open_flags(O_TMPFILE)).then(
            [] (auto result) {
        return make_ready_future<file>(result);
    });
}

