#include "helpers.h"
#include <utility>
#include <fmt/format.h>
#include <core/memory.hh>
#include <core/reactor.hh>
#include <core/temporary_buffer.hh>
#include <core/print.hh>

using namespace seastar;

uint64_t get_max_buffer_size() {
#if TEST_MEMORY_LIMITS
    const auto stats = memory::stats();
    const auto total_free = stats.free_memory();
    const auto thread_count = smp::count;
    seastar::print("free mem: %s, thread_count: %d\n", pp_number(total_free), thread_count);
    seastar::print("memory per thread?? %s\n", pp_number(total_free / thread_count));
    seastar::print("allocated: %s\n", pp_number(stats.allocated_memory()));
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
//            seastar::print("Allocated buffer of size %s\n", pp_number(buf.size()));
#endif
        } catch (const std::bad_alloc &) {
#if TEST_MEMORY_LIMITS
//            seastar::print("Got bad_alloc on size %s\n", pp_number(current));
#endif
            break;
        }
        current += increment;
    }
#if TEST_MEMORY_LIMITS
//    seastar::print("Allocating buffers starting from size %s with decrement %s\n", pp_number(current), pp_number(decrement));
#endif
    while (true) {
        try {
            auto buf = temporary_buffer<char>::aligned(4096, current);
#if TEST_MEMORY_LIMITS
//            seastar::print("Allocated buffer of size %s\n", pp_number(buf.size()));
#endif
            break;
        } catch (const std::bad_alloc &) {
#if TEST_MEMORY_LIMITS
//            seastar::print("Got bad_alloc on size %s\n", pp_number(current));
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


// TODO: open file with open_flags::exclusive
// TODO: delete file after use
future<std::pair<sstring, file>> open_temp_file(const sstring &orig_filename) {
    static size_t counter = 0;

    const sstring name = format("{}.tmp_{}", orig_filename, counter++);
    return open_file_dma(name, open_flags::rw | open_flags::create | open_flags::truncate).then(
            [name] (file result) -> future<std::pair<sstring, file>> {
        return make_ready_future<std::pair<sstring, file>>(std::make_pair(name, result));
    });
}

