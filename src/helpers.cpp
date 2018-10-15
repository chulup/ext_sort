#include "helpers.h"
#include <fmt/format.h>
#include <core/memory.hh>
#include <core/reactor.hh>
#include <core/temporary_buffer.hh>
#include <core/print.hh>

uint64_t get_max_buffer_size() {
    const auto stats = seastar::memory::stats();

    const auto total_free = stats.free_memory();
    const auto thread_count = seastar::smp::count;

//    seastar::print("free mem: %s, thread_count: %d\n", pp_number(total_free), thread_count);
//    seastar::print("memory per thread?? %s\n", pp_number(total_free / thread_count));
//    seastar::print("allocated: %s\n", pp_number(stats.allocated_memory()));

    const uint64_t increment = 512 * 1024 * 1024; // 512M
    const uint64_t decrement = 64 * 1024 * 1024; // 64M

    uint64_t current = increment;

//    seastar::print("Allocating buffers starting from size %s with increment %s\n", pp_number(current), pp_number(increment));
    while (true) {
        try {
            auto buf = seastar::temporary_buffer<char>::aligned(4096, current);
//            seastar::print("Allocated buffer of size %s\n", pp_number(buf.size()));
        } catch (const std::bad_alloc &) {
//            seastar::print("Got bad_alloc on size %s\n", pp_number(current));
            break;
        }
        current += increment;
    }
//    seastar::print("Allocating buffers starting from size %s with decrement %s\n", pp_number(current), pp_number(decrement));
    while (true) {
        try {
            auto buf = seastar::temporary_buffer<char>::aligned(4096, current);
//            seastar::print("Allocated buffer of size %s\n", pp_number(buf.size()));
            break;
        } catch (const std::bad_alloc &) {
//            seastar::print("Got bad_alloc on size %s\n", pp_number(current));
        }
        current -= decrement;
    }

    seastar::print("Maximum buffer size is %s\n", pp_number(current));

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
