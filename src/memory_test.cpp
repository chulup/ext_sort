#include <core/memory.hh>
#include <core/reactor.hh>
#include <core/temporary_buffer.hh>
#include <core/print.hh>
#include <core/app-template.hh>

const auto GB = 1024*1024*1024.0;

int main(int argc, char *argv[]) {
    seastar::app_template app;

    app.run(argc, argv, [&] {
        const auto stats = seastar::memory::stats();
        const auto total_free = stats.free_memory();
        const auto thread_count = seastar::smp::count;
        seastar::print("free mem: %.2f G, thread_count: %u\n", total_free/GB, thread_count);
        seastar::print("memory per thread: %.2f G\n", (total_free/thread_count)/GB);

        const uint64_t increment = 512 * 1024 * 1024; // 512M
        const uint64_t decrement = 64 * 1024 * 1024; // 64M
        uint64_t current = increment;

        seastar::print("Allocating buffers starting from size %.2f G with increment %.2f G\n", current/GB, increment/GB);
        while (true) {
            try {
                auto buf = seastar::temporary_buffer<char>::aligned(4096, current);
                current += increment;
            } catch (const std::bad_alloc &) {
                seastar::print("Got bad_alloc on size %.2f G\n", current/GB);
                break;
            }
        }

        seastar::print("Allocating buffers starting from size %.2f G with decrement %.2f G\n", current/GB, decrement/GB);
        while (true) {
            try {
                auto buf = seastar::temporary_buffer<char>::aligned(4096, current);
                seastar::print("Successfully allocated buffer of size %.2f G\n", current/GB);
                break;
            } catch (const std::bad_alloc &) {
                current -= decrement;
            }
        }
        seastar::print("Maximum buffer size is %.2f G\n", current/GB);

        return seastar::make_ready_future<>();
    });

    return 0;
}
