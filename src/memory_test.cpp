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
        seastar::print("free_memory: %.2f G, smp::count: %u\n", total_free/GB, thread_count);
        seastar::print("free_memory/smp::count : %.2f G\n", (total_free/thread_count)/GB);

        const uint64_t increment = 512 * 1024 * 1024; // 512M
        const uint64_t decrement = 64 * 1024 * 1024; // 64M

        std::vector<seastar::temporary_buffer<char>> v(8);
        uint64_t total_size = 0;

        for (unsigned i = 0; i < v.size(); i++) {
            seastar::print("Allocating buffer #%d... ", i);
            seastar::temporary_buffer<char> buffer;
            uint64_t current = increment;
            while (true) {
                try {
                    buffer = seastar::temporary_buffer<char>::aligned(4096, current);
                    current += increment;
                    std::move(buffer);
                } catch (const std::bad_alloc &) {
                    break;
                }
            }

            while (true) {
                try {
                    buffer = seastar::temporary_buffer<char>::aligned(4096, current);
                    break;
                } catch (const std::bad_alloc &) {
                    current -= decrement;
                }
            }
            v[i] = std::move(buffer);
            total_size += v[i].size();
            seastar::print("maximum size is %.2f G\n", current/GB);
        }

        seastar::print("Total memory allocated in %d buffers is %.2f G\n", v.size(), total_size/GB);

        return seastar::make_ready_future<>();
    });

    return 0;
}
