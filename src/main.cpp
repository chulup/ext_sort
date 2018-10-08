#include <iostream>
#include <numeric>

#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/thread.hh>
#include <core/file.hh>
#include <core/sstring.hh>
#include <core/print.hh>
#include <core/fstream.hh>

using namespace seastar;

const size_t RECORD_SIZE = 4096;
const size_t IO_BLOCK_SIZE = 4096;

template<size_t RECORD_SIZE>
void sort_records(void *data, size_t len) {
    struct record_t {
        uint8_t data[RECORD_SIZE];
    };

    BOOST_ASSERT(len % sizeof(record_t) == 0);
    BOOST_ASSERT(sizeof(record_t) == RECORD_SIZE);
    const size_t count = len / sizeof(record_t);
    record_t *records = reinterpret_cast<record_t*>(data);
    std::sort(records, records + count-1, [] (const record_t& left, const record_t &right) {
        return std::memcmp(left.data, right.data, RECORD_SIZE) < 0;
    });
}

std::pair<size_t, uint64_t> calculate_smp_and_block_size(uint64_t /*filesize*/) {
    const uint64_t MEM_AVAILABLE = 1 << 21; // 10M

    // Sorting with maximum block size
    return std::make_pair(1, MEM_AVAILABLE);
}

int main(int argc, char *argv[]) {
    seastar::app_template app;
    namespace bpo = boost::program_options;

    app.add_positional_options({
        {
            "filename",
            bpo::value<sstring>()->required(),
            "file to sort",
            1
        }
    });

    app.run(argc, argv, [&] {
        auto &opts = app.configuration();
        const auto &filename = opts["filename"].as<sstring>();

        return async([&filename] () {

            file f = open_file_dma(filename, open_flags::rw).get0();
            uint64_t fsize = f.size().get0();
            if (fsize % RECORD_SIZE != 0) {
                throw std::runtime_error("File size is not a multiple of RECORD_SIZE");
            }

            size_t sort_concurrency;
            uint64_t sort_block_size;
            std::tie(sort_concurrency, sort_block_size) = calculate_smp_and_block_size(fsize);

            /* QtCreator checker doesn't like this way*/
//            const auto [sort_concurrency, sort_block_size] = calculate_smp_and_block_size(fsize);

            // Sort blocks
            // This is done in one thread for blocks to be as large as possible
            // Change calculate_smp_and_block_size to do it in another way
            parallel_for_each(
                    boost::irange<size_t>(0, sort_concurrency),
                    [&f, fsize, sort_concurrency, sort_block_size] (size_t id) -> future<> {
                size_t start = sort_block_size * id;

                auto buf = temporary_buffer<uint8_t>::aligned(f.memory_dma_alignment(), sort_block_size);
                // Calculate positions current execution thread will sort;
                // currently for 4 threads it's 0, 4, 8...; 1, 5, 9...; 2, 6, 10...; 3, 7, 11...
                auto positions = boost::irange<uint64_t>(start, fsize, sort_block_size * sort_concurrency);

                // Sort blocks
                do_for_each(positions,
                        [&buf, &f] (uint64_t position) mutable -> future<> {
                    return f.dma_read(position, buf.get_write(), buf.size())
                            .then([&buf, position, &f] (size_t read_bytes) mutable -> future<>
                    {
                        // read_bytes could be less than buf.size() when in the end of file;
                        sort_records<RECORD_SIZE>(buf.get_write(), read_bytes);

                        return f.dma_write(position, buf.get(), read_bytes)
                            .then([read_bytes] (auto write_bytes) {
                                if (read_bytes != write_bytes) {
                                    return make_exception_future(std::runtime_error("Failed to write all the data"));
                                }
                                return make_ready_future();
                            });
                    });
                }).get();
                return make_ready_future();
            }).get();

//            // Merge blocks
//            auto buf1 = temporary_buffer<uint8_t>::aligned(f.memory_dma_alignment(), sort_block_size);
//            auto buf2 = temporary_buffer<uint8_t>::aligned(f.memory_dma_alignment(), sort_block_size);

        });
    });

    return 0;
}
