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

const size_t RECORD_SIZE = 16;
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
//    std::sort(records, records + count-1, [] (const record_t& left, const record_t &right) {
//        return std::memcmp(left.data, right.data, RECORD_SIZE) < 0;
//    });

    std::vector<size_t> indexes(count);
    std::iota(indexes.begin(), indexes.end(), 0);
    std::sort(indexes.begin(), indexes.end(), [&] (size_t left, size_t right) {
        return std::memcmp(records[left].data, records[right].data, sizeof(record_t)) < 0;
    });

    std::vector<record_t> tmp(indexes.size());
    for (size_t i = 0; i < tmp.size(); i++) {
        tmp[i] = records[indexes[i]];
    }
    std::memcpy(data, tmp.data(), tmp.size() * sizeof(tmp[0]));
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
            const size_t sort_concurrency = seastar::smp::count;
            const size_t sort_block_size = IO_BLOCK_SIZE; // while RECORD_SIZE is small

            parallel_for_each(
                    boost::irange<size_t>(0, sort_concurrency),
                    [&f, fsize, sort_concurrency] (size_t id) -> future<> {
                size_t start = sort_block_size * id;

                auto buf = temporary_buffer<uint8_t>::aligned(f.memory_dma_alignment(), sort_block_size);
                // Calculate positions current execution thread will sort;
                // currently for 4 threads it's 0, 4, 8...; 1, 5, 9...; 2, 6, 10...; 3, 7, 11...
                auto positions = boost::irange<uint64_t>(start, fsize, sort_block_size * sort_concurrency);

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
        });
    });

    return 0;
}
