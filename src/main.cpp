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
const size_t ALIGNMENT = 4096;

#define USE_DMA_READ 0

future<> read_from_file(file &f, uint64_t pos, size_t length, char *out_buf) {
#if USE_DMA_READ
//    return f.dma_read(pos, out_buf,  length).then([&](size_t read) {
//        BOOST_ASSERT(read == length);
//        return make_ready_future<>();
//    });
    return f.template dma_read_bulk<char>(pos, length).then([out_buf, length] (temporary_buffer<char> buf) {
        std::memcpy(out_buf, buf.begin(), std::min(buf.size(), length));
    });
#else
    return do_with(input_stream<char>(make_file_input_stream(f, pos, length)),
        [out_buf, length](input_stream<char>& is) -> future<> {
            return is.read_exactly(length).then([out_buf, length](temporary_buffer<char> buf) {
                std::memcpy(out_buf, buf.begin(), length);
            });
        });
#endif
}

template<size_t RECORD_SIZE>
void sort_records(void *data, size_t len) {
    BOOST_ASSERT(len % RECORD_SIZE == 0);

    struct record_t {
        uint8_t data[RECORD_SIZE];
    };

    record_t *records = reinterpret_cast<record_t*>(data);
    const size_t count = len / RECORD_SIZE;

    std::sort(records, &records[count], [] (const record_t& left, const record_t &right) {
        return std::memcmp(left.data, right.data, RECORD_SIZE) < 0;
    });
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

        return async([&filename] {
            const size_t sort_concurrency = seastar::smp::count;
            const size_t sort_block_size = IO_BLOCK_SIZE; // while RECORD_SIZE is small

            file f = open_file_dma(filename, open_flags::rw).get0();

            parallel_for_each(
                    boost::irange<size_t>(0, sort_concurrency),
                    [&f] (size_t id) {
                auto buf = temporary_buffer<uint8_t>::aligned(f.memory_dma_alignment(), sort_block_size);
                const size_t position = sort_block_size * id;

                return f.dma_read(position, buf.get_write(), buf.size())
                        .then([&buf, position, &f] (size_t read_bytes) mutable {
                    sort_records<RECORD_SIZE>(buf.get_write(), read_bytes);

                    return f.dma_write(position, buf.get(), read_bytes)
                        .then([read_bytes] (auto write_bytes) {
                            if (read_bytes != write_bytes) {
                                return make_exception_future(std::runtime_error("Failed to write all the data"));
                            }
                            return make_ready_future();
                        });
                }).get0();
            }).get();
        });

//        return file_size(filename).then([&filename, &bufptr] (uint64_t fsize) {
//            return open_file_dma(filename, open_flags::rw).then([/*fsize, */&bufptr](file f) {
//                print("File has been opened\n");

//                size_t pos = 0;

//                return read_from_file(f, pos, bufptr.size(), bufptr.get_write())/*.then([&bufptr] () mutable {
//                    const auto buf = bufptr.begin();
//                    print("Read data: %X %X %X %X\n", buf[0], buf[1], buf[2], buf[3]);
//                    return sort_records(bufptr);
//                })*/;
//            });
//        });
    });

    return 0;
}
