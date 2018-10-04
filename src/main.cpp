#include <iostream>

#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/thread.hh>
#include <core/file.hh>
#include <core/sstring.hh>
#include <core/print.hh>
#include <core/fstream.hh>

using namespace seastar;

const size_t BLOCK = 4096;
const size_t ALIGNMENT = 4096;

#define USE_DMA_READ 0

future<> read_from_file(file &f, size_t pos, size_t length, char *out_buf) {
#if USE_DMA_READ
    return f.dma_read(pos, out_buf,  length).then([&](size_t read) {
        BOOST_ASSERT(read == length);
        return make_ready_future<>();
    });
#else
    return do_with(input_stream<char>(make_file_input_stream(f, pos, length)),
        [&](input_stream<char>& is) -> future<> {
            return is.read_exactly(length).then([&](temporary_buffer<char> buf) {
                std::memcpy(out_buf, buf.begin(), length);
            });
        });
#endif
}

future<> read_data(const sstring &filename) {
//    print("before open: %s\n", filename);
    return open_file_dma(filename, open_flags::ro).then([filename](file f) {
//            print("after open: %s\n", filename);
//            f.size().then([f = std::move(f)/*, &filename*/] (auto fsize) mutable {
//                print("File \"%s\" of size %ul\n", filename, fsize);
//                print("Read: %d, write: %d, memory: %d\n",
//                    f.disk_read_dma_alignment(),
//                    f.disk_write_dma_alignment(),
//                    f.memory_dma_alignment());
        const size_t size = BLOCK;
        auto bufptr = allocate_aligned_buffer<char>(size, ALIGNMENT);
        auto buf = bufptr.get();
        return read_from_file(f, 0, size, buf).then([bufptr = std::move(bufptr)] () {
            auto buf = bufptr.get();
            print("read data: %X%X%X%X\n",
                buf[0], buf[1], buf[2], buf[3]);
            return make_ready_future<>();
    });
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
        auto &filename = opts["filename"].as<sstring>();

        return read_data(filename);
    });

    return 0;
}
