#include <iostream>

#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/thread.hh>
#include <core/file.hh>
#include <core/sstring.hh>
#include <core/print.hh>
#include <core/fstream.hh>

using namespace seastar;

//const size_t BLOCK = 4096;
//const size_t ALIGNMENT = 4096;

future<> read_data(const sstring &filename) {
    print("before open: %s\n", filename);
    return open_file_dma(filename, open_flags::ro)
        .then([filename](file f) {
            print("after open: %s\n", filename);
            f.size().then([f = std::move(f), &filename] (auto fsize) mutable {
                print("File \"%s\" of size %ul\n", filename, fsize);
                print("Read: %d, write: %d, memory: %d\n",
                    f.disk_read_dma_alignment(),
                    f.disk_write_dma_alignment(),
                    f.memory_dma_alignment());

#if 1
                return f.template dma_read_exactly<char>(0, 4096).then([](auto buf) {
                    print("Read %d bytes: %X%X%X%X\nsizeof(tmp_buf[0]) = %d\n",
                                buf.size(),
                                buf[0], buf[1], buf[2], buf[3],
                                sizeof(buf[1]));
                    return make_ready_future<>();
                }).finally([&f]{});
#else
                return do_with(input_stream<char>(make_file_input_stream(std::move(f))),
                    [](input_stream<char>& is) -> future<> {
                        return is.read_up_to(4096).then([](auto tmp_buf) {
                            print("Read %d bytes: %X%X%X%X\nsizeof(tmp_buf[0]) = %d\n",
                                tmp_buf.size(),
                                tmp_buf[0], tmp_buf[1], tmp_buf[2], tmp_buf[3],
                                sizeof(tmp_buf[1]));
                            return make_ready_future<>();
                        });
                });
#endif
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
