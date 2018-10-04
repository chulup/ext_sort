#include <iostream>

#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/thread.hh>
#include <core/file.hh>
#include <core/sstring.hh>
#include <core/print.hh>

using namespace seastar;

const size_t BLOCK = 4096;
const size_t ALIGNMENT = 4096;

future<> read_data(const sstring &filename) {
    return open_file_dma(filename, open_flags::ro)
        .then([](file f) {
            f.size().then([&](auto fsize) {
                return f.dma_read<uint8_t>(0, 4096).then([](auto temp_buf){

                });
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
