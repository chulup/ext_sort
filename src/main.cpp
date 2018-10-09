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
            file orig_file = open_file_dma(filename, open_flags::rw).get0();

            const sstring tmp_filename = sprint("%s.sort_tmp", filename);
            file tmpfile = open_file_dma(tmp_filename, open_flags::rw | open_flags::create | open_flags::truncate).get0();

            auto in_stream = make_lw_shared(make_file_input_stream(orig_file, file_input_stream_options{102400}));
            auto out_stream = make_lw_shared(make_file_output_stream(tmpfile, 102400));

            do_until(
                [in_stream] () -> bool {
                    return in_stream->eof();
                },
                [out_stream, in_stream] () mutable -> future<> {
                    return in_stream->read_exactly(4096).then([out_stream] (auto buf) {
                        out_stream->write(buf.clone()).get();
//                        out_stream->write(std::move(buf)).get();
                    });
                }
            ).get();
            out_stream->flush().get(); // <= crash here
            tmpfile.close();
        });
    });

    return 0;
}
