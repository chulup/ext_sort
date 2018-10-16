#include <iostream>
#include <numeric>

#include "structures.h"
#include "helpers.h"

#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/thread.hh>
#include <core/file.hh>
#include <core/sstring.hh>
#include <core/print.hh>
#include <core/fstream.hh>

using namespace seastar;





/// Sort block of in_file data, write to out_file from the start
/// Caller must ensure the output file will not have excess data at the end
future<> sort_block(file &in_file, temp_data_t &temp_data) {
    auto buffer = tmp_buf::aligned(in_file.disk_read_dma_alignment(), temp_data.size);
    return in_file.dma_read(temp_data.orig_position, buffer.get_write(), temp_data.size).then(
            [out_file = temp_data.file, buffer = buffer.share()] (const uint64_t read_bytes) mutable {
        if (read_bytes % RECORD_SIZE != 0) {
            // Something went wrong
            // We know file size is a multiple of RECORD_SIZE and every write we do have to be multiple of that too
            return make_exception_future(std::runtime_error("dma_read() read unexpected byte count"));
        }

        // Use std::sort to sort data in memory
        const size_t count = read_bytes / sizeof(record_t);
        record_t *records = reinterpret_cast<record_t*>(buffer.get_write());
        std::sort(records, records + count);

        // Write sorted block to the same place in temporary file
        return out_file.dma_write(0, buffer.get(), read_bytes).then(
                [buffer = buffer.share()] (const auto /*written_bytes*/) {
            // read_bytes != written_bytes ???
            return make_ready_future();
        });
    });
}

future<> write_minimum_record(std::vector<stream_with_record> &streams, output_stream<char> &out_stream) {
    // Find stream with minimal record using
    auto min_stream = std::min_element(streams.begin(), streams.end());
    auto min_record = min_stream->current_record.share();

    // Simultaneously send min_record to be written and read new at the same stream
    return do_with(
            std::move(min_stream),
            std::move(min_record),
            [&streams, &out_stream] (auto &min_stream, auto &min_record) mutable -> future<> {
        return when_all_succeed(
            min_stream->stream.read_exactly(RECORD_SIZE).then(
                    [&streams, &min_stream] (tmp_buf buffer) mutable -> future<> {
                if (min_stream->stream.eof()) {
                    streams.erase(min_stream);
                    return make_ready_future();
                }
                min_stream->current_record = std::move(buffer);
                return make_ready_future();
            }),
            out_stream.write(min_record.get(), min_record.size())
        );
    });
}

// Merge sorted blocks of data from known positions in in_file, using up to `mem_available` memory for buffers
// Write sorted data to `out_file`
future<> merge_files(const std::vector<temp_data_t> &input_files, file &out_file, size_t mem_available) {
    auto buffer_size = mem_available / (input_files.size() + 2 /* twice the size for output buffer */);
     // make sure buffer size is a multiple of write_alignment
    buffer_size = (buffer_size / out_file.disk_write_dma_alignment()) * out_file.disk_write_dma_alignment();

    // Create output streams
    std::vector<stream_with_record> sorted_streams;
    std::for_each(input_files.begin(), input_files.end(),
            [&sorted_streams, buffer_size] (auto data_file) mutable {
        sorted_streams.push_back(stream_with_record {
            make_file_input_stream(data_file.file, 0, data_file.size, file_input_stream_options{buffer_size})
        });
    });
    // give writing stream twice the memory of reading streams for less write operations
    // allow it to write its buffers in background
    file_output_stream_options out_options;
    out_options.buffer_size = unsigned(buffer_size * 2);
    out_options.write_behind = 2;

    // Fill buffers with initial records
    return do_with(
            std::move(sorted_streams),
            make_file_output_stream(out_file, out_options),
            [] (auto &sorted_streams, auto &out_stream) -> future<>
    {
        return parallel_for_each(boost::irange<size_t>(0, sorted_streams.size()),
                [&sorted_streams] (size_t index) -> future<> {
            return sorted_streams.at(index).stream.read_exactly(RECORD_SIZE).then(
                    [index, &sorted_streams] (tmp_buf buffer) mutable {
                auto &s = sorted_streams.at(index);
                s.current_record = std::move(buffer);
                return make_ready_future();
            });
        }).then([&sorted_streams, &out_stream] () mutable -> future<> {
            return do_until(
                [&sorted_streams] () -> bool {
                    return sorted_streams.empty();
                },
                [&sorted_streams, &out_stream] () mutable -> future<> {
                    return write_minimum_record(sorted_streams, out_stream);
                }
            ).then([&out_stream] {
                out_stream.flush();
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
        const auto &filename = opts["filename"].as<sstring>();

        return async([filename] () {
            file orig_file = open_file_dma(filename, open_flags::rw).get0();
            uint64_t fsize = orig_file.size().get0();

            if (fsize % RECORD_SIZE != 0) {
                throw std::runtime_error("File size is not a multiple of RECORD_SIZE");
            }
            const uint64_t max_buffer_size = get_max_buffer_size();

#if TEST_MEMORY_LIMITS
            return;
#endif
            std::vector<temp_data_t> temp_files;
            parallel_for_each(boost::irange<uint64_t>(0, fsize, max_buffer_size),
                    [&temp_files, &filename, max_buffer_size] (uint64_t position) {
                return open_temp_file(filename).then(
                        [&temp_files, position, max_buffer_size] (auto temp_file) {
                    temp_files.push_back(temp_data_t{
                        temp_file,
                        max_buffer_size,
                        position
                    });
                    return make_ready_future();
                });
            }).then([&orig_file, &temp_files, max_buffer_size] {
                return do_with(
                        std::move(orig_file),
                        std::move(temp_files),
                        [max_buffer_size] (auto &orig_file, auto &tmp_files) -> future<>
                {
                    // Sort each block and write it to the temporary file
                    return do_for_each(tmp_files,
                            [&orig_file] (auto &tmp_file) -> future<> {
                        return sort_block(orig_file, tmp_file);

                    // Merge all blocks from temporary file, writing them back to original file
                    }).then([&orig_file, max_buffer_size, &tmp_files] {
                        return merge_files(tmp_files, orig_file, max_buffer_size);

                    // flush files at the end
                    })
//                    .then([&tmp_files] {
//                      return do_for_each(tmp_files, [] (auto tmp_file) {
//                            return tmp_file.file.close();
//                        });
//                    })
                    .then([&orig_file] {
                        return orig_file.flush();
                    })
                    /*.finally([&orig_file] {
                        return orig_file.close();
                    })*/
                    .then([] {
                        return seastar::sync_directory("/media/chulup/c3837106-b315-4c5e-bf26-2015ccb774d7/");
                    })
//                    .then([&orig_file] {
//                        return orig_file.close();
//                    })
                    ;
                });
            }).get();
        });
    });

    return 0;
}
