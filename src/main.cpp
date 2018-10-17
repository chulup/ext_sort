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
#include "util/log.hh"

using namespace seastar;

seastar::logger logs("ext_sort");

/// Sort block of in_file data, write to out_file from the start
/// Caller must ensure the output file will not have excess data at the end
future<> sort_block(file &in_file, temp_data_t &temp_data) {
    auto buffer = tmp_buf::aligned(in_file.disk_read_dma_alignment(), temp_data.size);
    logs.info("Sorting block at position {} with size of {}\n",
        pp_number(temp_data.orig_position), pp_number(temp_data.size));
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
                    logs.info("Erasing stream\n");
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
template<class Range>
future<> merge_files(const Range &input_files, file out_file, size_t mem_available) {
    auto buffer_size = mem_available / (input_files.size() + 4 /* twice the size for output buffer */);
     // make sure buffer size is a multiple of write_alignment
    buffer_size = (buffer_size / out_file.disk_write_dma_alignment()) * out_file.disk_write_dma_alignment();

    size_t total_size = 0;
    // Create output streams
    std::vector<stream_with_record> sorted_streams;
    std::for_each(input_files.begin(), input_files.end(),
            [&sorted_streams, buffer_size, &total_size] (auto data_file) mutable {
        sorted_streams.push_back(stream_with_record {
            make_file_input_stream(data_file.file, 0, data_file.size, file_input_stream_options{buffer_size})
        });
        logs.info("Created stream for temp file of size {}", pp_number(data_file.size));
        total_size += data_file.size;
    });

    logs.info("Merging {} files with total size of {}; each input stream got {} buffer\n",
        sorted_streams.size(), pp_number(total_size), pp_number(buffer_size));
    print_mem_stats();

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
                return out_stream.flush();
            });
        });
    });
}

future<> remove_n_first(std::vector<temp_data_t> &in_files, size_t count) {
    if (in_files.size() < count) {
        count = in_files.size();
    }
    return parallel_for_each(in_files.begin(), in_files.begin() + static_cast<int>(count),
            [] (auto temp_data) {
        return temp_data.file.close();
    })
    .then([&in_files, count] {
        in_files.erase(in_files.begin(), in_files.begin() + static_cast<int>(count));
        return make_ready_future();
    });
}

future<> merge_smallest_files(std::vector<temp_data_t> &in_files, size_t mem_available, sstring &path) {
    return do_until(
            [&in_files, mem_available] {
        // Stop when temp file count is MERGE_WAYS or less, or buffer size for each temp file is more than MIN_BUFFER_SIZE
        return (in_files.size() <= MERGE_WAYS) || ((mem_available / in_files.size() + 4) > MIN_BUFFER_SIZE);
    }, [&in_files, mem_available, &path] () mutable {
        // create new temporary file
        return open_temp_file(path)
            .then([&in_files, mem_available] (auto new_file) {
            // find MERGE_WAYS smallest files
            std::partial_sort(in_files.begin(), in_files.begin() + MERGE_WAYS, in_files.end());

            auto &new_temp_data = in_files.emplace_back(temp_data_t{new_file, 0u /*size*/, 0u /*position*/});
            auto range = boost::make_iterator_range(in_files.begin(), in_files.begin() + MERGE_WAYS);
            for (const auto &temp: range) {
                new_temp_data.size += temp.size;
            }

            return merge_files(range, new_temp_data.file, mem_available);
        })
        .then([&in_files] {
            return remove_n_first(in_files, MERGE_WAYS);
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
            }).then([&orig_file, &temp_files, max_buffer_size, filename] {
                return do_with(
                        std::move(orig_file),
                        std::move(temp_files),
                        std::move(filename),
                        [max_buffer_size] (auto &orig_file, auto &tmp_files, auto &filename) -> future<>
                {
                    // Sort each block and write it to the temporary file
                    return do_for_each(tmp_files,
                            [&orig_file] (auto &tmp_file) -> future<> {
                        return sort_block(orig_file, tmp_file);

                    })

                    // Merge MERGE_WAYS of the smallest files until we have no more than MERGE_WAYS files left
                    // or the memory buffer size for each input is no less than MIN_BUFFER_SIZE
                    .then([&tmp_files, max_buffer_size, &filename] () mutable {
                        return merge_smallest_files(tmp_files, max_buffer_size, filename);
                    })

                    // Merge all remaining temporary files, writing them back to original file
                    .then([&orig_file, max_buffer_size, &tmp_files] () {
                        auto range = boost::make_iterator_range(tmp_files.begin(), tmp_files.end());
                        return merge_files(range, orig_file, max_buffer_size);

                    })
                    // close temp files
                    .then([&tmp_files] {
                        return remove_n_first(tmp_files, tmp_files.size());
                    })
                    .then([&orig_file] {
                        logs.info("Flushing original file");
                        return orig_file.flush();
                    })
                    .finally([&orig_file] {
                        logs.info("Closing original file");
                        return orig_file.close();
                    })
                    ;
                })
                ;
            }).get();

        });
    });

    return 0;
}
