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

// Helper structure which data could be casted to; allows us to use ::operator<() on raw blocks of known size
struct record_t {
    char data[RECORD_SIZE];
};

bool operator<(const record_t &left, const record_t &right) {
    return std::memcmp(left.data, right.data, sizeof(record_t::data)) < 0;
}

typedef temporary_buffer<char> tmp_buf;

uint64_t get_available_memory(uint64_t /*filesize*/) {
    const uint64_t MEM_AVAILABLE = 1 << 30; // 1M

    // Sorting with maximum block size
    return MEM_AVAILABLE;
}

/// Sort block of in_file data, write on the same position to out_file
future<> sort_block(file in_file, file out_file, uint64_t position, uint64_t size) {
    auto buffer = tmp_buf::aligned(in_file.disk_read_dma_alignment(), size);
    return in_file.dma_read(position, buffer.get_write(), size).then(
            [position, out_file, buffer = buffer.share()] (const uint64_t read_bytes) mutable {
        if (read_bytes % RECORD_SIZE != 0) {
            // Something went wrong
            // We know file size is a multiple of RECORD_SIZE and every write we do have to be multiple of that too
            throw std::runtime_error("dma_read() read unexpected byte count");
        }

        // Use std::sort to sort data in memory
        const size_t count = buffer.size() / sizeof(record_t);
        record_t *records = reinterpret_cast<record_t*>(buffer.get_write());
        std::sort(records, records + count);

        // Write sorted block to the same place in temporary file
        return out_file.dma_write(position, buffer.get(), read_bytes).then(
                [out_file, buffer = buffer.share()] (const auto /*written_bytes*/) {
            // read_bytes != written_bytes ???
            return make_ready_future();
        });
    });
}

typedef struct {
    input_stream<char> stream;
    tmp_buf current_record = {};
} stream_with_record;

bool operator< (const stream_with_record &left, const stream_with_record &right) {
    const record_t *left_r = reinterpret_cast<const record_t*>(left.current_record.get());
    const record_t *right_r = reinterpret_cast<const record_t*>(right.current_record.get());

    return *left_r < *right_r;
};

// Merge sorted blocks of data from known positions in in_file, using up to `mem_available` memory for buffers
// Write sorted data to `out_file`
future<> merge_blocks(file in_file, file out_file, std::vector<uint64_t> positions, uint64_t block_size, size_t mem_available) {
    auto buffer_size = mem_available / (positions.size() + 2 /* twice the size for output buffer */);
     // make sure buffer size is a multiple of write_alignment
    buffer_size = (buffer_size / out_file.disk_write_dma_alignment()) * out_file.disk_write_dma_alignment();


    // Create output streams
    auto sorted_streams = make_lw_shared(std::vector<stream_with_record>{});
    std::for_each(positions.begin(), positions.end(),
            [&, sorted_streams] (auto pos) {
        sorted_streams->push_back(stream_with_record {
            make_file_input_stream(in_file, pos, block_size, file_input_stream_options{buffer_size})
        });
    });

    // Fill buffers with initial records
    return parallel_for_each(boost::irange<size_t>(0, sorted_streams->size()),
            [sorted_streams] (size_t index) -> future<> {
        return sorted_streams->at(index).stream.read_exactly(RECORD_SIZE).then(
                [index, sorted_streams] (tmp_buf buffer) {
            auto &s = sorted_streams->at(index);
            s.current_record = std::move(buffer);
            return make_ready_future();
        });
    }).then([sorted_streams, out_file, buffer_size] {
        auto out_stream = make_lw_shared(make_file_output_stream(out_file, buffer_size * 2));

        return do_until(
            [sorted_streams] () -> bool {
                return sorted_streams->empty();
            },
            [sorted_streams, out_stream] () mutable -> future<> {
                auto min_stream = std::min_element(sorted_streams->begin(), sorted_streams->end());
                auto min_record = min_stream->current_record.share();

                // Simultaneously send min_record to be written and read new at the same stream
                return when_all_succeed(
                    out_stream->write(min_record.get(), min_record.size()),
                    min_stream->stream.read_exactly(RECORD_SIZE)
                        .then([sorted_streams, min_stream] (tmp_buf buffer) mutable {
                            if (min_stream->stream.eof()) {
                                sorted_streams->erase(min_stream);
                                return make_ready_future();
                            }
                            min_stream->current_record = std::move(buffer);
                            return make_ready_future();
                        })
                ).then([min_record = std::move(min_record)] {}); // We need to wait for possible sorted_streams change before proceeding
            }
        ).then([out_stream] {
            out_stream->flush();
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

        return async([&filename] () {
            file orig_file = open_file_dma(filename, open_flags::rw).get0();
            uint64_t fsize = orig_file.size().get0();
            if (fsize % RECORD_SIZE != 0) {
                throw std::runtime_error("File size is not a multiple of RECORD_SIZE");
            }
            const sstring out_name = sprint("%s.sort_tmp", filename);
            file tmp_file = open_file_dma(out_name, open_flags::rw | open_flags::create | open_flags::truncate).get0();

            const uint64_t block_size = get_available_memory(fsize);
            const auto positions = boost::irange<uint64_t>(0, fsize, block_size);

            do_for_each(positions,
                    [=] (const auto pos) -> future<> {
                return sort_block(orig_file, tmp_file, pos, block_size);
            }).then([=] {
                std::vector<uint64_t> positions_vec;
                std::for_each(positions.begin(), positions.end(), [&positions_vec] (uint64_t pos) {
                    positions_vec.push_back(pos);
                });
                return merge_blocks(tmp_file, orig_file, std::move(positions_vec), block_size, block_size);
            }).get();

            orig_file.close();
            tmp_file.close();
        });
    });

    return 0;
}
