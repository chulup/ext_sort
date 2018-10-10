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

//#pragma pack(push, 1)
struct record_t {
    char data[RECORD_SIZE];
};
//#pragma pack(pop)
bool operator<(const record_t &left, const record_t &right) {
    return std::memcmp(left.data, right.data, sizeof(record_t::data)) < 0;
}

typedef temporary_buffer<char> tmp_buf;

typedef struct {
    input_stream<char> stream;
    tmp_buf current_record = {};
    uint64_t read_bytes = 0;
    uint64_t reads = 0;
} stream_with_record;

bool operator< (const stream_with_record &left, const stream_with_record &right) {
    const record_t *left_r = reinterpret_cast<const record_t*>(left.current_record.get());
    const record_t *right_r = reinterpret_cast<const record_t*>(right.current_record.get());

    return *left_r < *right_r;
};

uint64_t get_available_memory(uint64_t /*filesize*/) {
    const uint64_t MEM_AVAILABLE = 1 << 30; // 1M

    // Sorting with maximum block size
    return MEM_AVAILABLE;
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
            file in_file = open_file_dma(filename, open_flags::ro).get0();
            uint64_t fsize = in_file.size().get0();
            if (fsize % RECORD_SIZE != 0) {
                throw std::runtime_error("File size is not a multiple of RECORD_SIZE");
            }
            const sstring out_name = sprint("%s.sort_tmp", filename);
            file out_file = open_file_dma(out_name, open_flags::rw | open_flags::create | open_flags::truncate).get0();

            const uint64_t block_size = get_available_memory(fsize);
            const auto positions = boost::irange<uint64_t>(0, fsize, block_size);
            {
                auto buf = tmp_buf::aligned(in_file.memory_dma_alignment(), block_size);

                do_for_each(positions,
                        [&] (const auto pos) -> future<> {
                    const auto read_bytes = in_file.dma_read(pos, buf.get_write(), buf.size()).get0();
                    if (read_bytes % RECORD_SIZE != 0) {
                        // Something went wrong
                        // We know file size is a multiple of RECORD_SIZE and every write we do have to be multiple of that too
                        throw std::runtime_error("dma_read() read unexpected byte count");
                    }

                    const size_t count = read_bytes / sizeof(record_t);
                    record_t *records = reinterpret_cast<record_t*>(buf.get_write());
                    std::sort(records, records + count);

                    // Write sorted block to the same place in temporary file
                    const auto written_bytes = out_file.dma_write(pos, buf.get(), read_bytes).get0();
                    // read_bytes != written_bytes ???
                    return make_ready_future();
                }).get();
            }
            in_file.close();

            in_file = open_file_dma(filename, open_flags::rw).get0();
            auto buffer_size = block_size / (positions.size() + 2 /* twice the size for output buffer */);

             // make sure buffer size is a multiple of write_alignment
            buffer_size = (buffer_size / in_file.disk_write_dma_alignment()) * in_file.disk_write_dma_alignment();
            auto out_stream = make_lw_shared(make_file_output_stream(in_file, buffer_size * 2));


            // Create output streams
            auto sorted_streams = make_lw_shared(std::vector<stream_with_record>{});
            std::for_each(positions.begin(), positions.end(),
                    [&, sorted_streams] (auto pos) {
                sorted_streams->push_back(stream_with_record {
                    make_file_input_stream(out_file, pos, block_size, file_input_stream_options{buffer_size})
                });
            });

            // Fill buffers with initial records
            parallel_for_each(boost::irange<size_t>(0, sorted_streams->size()),
                    [sorted_streams] (size_t index) -> future<> {
                return sorted_streams->at(index).stream.read_exactly(RECORD_SIZE).then(
                        [index, sorted_streams] (tmp_buf buffer) {
                    auto &s = sorted_streams->at(index);
                    s.read_bytes = buffer.size();
                    s.reads ++;
                    s.current_record = std::move(buffer);
                    return make_ready_future();
                });
            }).get();

            uint64_t writes = 0;
            uint64_t written_bytes = 0;

            do_until(
                [sorted_streams] () -> bool {
                    return sorted_streams->empty();
                },
                [sorted_streams, out_stream, &writes, &written_bytes] () mutable -> future<> {
                    auto min_stream = std::min_element(sorted_streams->begin(), sorted_streams->end());
                    auto min_record = min_stream->current_record.share();
                    writes++;
                    written_bytes += min_record.size();
                    return when_all_succeed(
                        out_stream->write(min_record.get(), min_record.size()),
                        min_stream->stream.read_exactly(RECORD_SIZE)
                            .then([sorted_streams, min_stream] (tmp_buf buffer) mutable {
                                if (min_stream->stream.eof()) {
                                    sorted_streams->erase(min_stream);
                                    return make_ready_future();
                                }
                                min_stream->read_bytes += buffer.size();
                                min_stream->reads++;
                                min_stream->current_record = std::move(buffer);
                                return make_ready_future();
                            })
                    ).then([min_record = std::move(min_record)] {}); // We need to wait for possible sorted_streams change before proceeding
                }
            ).get();
            out_stream->flush().get();
            in_file.close();
        });
    });

    return 0;
}
