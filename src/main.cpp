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

#pragma pack(push, 1)
struct record_t {
    uint8_t data[RECORD_SIZE];
};
#pragma pack(pop)

bool operator<(const record_t &left, const record_t &right) {
    return std::memcmp(left.data, right.data, sizeof(record_t::data)) < 0;
}

std::pair<size_t, uint64_t> calculate_smp_and_block_size(uint64_t /*filesize*/) {

uint64_t get_available_memory(uint64_t /*filesize*/) {
    const uint64_t MEM_AVAILABLE = 1 << 18; // 10M

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
            file in_file = open_file_dma(filename, open_flags::rw).get0();
            uint64_t fsize = in_file.size().get0();
            if (fsize % RECORD_SIZE != 0) {
                throw std::runtime_error("File size is not a multiple of RECORD_SIZE");
            }
            const sstring out_name = sprint("%s.sort_tmp", filename);
            file out_file = open_file_dma(out_name, open_flags::rw | open_flags::create).get0();

            uint64_t mem_available = get_available_memory(fsize);
            const auto positions = boost::irange<uint64_t>(0, fsize, mem_available);
            {
                auto buf = tmp_buf::aligned(in_file.memory_dma_alignment(), mem_available);

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
                });
            }

            });


        });
    });

    return 0;
}
