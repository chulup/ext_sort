#ifndef STRUCTURES_H
#define STRUCTURES_H

#include <cstring>
#include <core/temporary_buffer.hh>
#include <core/iostream.hh>
#include <core/file.hh>

const size_t RECORD_SIZE = 4096;
const size_t MIN_BUFFER_SIZE = 100 * 1024 * 1024; // 100M
const size_t MERGE_WAYS = 5;

/// Helper structure which data could be casted to
/// It represents individual data records to be sorted
/// It is possible to sort them with std::sort by implementing operator<() on it
struct record_t {
    char data[RECORD_SIZE];
};
bool operator<(const record_t &left, const record_t &right) {
    return std::memcmp(left.data, right.data, sizeof(record_t::data)) < 0;
}

typedef seastar::temporary_buffer<char> tmp_buf;

/// Container for input_stream that allows for choosing next minimal block
/// input_stream does not support peek() feature and that makes it necessary to
/// save next block from sorted stream for comparing with other streams
typedef struct {
    seastar::input_stream<char> stream;
    tmp_buf current_record = {};
} stream_with_record;
bool operator< (const stream_with_record &left, const stream_with_record &right) {
    const record_t *left_r = reinterpret_cast<const record_t*>(left.current_record.get());
    const record_t *right_r = reinterpret_cast<const record_t*>(right.current_record.get());

    return *left_r < *right_r;
};

/// Container for temporary
typedef struct {
    seastar::file file;
    uint64_t size;
    uint64_t orig_position;
} temp_data_t;
bool operator< (const temp_data_t &left, const temp_data_t &right) {
    return left.size < right.size;
}

#endif // STRUCTURES_H
