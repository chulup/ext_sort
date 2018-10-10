#ifndef HELPERS_H
#define HELPERS_H

#include <core/app-template.hh>

struct sort_options {
    uint64_t block_size;
    unsigned thread_count;
    seastar::sstring temp_file_name;
};

sort_options get_sort_options(const boost::program_options::variables_map &options);

#endif // HELPERS_H
