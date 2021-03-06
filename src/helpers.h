#ifndef SRC_HELPERS_H
#define SRC_HELPERS_H

#include <stdint.h>
#include <string>
#include <core/file.hh>

#define TEST_MEMORY_LIMITS 1

uint64_t get_max_buffer_size();

std::string pp_number(uint64_t number);

seastar::future<seastar::file> open_temp_file(const seastar::sstring &path);

void print_mem_stats();

#endif
