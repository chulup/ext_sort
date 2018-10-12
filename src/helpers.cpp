#include "helpers.h"
#include <fmt/format.h>

uint64_t get_available_memory() {
    return 1 << 30;
}

std::string pp_number(uint64_t number) {
    static std::vector<char> prefixes = {
        'b',
        'K',
        'M',
        'G',
        'T',
        'P',
        'E',
    };
    double test = number;
    size_t order = 0;
    while (test > 1000) {
        test /= 1000;
        order ++;
    }
    return fmt::format("{:.1f} {}", test, prefixes[order]);
}
