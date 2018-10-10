#!/usr/bin/env python3

import sys

def_block_size = 16

def usage():
    print("Usage:")
    print("    {} FILENAME [block_size]".format(sys.argv[0]))
    print("  FILENAME - file to test if sorted")
    print("  block_size - size of individual data block. Default is {}".format(def_block_size))

if len(sys.argv) < 2 or len(sys.argv) > 3:
    usage()
    exit(1)

filename = sys.argv[1]
block_size = def_block_size
if len(sys.argv) == 3:
    block_size = int(sys.argv[2])

def prettify_hex(data, count):
    return " ".join(["{:X}".format(data[i]) for i in range(count)])

with open(filename, 'rb') as f:
    prev_block = f.read(block_size)
    block = f.read(block_size)
    while block:
        if block < prev_block:
            print("Block at {} is less than previous one!".format(f.tell() - block_size))
            print("{:7}: {} ...".format(f.tell()-2*block_size, prettify_hex(prev_block, 5)))
            print("{:7}: {} ...".format(f.tell()-block_size, prettify_hex(block, 5)))
            exit()
        prev_block = block
        block = f.read(block_size)

print("File is sorted in blocks of {} bytes".format(block_size))
