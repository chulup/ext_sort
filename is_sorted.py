#!/usr/bin/env python3

import sys
from hashlib import sha256
from zlib import crc32

def_block_size = 16

def usage():
    print("This program reads the file and does following:")
    print(" 1. Checks its blocks are in ascending order")
    print(" 2. Computes SHA256 hash of the whole file")
    print(" 3. Calculates CRC32 of every block and counts them, then outputs SHA256 of the resulting dictionary")
    print("Usage:")
    print("    {} FILENAME [block_size] [cont]".format(sys.argv[0]))
    print("  FILENAME - file to test if sorted")
    print("  block_size - size of individual data block. Default is {}".format(def_block_size))
    print("  cont - if 'cont' is present on command line the calculation will proceed even when the file is not sorted. This is necessary to calculate dictionary of all blocks' CRC32")


if len(sys.argv) < 2 or len(sys.argv) > 4:
    usage()
    exit(1)

filename = sys.argv[1]
block_size = def_block_size
cont = False
if len(sys.argv) >= 3:
    block_size = int(sys.argv[2])
    if len(sys.argv) == 4:
        cont = True

def prettify_hex(data, count):
    return " ".join(["{:X}".format(data[i]) for i in range(count)])

full_hash = sha256()
full_dict = dict()
def updates(block):
    global full_hash
    global full_dict
    full_hash.update(block)

    c32 = crc32(block)
    full_dict[c32] = full_dict.get(c32, 0) + 1

is_sorted = True

with open(filename, 'rb') as f:
    prev_block = f.read(block_size)
    block = f.read(block_size)
    updates(prev_block)
    while block:
        updates(block)
        if is_sorted and block < prev_block:
            is_sorted = False
            print("Block at {} is less than previous one!".format(f.tell() - block_size))
            print("{:7}: {} ...".format(f.tell()-2*block_size, prettify_hex(prev_block, 5)))
            print("{:7}: {} ...".format(f.tell()-block_size, prettify_hex(block, 5)))
            if not cont:
                exit()
        prev_block = block
        block = f.read(block_size)

if is_sorted:
    print("File is sorted in blocks of {} bytes".format(block_size))
else: 
    print("File is not sorted")

print("SHA256: {}".format(full_hash.hexdigest()))
keys = sorted(full_dict.keys())
dict_hash = sha256(("".join([str((k,full_dict[k])) for k in keys])).encode("utf-8"))
print("SHA256 of content dict: {}".format(dict_hash.hexdigest()))

ones = sorted([k for k, v in full_dict.items() if v == 1])
for k in ones:
    del full_dict[k]
twos = sorted([k for k, v in full_dict.items() if v == 2])
for k in twos:
    del full_dict[k]
    
print("content dict without 1 and 2, {} elements: {}".format(len(full_dict), str(full_dict))[:60])
print("{} elems with 1 occurence: {}".format(len(ones), str(ones)[:60]))
print("{} elems with 2 occurences: {}".format(len(twos), str(twos)[:60]))
