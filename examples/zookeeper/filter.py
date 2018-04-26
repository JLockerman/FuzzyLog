#!/usr/local/bin/python2.7

array = [
    3081186,
    2321230,
    2061901,
    1638528,
    737546,
    1106974,
    297181,
    609149,
    216574,
    345637,
    4069959,
    1267186,
    2578993,
    1175917,
    737942,
    1070143,
    266270,
    734787,
    282598,
    386876,
    434618,
    756617,
    360364,
    658804,
    283607,
    528643,
    214238,
    356830,
    119026,
    98111,
    524433,
    547037,
    326763,
    559806,
    266205,
    542412,
    179787,
    385465,
    89563,
    191446
]

reads = []
writes = []

for i in range(0, len(array), 2):
    reads.append(array[i])
    writes.append(array[i+1])

read_str = ''
write_str = ''
for row in range(0, 5):
    for col in range(0, 4):
        read_str += '{:>7}'.format(reads[row + 5 * col]) + '  '
        write_str += '{:>7}'.format(writes[row + 5 * col]) + '  '

    read_str += '\n'
    write_str += '\n'

print('reads')
print(str(read_str))
print('writes')
print(str(write_str))
