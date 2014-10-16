import numpy as np
import matplotlib.pyplot as plt
import sys
import os

colors = [ 'r', 'y', 'b', 'g', 'c', 'm' ]

tasks = { }

def check_exists(d, k):
    if not k in d:
        d[k] = [ ]

def cmp(a, b):
    if a[0] == b[0]:
        return a[1] - b[1]
    else:
        return a[0] - b[0]

def ordered_insert(l, k, v):
    i = 0
    length = len(l)

    while i < length and cmp(k, l[i][0]) > 0:
        i = i + 1
    l.insert(i, [k, v])

def append(l, v):
    l.append( [ len(l), v ] )

def constr_key(mcr, type_str):
    return mcr+':'+type_str

def str_to_tuple(s):
    s = s[1:]
    s = s[:len(s)-1]
    tokens = s.split(',')
    return ( int(tokens[0]), int(tokens[1]) )

def simple_append_case(mcr, curr_str, timestamp):
    check_exists(tasks, constr_key(mcr, curr_str))
    append(tasks[constr_key(mcr, curr_str)], timestamp)

def compare_keys(key1, key2):
    mcr1 = key1.split(':')[0]
    mcr2 = key2.split(':')[0]

    typ1 = key1.split(':')[1]
    typ2 = key2.split(':')[1]

    if typ1 < typ2:
        return -1
    elif typ1 > typ2:
        return 1
    else:
        if mcr1 < mcr2:
            return -1
        elif mcr1 > mcr2:
            return 1
        else:
            return 0

entering_run_str = 'entering run'
exiting_run_str = 'exiting run'
starting_read_str = 'starting read'
finishing_read_str = 'finishing read'
starting_write_str = 'starting write'
finishing_write_str = 'finishing write'
launching_kernel_str = 'launching kernel'
recovering_kernel_str = 'recovering completed kernel'
relaunching_kernel_str = 'relaunching kernel'
relaunch_recover_str = 'recovering relaunched kernel'
starting_block_str = 'Blocking on spillDone'
leaving_block_str = 'Unblocking on spillDone'
starting_write_block_str = 'Blocking in write'
leaving_write_block_str = 'Unblocking in write'
starting_flush_block_str = 'Blocking in flush'
leaving_flush_block_str = 'Unblocking in flush'
starting_collect_block_str = 'Blocking in collect'
leaving_collect_block_str = 'Unblocking in collect'
starting_input_alloc_str = 'start allocating input'
done_input_alloc_str = 'done allocating input'
starting_kernel_str = 'starting kernel'
returning_from_start_kernel_str = 'returning from kernel start'
start_prealloc_kernel_str = 'Preallocating kernels'
done_prealloc_kernel_str = 'Done preallocating kernels'
start_wait_for_input_str = 'Waiting for first input'
stop_wait_for_input_str = 'Done waiting for first input'
start_reading_opencl_str = 'started reading from opencl'
stop_reading_opencl_str = 'done reading from opencl'
start_cooperating_str = 'Started cooperating'
stop_cooperating_str = 'Finished cooperating'

closing_pair = { entering_run_str: exiting_run_str,
                 starting_read_str: finishing_read_str,
                 starting_write_str: finishing_write_str,
                 launching_kernel_str: recovering_kernel_str,
                 relaunching_kernel_str: relaunch_recover_str,
                 starting_block_str: leaving_block_str,
                 starting_write_block_str: leaving_write_block_str,
                 starting_flush_block_str: leaving_flush_block_str,
                 starting_collect_block_str: leaving_collect_block_str,
                 starting_input_alloc_str: done_input_alloc_str,
                 starting_kernel_str: returning_from_start_kernel_str,
                 start_prealloc_kernel_str: done_prealloc_kernel_str,
                 start_wait_for_input_str: stop_wait_for_input_str,
                 start_reading_opencl_str: stop_reading_opencl_str,
                 start_cooperating_str: stop_cooperating_str }

type_labels = { entering_run_str: 'overall',
                starting_read_str: 'reading',
                starting_write_str: 'writing',
                launching_kernel_str: 'kernel',
                relaunching_kernel_str: 'relaunch',
                starting_block_str: 'blocking',
                starting_write_block_str: 'write_blocking',
                starting_flush_block_str: 'flush_blocking',
                starting_collect_block_str: 'collect_blocking',
                starting_input_alloc_str: 'input_alloc',
                starting_kernel_str: 'starting kernel',
                start_prealloc_kernel_str: 'kernel_prealloc',
                start_wait_for_input_str: 'initial_input_wait',
                start_reading_opencl_str: 'reading_from_opencl',
                start_cooperating_str: 'cooperating' }

max_timestamp = 0
min_timestamp = -1
if len(sys.argv) < 2:
    print 'usage: python plot.py filename [filter]'
    sys.exit()

fp = open(sys.argv[1], 'r')

filt = None
if len(sys.argv) > 2:
    filt = [ ]
    for t in sys.argv[2:]:
        filt.append(t)

for line in fp:
    line = line[:len(line)-1]
    tokens = line.split()

    if tokens[0] != 'TIMING':
        continue

    if tokens[2] == 'OpenCL':
        continue

    timestamp = int(tokens[4])
    max_timestamp = max(max_timestamp, timestamp)
    if min_timestamp == -1:
        min_timestamp = timestamp
    else:
        min_timestamp = min(min_timestamp, timestamp)
    mcr = tokens[2]
    msg = ' '.join(tokens[6:])
    tokens = tokens[6:]

    if msg.find(entering_run_str) == 0:
        check_exists(tasks, constr_key(mcr, entering_run_str))
        append(tasks[constr_key(mcr, entering_run_str)], timestamp)
    elif msg.find(exiting_run_str) == 0:
        check_exists(tasks, constr_key(mcr, exiting_run_str))
        append(tasks[constr_key(mcr, exiting_run_str)], timestamp)
    elif msg.find(starting_read_str) == 0:
        check_exists(tasks, constr_key(mcr, starting_read_str))
        ordered_insert(tasks[constr_key(mcr, starting_read_str)],
                str_to_tuple(tokens[3]), timestamp)
    elif msg.find(finishing_read_str) == 0:
        check_exists(tasks, constr_key(mcr, finishing_read_str))
        ordered_insert(tasks[constr_key(mcr, finishing_read_str)],
                str_to_tuple(tokens[3]), timestamp)
    elif msg.find(starting_write_str) == 0:
        check_exists(tasks, constr_key(mcr, starting_write_str))
        ordered_insert(tasks[constr_key(mcr, starting_write_str)],
                str_to_tuple(tokens[3]), timestamp)
    elif msg.find(finishing_write_str) == 0:
        check_exists(tasks, constr_key(mcr, finishing_write_str))
        ordered_insert(tasks[constr_key(mcr, finishing_write_str)],
                str_to_tuple(tokens[3]), timestamp)
    elif msg.find(launching_kernel_str) == 0:
        check_exists(tasks, constr_key(mcr, launching_kernel_str))
        ordered_insert(tasks[constr_key(mcr, launching_kernel_str)],
                str_to_tuple(tokens[2]), timestamp)
    elif msg.find(recovering_kernel_str) == 0:
        check_exists(tasks, constr_key(mcr, recovering_kernel_str))
        ordered_insert(tasks[constr_key(mcr, recovering_kernel_str)],
                str_to_tuple(tokens[3]), timestamp)
    elif msg.find(relaunching_kernel_str) == 0:
        check_exists(tasks, constr_key(mcr, relaunching_kernel_str))
        ordered_insert(tasks[constr_key(mcr, relaunching_kernel_str)],
                str_to_tuple(tokens[2]), timestamp)
    elif msg.find(relaunch_recover_str) == 0:
        check_exists(tasks, constr_key(mcr, relaunch_recover_str))
        ordered_insert(tasks[constr_key(mcr, relaunch_recover_str)],
                str_to_tuple(tokens[3]), timestamp)
    elif msg.find(starting_block_str) == 0:
        simple_append_case(mcr, starting_block_str, timestamp)
    elif msg.find(leaving_block_str) == 0:
        simple_append_case(mcr, leaving_block_str, timestamp)
    elif msg.find(starting_write_block_str) == 0:
        simple_append_case(mcr, starting_write_block_str, timestamp)
    elif msg.find(leaving_write_block_str) == 0:
        simple_append_case(mcr, leaving_write_block_str, timestamp)
    elif msg.find(starting_flush_block_str) == 0:
        simple_append_case(mcr, starting_flush_block_str, timestamp)
    elif msg.find(leaving_flush_block_str) == 0:
        simple_append_case(mcr, leaving_flush_block_str, timestamp)
    elif msg.find(starting_collect_block_str) == 0:
        simple_append_case(mcr, starting_collect_block_str, timestamp)
    elif msg.find(leaving_collect_block_str) == 0:
        simple_append_case(mcr, leaving_collect_block_str, timestamp)
    elif msg.find(starting_input_alloc_str) == 0:
        simple_append_case(mcr, starting_input_alloc_str, timestamp)
    elif msg.find(done_input_alloc_str) == 0:
        simple_append_case(mcr, done_input_alloc_str, timestamp)
    elif msg.find(starting_kernel_str) == 0:
        simple_append_case(mcr, starting_kernel_str, timestamp)
    elif msg.find(returning_from_start_kernel_str) == 0:
        simple_append_case(mcr, returning_from_start_kernel_str, timestamp)
    elif msg.find(start_prealloc_kernel_str) == 0:
        simple_append_case(mcr, start_prealloc_kernel_str, timestamp)
    elif msg.find(done_prealloc_kernel_str) == 0:
        simple_append_case(mcr, done_prealloc_kernel_str, timestamp)
    elif msg.find(start_wait_for_input_str) == 0:
        simple_append_case(mcr, start_wait_for_input_str, timestamp)
    elif msg.find(stop_wait_for_input_str) == 0:
        simple_append_case(mcr, stop_wait_for_input_str, timestamp)
    elif msg.find(start_reading_opencl_str) == 0:
        simple_append_case(mcr, start_reading_opencl_str, timestamp)
    elif msg.find(stop_reading_opencl_str) == 0:
        simple_append_case(mcr, stop_reading_opencl_str, timestamp)
    elif msg.find(start_cooperating_str) == 0:
        simple_append_case(mcr, start_cooperating_str, timestamp)
    elif msg.find(stop_cooperating_str) == 0:
        simple_append_case(mcr, stop_cooperating_str, timestamp)

for k in tasks.keys():
    l = tasks[k]
    for i in l:
        i[1] = i[1] - min_timestamp

N = len(tasks)
labels = [ ]
fig = plt.figure(num=0, figsize=(18, 6), dpi=80)

width = 0.35       # the width of the bars: can also be len(x) sequence
color_counter = 0
ind = 0

sorted_keys = sorted(tasks.keys(), cmp=compare_keys)

for starter in sorted_keys:
    mcr = starter.split(':')[0]
    typ = starter.split(':')[1]
    if not filt is None:
        matchesAllFilter = True
        for f in filt:
            if not f in starter:
                matchesAllFilter = False
                break
        if not matchesAllFilter:
            continue

    if typ in closing_pair.keys():
        labels.append(mcr+':'+type_labels[typ])
        closer = mcr+':'+closing_pair[typ]
        starter_list = tasks[starter]
        closer_list = tasks[closer]

        total = 0
        length = len(starter_list)
        i = 0
        print mcr+' '+typ
        while i < length:
            plt.barh(ind, closer_list[i][1]-starter_list[i][1], height=width,
                    left=starter_list[i][1],
                    linewidth=1, color=colors[color_counter])
            total = total + (closer_list[i][1] - starter_list[i][1])
            i = i + 1
        ind = ind + width
        color_counter = (color_counter + 1) % len(colors)
        print starter+' '+str(length)+' '+str(total)

plt.ylabel('Tasks')
plt.xlabel('Time')
plt.yticks(np.arange(0, N, width)+width/2., labels )
# plt.xticks(np.arange(min_timestamp, max_timestamp, 10000))
# plt.axis([ min_timestamp, max_timestamp, 0, 5 ])
plt.axis([ 0, max_timestamp-min_timestamp, 0, ind ])
# plt.legend( (p1[0], p2[0]), ('Men', 'Women') )
plt.show()
