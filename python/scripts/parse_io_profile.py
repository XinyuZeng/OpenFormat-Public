import sys

column_names_cpu = ['%user', '%nice', '%system', '%iowait', '%steal', '%idle']
column_names_device = ['tps', 'kB_read/s', 'kB_wrtn/s',
                       'kB_dscd/s', 'kB_read', 'kB_wrtn', 'kB_dscd']


def parse_io_cpu_line(result_cpu, line):
    columns = line.split()
    for index, column_name in enumerate(column_names_cpu):
        result_cpu[column_name].append(float(columns[index]))


def parse_io_device_line(result_device, line):
    columns = line.split()
    for index, column_name in enumerate(column_names_device):
        result_device[column_name].append(float(columns[index + 1]))


def parse_io_file(file_name):
    result_cpu = {}
    for column_name_cpu in column_names_cpu:
        result_cpu[column_name_cpu] = []
    result_device = {}
    for column_name_device in column_names_device:
        result_device[column_name_device] = []
    with open(file_name, 'rt') as file:
        lines = file.readlines()
    index = 2
    while index != len(lines):
        parse_io_cpu_line(result_cpu, lines[index + 1])
        parse_io_device_line(result_device, lines[index + 5])
        index += 8
    time_array = []
    # print(result_device)
    # for index in range(len(result_device[column_names_device[0]])):
    #     if result_device['kB_read/s'][index]:
    #         time_array.append(
    #             result_device['kB_read'][index] / result_device['kB_read/s'][index])
    #     elif result_device['kB_wrtn/s'][index]:
    #         time_array.append(
    #             result_device['kB_wrtn'][index] / result_device['kB_wrtn/s'][index])
    #     elif result_device['kB_dscd/s'][index]:
    #         time_array.append(
    #             result_device['kB_dscd'][index] / result_device['kB_dscd/s'][index])
    #     elif result_device['tps'][index]:
    #         assert True
    #     else:
    #         print('ERROR: unable to recognize the time of record %d' % (index + 1))
    #         sys.exit(1)
    return result_cpu, result_device, time_array


if __name__ == '__main__':
    profile_name = sys.argv[1]
    file_name = '%s.log' % profile_name
    result_cpu, result_device, time_array = parse_io_file(file_name)
    print('avg-cpu:\n', result_cpu)
    print('Device:\n', result_device)
    print('Time:\n', time_array)
