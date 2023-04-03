import threading
import sys
import os


class DownloadThread(threading.Thread):
    def __init__(self, table_name, table_id):
        threading.Thread.__init__(self)
        self.table_name = table_name
        self.file_name = '%s_%d.csv' % (table_name, table_id)

    def run(self):
        os.system('curl https://homepages.cwi.nl/~boncz/PublicBIbenchmark/{0}/{1}.bz2 --output {1}.bz2 '.format
                  (self.table_name, self.file_name))
        os.system('pbzip2 -d ./%s.bz2' % self.file_name)
        os.system('mv %s tables' % self.file_name)
        print('Finish extraction of %s' % self.file_name)


def main():
    if len(sys.argv) < 3:
        print('Invalid number of arguments. A sample usage will be: \'python3 scripts/download.py Generico 3\'')
        sys.exit(1)
    table_name = sys.argv[1]
    number_of_table = int(sys.argv[2])
    threads = []
    for index in range(number_of_table):
        threads.append(DownloadThread(table_name, index + 1))
        threads[index].start()
    for index in range(number_of_table):
        threads[index].join()


if __name__ == '__main__':
    main()
