#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <vector>

using namespace std;

int main(int argc, char** argv) {
  string trace_file_name = argv[1];
  string source_file_name = argv[2];
  // Open source_file and return fd
  int fd = open(source_file_name.c_str(), O_RDONLY);
  vector<off_t> pos_list;
  vector<size_t> size_list;
  // Open the file with name as file_name
  ifstream file(trace_file_name);
  // Read two integers from file one by one
  off_t pos;
  size_t size;
  while (file >> pos >> size) {
    pos_list.push_back(pos);
    size_list.push_back(size);
  }

  char* buf = new char[35119920 + 10];
  auto start = chrono::steady_clock::now();
  for (int i = 0; i < pos_list.size(); i++) {
    // Read size_list[i] bytes from fd at pos_list[i]
    pread(fd, buf, size_list[i], pos_list[i]);
  }
  auto end = chrono::steady_clock::now();
  delete[] buf;
  auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
  cout << duration.count() << endl;
  return 0;
}