#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <queue>
#include <iostream>

using std::vector, std::pair, std::string;

class Sorting {
 public:
    Sorting(const string& input, const string& output) :
                input_(input),
                output_(output),
                names_(vector<string>()),
                buffer_(count_) {
    }

    void Sort() {
      SplitFiles();
      MergeFiles();
      for(const auto& element : names_) {
        remove(element.c_str());
      }
      output_.close();
    }

 private:

    void SplitFiles() {
      string str;
      int file_number = 0;
      while(getline(input_, str)) {
        string file_name = std::to_string(file_number) + "sort_data.txt";
        std::ofstream fout(file_name);
        std::vector<string> file_data;
        file_data.push_back(str);
        for(int i = 0; i < size_; ++i) {
          if(getline(input_, str)) {
            file_data.push_back(str);
          }
        }
        std::sort(file_data.begin(), file_data.end());
        for(const auto& element : file_data) {
          fout << element << '\n';
        }
        names_.push_back(file_name);
        ++file_number;
      }
    }

    struct compare {
        bool operator()(const pair<uint64_t, string> &l,
                        const pair<uint64_t, string> &r) {
          return l.second > r.second;
        }
    };

    void MergeFiles() {
      std::priority_queue<pair<uint32_t, string>,
          vector<pair<uint32_t, string>>, compare> queue;
      files_.resize(names_.size());
      uint64_t t, position_in_out_buffer = 0;
      for (uint32_t i = 0; i < names_.size(); ++i) {
        files_[i] = std::ifstream(names_[i]);
        string str;
        if (getline(files_[i], str)) {
          queue.push({i, str});
        }
      }
      while (!queue.empty()) {
        buffer_[position_in_out_buffer] = string(queue.top().second);
        pair<uint32_t, string> element = queue.top();
        queue.pop();
        string str;
        if (getline(files_[element.first], str)) {
          queue.push({element.first, str});
        }
        ++position_in_out_buffer;
        if (position_in_out_buffer == count_) {
          position_in_out_buffer = 0;
          for(int i = 0; i < count_; ++i) {
            output_ << buffer_[i] << '\n';
          }
        }
      }

      if (position_in_out_buffer) {
        for(int i = 0; i < position_in_out_buffer; ++i) {
          output_ << buffer_[i] << '\n';
        }
      }

      for (auto &file: files_) {
        file.close();
      }
    }

    std::ifstream input_;
    std::ofstream output_;
    vector<string> names_;
    vector<std::ifstream> files_;
    uint64_t size_ = 20;
    uint64_t count_ = 100;
    vector<string> buffer_;
};

int main(int argc, char* argv[]) {
  Sorting s(argv[1], argv[2]);
  s.Sort();
}
