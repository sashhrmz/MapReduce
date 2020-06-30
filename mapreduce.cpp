#include <boost/asio.hpp>
#include <boost/process/io.hpp>
#include <boost/process.hpp>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using std::string, std::vector;

int main(int argc, char* argv[]) {
  string operation_name;
  string script_path;
  string input_file;
  string output_file;
  operation_name = argv[1];
  script_path = argv[2];
  input_file = argv[3];
  output_file = argv[4];
  vector<string> input_file_names;
  vector<string> output_file_names;
  vector<boost::process::child> children;

  if(operation_name == "map") {
    int url_count = 100;
    std::ifstream fin(input_file);
    for(int i = 0; i < url_count; ++i) {
      int position = 0;
      string file_name = std::to_string(i) + "input_map_data.txt";
      input_file_names.push_back(file_name);
      std::ofstream map_fin(file_name);
      for(int j = 0; j < url_count; ++j, ++position) {
        string str;
        getline(fin, str);
        if(str.empty()) {
          break;
        }
        map_fin << str << '\n';
      }
      map_fin.close();
    }
    fin.close();

    int file_num = 0;
    for(const auto& file_name : input_file_names) {
      string output_map_file = std::to_string(file_num) + "output_map_data.txt";
      output_file_names.push_back(output_map_file);
      children.emplace_back(boost::process::child("./" + script_path,
                             boost::process::std_in < file_name,
                             boost::process::std_out > output_map_file));
      ++file_num;
    }

    for (auto& process : children) {
      std::error_code error_code;
      process.wait(error_code);
      if (error_code.value() != 0) {
        throw std::system_error(error_code);
      }
    }

    for(const auto& file_name: input_file_names) {
      remove(file_name.c_str());
    }

    std::ofstream fout(output_file);
    for(const auto& file_name : output_file_names) {
      string str;
      std::ifstream map_fin(file_name);
      while (map_fin.peek() != EOF) {
        getline(map_fin, str);
        fout << str << '\n';
      }
      map_fin.close();
    }

    fout.close();

    for(const auto& file_name: output_file_names) {
      remove(file_name.c_str());
    }
  } else if(operation_name == "reduce") {
    std::ifstream fin(input_file);
    string current_key;
    string previous_key;
    string reduce_input_file(std::to_string(0) + "input_reduce_data.txt");
    input_file_names.push_back(reduce_input_file);
    std::ofstream reduce_fin(reduce_input_file);
    int file_num = 1;
    getline(fin, previous_key);
    while (fin.peek() != EOF) {
      reduce_fin << previous_key << '\n';
      getline(fin, current_key);

      size_t pos1 = current_key.find('\t', 0);
      size_t pos2 = previous_key.find('\t', 0);
      if (current_key.substr(0, pos1) != previous_key.substr(0, pos2)) {
        std::string key;
        reduce_fin.close();
        reduce_input_file = (std::to_string(file_num) + "input_reduce_data.txt");
        input_file_names.push_back(reduce_input_file);
        ++file_num;
        reduce_fin = std::ofstream(reduce_input_file);
      }
      previous_key = current_key;
    }
    reduce_fin << previous_key << '\n';
    reduce_fin.close();

    fin.close();

    file_num = 0;
    for(const auto& file_name : input_file_names) {
      string output_reduce_file = std::to_string(file_num) + "output_reduce_data.txt";
      output_file_names.push_back(output_reduce_file);
      children.emplace_back(boost::process::child("./" + script_path,
                             boost::process::std_in < file_name,
                             boost::process::std_out > output_reduce_file));
      ++file_num;
    }

    for (auto& process : children) {
      std::error_code error_code;
      process.wait(error_code);
      if (error_code.value() != 0) {
        throw std::system_error(error_code);
      }
    }

    for(const auto& file_name: input_file_names) {
      remove(file_name.c_str());
    }

    std::ofstream fout(output_file);
    for(const auto& file_name : output_file_names) {
      string str;
      std::ifstream reduce_fin(file_name);
      while (reduce_fin.peek() != EOF) {
        getline(reduce_fin, str);
        if(!str.empty()) {
          fout << str << '\n';
        }
      }
      reduce_fin.close();
    }
    fout.close();

    for(const auto& file_name: output_file_names) {
      remove(file_name.c_str());
    }
  }
  return 0;
}

