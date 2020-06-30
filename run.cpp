#include <boost/asio.hpp>
#include <boost/process/io.hpp>
#include <boost/process.hpp>
#include <string>
#include <fstream>
#include <cstdlib>

using std::string;

void GetUnvisitedUrls(const string& from_that_file, const string& to_that_file) {
   std::ifstream fin(from_that_file);
   std::ofstream fout(to_that_file);

   while(fin.peek() != EOF) {
     string str;
     string count;
     getline(fin, str);
     if(str.empty()) {
       break;
     }
     size_t pos1 = str.find('\t', 0);
     size_t pos2 = str.find('\n', 0);
     count = str.substr(pos1 + 1, pos2);
     if (count == "0") {
       str = str.substr(0, pos1);
       fout << str << '\t' << "0" << '\n';
     }
   }
   fin.close();
   fout.close();
}

int main(int argc, char* argv[]) {
  string result_file_name = "result_of_work.txt";
  std::ofstream fout(result_file_name);
  string mapreduce = argv[1];
  string map = argv[2];
  string sort = argv[3];
  string reduce = argv[4];
  int depth = atoi(argv[5]);
  string input = argv[6];
  GetUnvisitedUrls(input, result_file_name);
  string map_input = "map_input_in_run.txt";
  string map_output = "map_output_in_run.txt";
  string sort_output = "sort_output_in_run.txt";
  string reduce_output = "reduce_output_in_run.txt";

  for(int i = 0; i < depth; ++i) {
    GetUnvisitedUrls(result_file_name, map_input);
    string map_call = "./" + mapreduce;
    map_call += " map ";
    map_call += map;
    map_call += " ";
    map_call += map_input;
    map_call += " ";
    map_call += map_output;
    boost::process::system(map_call);
    string sort_call = "./" + sort;
    sort_call += " ";
    sort_call += map_output;
    sort_call += " ";
    sort_call += sort_output;
    boost::process::system(sort_call);
    string reduce_call = "./" + mapreduce;
    reduce_call += " reduce ";
    reduce_call += reduce;
    reduce_call += " ";
    reduce_call += sort_output;
    reduce_call += " ";
    reduce_call += reduce_output;
    boost::process::system(reduce_call);


    std::ifstream merge_fin(reduce_output);
    std::ofstream merge_fout(result_file_name, std::ios::app);
    string str;
    while(getline(merge_fin, str)) {
      merge_fout << str << '\n';
    }

    merge_fin.close();
    merge_fout.close();

    sort_call = "./" + sort;
    sort_call += " ";
    sort_call += result_file_name;
    sort_call += " ";
    sort_call += sort_output;

    boost::process::system(sort_call);

    reduce_call = "./" + mapreduce;
    reduce_call += " reduce ";
    reduce_call += reduce;
    reduce_call += " ";
    reduce_call += sort_output;
    reduce_call += " ";
    reduce_call += result_file_name;
    boost::process::system(reduce_call);
  }
  remove(map_input.c_str());
  remove(map_output.c_str());
  remove(sort_output.c_str());
  remove(reduce_output.c_str());
return 0;
}