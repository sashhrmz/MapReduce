#include<iostream>
#include<map>
#include<string>

int main() {
  std::map<std::string, std::string> result;
  std::string key;
  std::string value;
  while (getline(std::cin, key)) {
    if (key.empty()) {
      break;
    }
    size_t pos1 = key.find('\t', 0);
    size_t pos2 = key.length();
    value = key.substr(pos1 + 1, pos2);
    key = key.substr(0, pos1);
    if (!key.empty() && (result.find(key) == result.end() || value == "1")) {
      result[key] = value;
    }
  }

  for (const auto& element : result) {
    std::cout << element.first << '\t' << element.second << '\n';
  }
  return 0;
}
