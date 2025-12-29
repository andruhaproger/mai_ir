#include <iostream>
#include <fstream>
#include <string>
#include <cctype>
#include <chrono>
#include <filesystem>
#include "../mystl/vector.hpp"

namespace fs = std::filesystem;
using mystl::Vector;

static inline char tolower_ascii(char c) {
    if (c >= 'A' && c <= 'Z') return (char)(c - 'A' + 'a');
    return c;
}

static bool is_word_char(char c) {
    return std::isalnum((unsigned char)c) != 0;
}

static void tokenize_line(const std::string& line, Vector<std::string>& out_tokens) {
    std::string cur;
    cur.reserve(32);

    auto flush = [&]() {
        if (cur.size() >= 2) out_tokens.push_back(cur);
        cur.clear();
    };

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        char lc = tolower_ascii(c);

        bool ok = is_word_char(lc);
        bool hyphen = (lc == '-' && !cur.empty());
        bool apost = (lc == '\'' && !cur.empty());

        if (ok || hyphen || apost) {
            cur.push_back(lc);
        } else {
            flush();
        }
    }
    flush();
}

static bool ends_with(const std::string& s, const char* suf) {
    size_t n = s.size();
    size_t m = 0;
    while (suf[m]) ++m;
    if (m > n) return false;
    for (size_t i = 0; i < m; ++i) {
        if (s[n - m + i] != suf[i]) return false;
    }
    return true;
}

static void stem_inplace(std::string& w) {
    if (w.size() < 4) return;

    if (ends_with(w, "'s") && w.size() > 3) w.resize(w.size() - 2);

    if (ends_with(w, "sses") && w.size() > 6) { w.resize(w.size() - 2); return; }
    if (ends_with(w, "ies")  && w.size() > 5) { w.resize(w.size() - 3); w.push_back('y'); return; }
    if (ends_with(w, "s")    && w.size() > 4 && !ends_with(w, "ss")) { w.resize(w.size() - 1); }

    if (ends_with(w, "ing") && w.size() > 6) { w.resize(w.size() - 3); return; }
    if (ends_with(w, "ed")  && w.size() > 5) { w.resize(w.size() - 2); return; }

    if (ends_with(w, "ly")  && w.size() > 6) { w.resize(w.size() - 2); return; }
    if (ends_with(w, "tion") && w.size() > 7) { w.resize(w.size() - 3); return; }
    if (ends_with(w, "ment") && w.size() > 8) { w.resize(w.size() - 4); return; }
}

static void usage() {
    std::cout << "Usage: stemming --input_dir data_text\n";
}

int main(int argc, char** argv) {
    std::string input_dir = "data_text";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--input_dir" && i + 1 < argc) input_dir = argv[++i];
        else if (a == "-h" || a == "--help") { usage(); return 0; }
    }

    Vector<fs::path> files;
    for (auto src : {"wikipedia_en", "marinelink"}) {
        fs::path p = fs::path(input_dir) / src;
        if (!fs::exists(p)) continue;
        for (auto& e : fs::directory_iterator(p)) {
            if (e.is_regular_file() && e.path().extension() == ".txt") {
                files.push_back(e.path());
            }
        }
    }

    uint64_t total_tokens = 0;
    uint64_t total_token_chars = 0;
    uint64_t total_bytes = 0;

    auto t0 = std::chrono::high_resolution_clock::now();

    Vector<std::string> toks;
    toks.reserve(4096);

    for (size_t fi = 0; fi < files.size(); ++fi) {
        std::ifstream in(files[fi], std::ios::binary);
        if (!in) continue;

        in.seekg(0, std::ios::end);
        total_bytes += (uint64_t)in.tellg();
        in.seekg(0, std::ios::beg);

        std::string line;
        while (std::getline(in, line)) {
            toks.clear();
            tokenize_line(line, toks);
            for (size_t j = 0; j < toks.size(); ++j) {
                stem_inplace(toks[j]);
                total_tokens += 1;
                total_token_chars += toks[j].size();
            }
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    double kb = (double)total_bytes / 1024.0;
    double speed = (sec > 0.0) ? (kb / sec) : 0.0;
    double avg_len = (total_tokens > 0) ? (double)total_token_chars / (double)total_tokens : 0.0;

    std::cout << "files: " << files.size() << "\n";
    std::cout << "total_tokens: " << total_tokens << "\n";
    std::cout << "avg_token_len: " << avg_len << "\n";
    std::cout << "input_kb: " << kb << "\n";
    std::cout << "time_s: " << sec << "\n";
    std::cout << "speed_kb_s: " << speed << "\n";
    return 0;
}
