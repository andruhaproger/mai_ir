#include <iostream>
#include <fstream>
#include <string>
#include <cctype>
#include <filesystem>
#include <chrono>
#include <cstdint>

#include "../mystl/vector.hpp"
#include "../mystl/hashmap.hpp"

namespace fs = std::filesystem;
using mystl::Vector;

struct PostingList {
    Vector<uint32_t> docs;
};

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

        if (ok || hyphen || apost) cur.push_back(lc);
        else flush();
    }
    flush();
}

static bool ends_with(const std::string& s, const char* suf) {
    size_t n = s.size();
    size_t m = 0;
    while (suf[m]) ++m;
    if (m > n) return false;
    for (size_t i = 0; i < m; ++i) if (s[n - m + i] != suf[i]) return false;
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
    if (ends_with(w, "ment") && w.size() > 8) { w.resize(w.size() - 4); return; }
}

static void write_varint(std::ofstream& out, uint32_t v) {
    while (v >= 0x80) {
        uint8_t b = (uint8_t)((v & 0x7F) | 0x80);
        out.write((char*)&b, 1);
        v >>= 7;
    }
    uint8_t b = (uint8_t)(v & 0x7F);
    out.write((char*)&b, 1);
}

static void quicksort_indices(Vector<size_t>& idx, size_t l, size_t r, const mystl::HashMap<PostingList>& map) {
    if (l >= r) return;
    size_t i = l, j = r;
    const std::string& pivot = map.buckets()[ idx[(l + r) / 2] ].key;

    while (i <= j) {
        while (map.buckets()[ idx[i] ].key < pivot) ++i;
        while (map.buckets()[ idx[j] ].key > pivot) { if (j==0) break; --j; }
        if (i <= j) {
            size_t tmp = idx[i]; idx[i] = idx[j]; idx[j] = tmp;
            ++i;
            if (j==0) break;
            --j;
        }
    }
    if (j > l) quicksort_indices(idx, l, j, map);
    if (i < r) quicksort_indices(idx, i, r, map);
}

static void usage() {
    std::cout << "Usage: boolindex --input_dir data_text --out_dir out_bool\n";
}

int main(int argc, char** argv) {
    std::string input_dir = "data_text";
    std::string out_dir = "out_bool";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--input_dir" && i + 1 < argc) input_dir = argv[++i];
        else if (a == "--out_dir" && i + 1 < argc) out_dir = argv[++i];
        else if (a == "-h" || a == "--help") { usage(); return 0; }
    }

    fs::path out_index = fs::path(out_dir) / "index";
    fs::create_directories(out_index);

    Vector<fs::path> doc_paths;
    Vector<std::string> doc_sources;

    for (auto src : {"wikipedia_en", "marinelink"}) {
        fs::path p = fs::path(input_dir) / src;
        if (!fs::exists(p)) continue;
        for (auto& e : fs::directory_iterator(p)) {
            if (e.is_regular_file() && e.path().extension() == ".txt") {
                doc_paths.push_back(e.path());
                doc_sources.push_back(std::string(src));
            }
        }
    }

    {
        std::ofstream docs(fs::path(out_index) / "docs.tsv", std::ios::binary);
        for (size_t i = 0; i < doc_paths.size(); ++i) {
            docs << i << "\t" << doc_sources[i] << "\t" << doc_paths[i].string() << "\n";
        }
    }

    mystl::HashMap<PostingList> inv;

    auto t0 = std::chrono::high_resolution_clock::now();

    Vector<std::string> toks;
    toks.reserve(4096);

    for (size_t di = 0; di < doc_paths.size(); ++di) {
        std::ifstream in(doc_paths[di], std::ios::binary);
        if (!in) continue;

        std::string line;
        while (std::getline(in, line)) {
            toks.clear();
            tokenize_line(line, toks);
            for (size_t j = 0; j < toks.size(); ++j) {
                stem_inplace(toks[j]);
                if (toks[j].size() < 2) continue;

                PostingList* pl = inv.find(toks[j]);
                if (!pl) {
                    PostingList empty;
                    PostingList& ref = inv.get_or_insert(toks[j], empty);
                    ref.docs.push_back((uint32_t)di);
                } else {
                    if (pl->docs.empty() || pl->docs[pl->docs.size() - 1] != (uint32_t)di) {
                        pl->docs.push_back((uint32_t)di);
                    }
                }
            }
        }
    }

    Vector<size_t> idx;
    idx.reserve(inv.bucket_count());
    for (size_t i = 0; i < inv.bucket_count(); ++i) {
        const auto& b = inv.buckets()[i];
        if (b.used) idx.push_back(i);
    }
    if (!idx.empty()) quicksort_indices(idx, 0, idx.size() - 1, inv);

    std::ofstream postings(fs::path(out_index) / "postings.bin", std::ios::binary);
    std::ofstream dict(fs::path(out_index) / "dict.tsv", std::ios::binary);

    uint64_t offset = 0;
    for (size_t k = 0; k < idx.size(); ++k) {
        const auto& b = inv.buckets()[ idx[k] ];
        const std::string& term = b.key;
        const PostingList& pl = b.value;

        dict << term << "\t" << offset << "\t" << pl.docs.size() << "\n";

        uint32_t prev = 0;
        for (size_t j = 0; j < pl.docs.size(); ++j) {
            uint32_t v = pl.docs[j];
            uint32_t gap = (j == 0) ? v : (v - prev);
            write_varint(postings, gap);
            prev = v;
        }
        offset = (uint64_t)postings.tellp();
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();

    std::cout << "docs: " << doc_paths.size() << "\n";
    std::cout << "terms: " << idx.size() << "\n";
    std::cout << "index_dir: " << out_index.string() << "\n";
    std::cout << "time_s: " << sec << "\n";
    std::cout << "files: docs.tsv dict.tsv postings.bin\n";
    return 0;
}
