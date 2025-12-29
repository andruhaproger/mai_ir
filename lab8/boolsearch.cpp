#include <iostream>
#include <fstream>
#include <string>
#include <cctype>
#include <filesystem>
#include <cstdint>

#include "../mystl/vector.hpp"
#include "../mystl/hashmap.hpp"

namespace fs = std::filesystem;
using mystl::Vector;

struct TermInfo {
    uint64_t offset = 0;
    uint32_t df = 0;
};

static inline char tolower_ascii(char c) {
    if (c >= 'A' && c <= 'Z') return (char)(c - 'A' + 'a');
    return c;
}

static bool is_word_char(char c) {
    return std::isalnum((unsigned char)c) != 0;
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

static uint32_t read_varint(std::ifstream& in) {
    uint32_t v = 0;
    uint32_t shift = 0;
    for (;;) {
        uint8_t b = 0;
        in.read((char*)&b, 1);
        v |= (uint32_t)(b & 0x7F) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
    }
    return v;
}

static Vector<uint32_t> intersect_sorted(const Vector<uint32_t>& a, const Vector<uint32_t>& b) {
    Vector<uint32_t> r;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        uint32_t x = a[i], y = b[j];
        if (x == y) { r.push_back(x); ++i; ++j; }
        else if (x < y) ++i;
        else ++j;
    }
    return r;
}

static Vector<uint32_t> union_sorted(const Vector<uint32_t>& a, const Vector<uint32_t>& b) {
    Vector<uint32_t> r;
    size_t i = 0, j = 0;
    while (i < a.size() || j < b.size()) {
        if (j >= b.size() || (i < a.size() && a[i] < b[j])) r.push_back(a[i++]);
        else if (i >= a.size() || (j < b.size() && b[j] < a[i])) r.push_back(b[j++]);
        else { r.push_back(a[i]); ++i; ++j; }
    }
    return r;
}

static Vector<uint32_t> complement_sorted(const Vector<uint32_t>& a, uint32_t n_docs) {
    Vector<uint32_t> r;
    size_t j = 0;
    for (uint32_t id = 0; id < n_docs; ++id) {
        if (j < a.size() && a[j] == id) ++j;
        else r.push_back(id);
    }
    return r;
}

enum TokenType { TT_TERM, TT_AND, TT_OR, TT_NOT, TT_LP, TT_RP };

struct QToken {
    TokenType type;
    std::string text;
};

static bool is_space(char c) { return std::isspace((unsigned char)c) != 0; }

static std::string upper_word(const std::string& s) {
    std::string r;
    r.reserve(s.size());
    for (char c : s) {
        if (c >= 'a' && c <= 'z') r.push_back((char)(c - 'a' + 'A'));
        else r.push_back(c);
    }
    return r;
}

static void query_tokenize(const std::string& q, Vector<QToken>& out) {
    size_t i = 0;
    while (i < q.size()) {
        char c = q[i];
        if (is_space(c)) { ++i; continue; }
        if (c == '(') { out.push_back({TT_LP, ""}); ++i; continue; }
        if (c == ')') { out.push_back({TT_RP, ""}); ++i; continue; }

        if (is_word_char(c) || c=='-' || c=='\'') {
            std::string w;
            while (i < q.size()) {
                char cc = q[i];
                if (is_word_char(cc) || cc=='-' || cc=='\'') { w.push_back(tolower_ascii(cc)); ++i; }
                else break;
            }
            std::string up = upper_word(w);
            if (up == "AND") out.push_back({TT_AND, ""});
            else if (up == "OR") out.push_back({TT_OR, ""});
            else if (up == "NOT") out.push_back({TT_NOT, ""});
            else {
                stem_inplace(w);
                if (w.size() >= 2) out.push_back({TT_TERM, w});
            }
            continue;
        }
        ++i;
    }
}

static int prec(TokenType t) {
    if (t == TT_NOT) return 3;
    if (t == TT_AND) return 2;
    if (t == TT_OR)  return 1;
    return 0;
}

static bool is_op(TokenType t) { return t==TT_AND || t==TT_OR || t==TT_NOT; }

static void to_rpn(const Vector<QToken>& in, Vector<QToken>& out) {
    Vector<QToken> st;
    for (size_t i = 0; i < in.size(); ++i) {
        const QToken& tok = in[i];
        if (tok.type == TT_TERM) out.push_back(tok);
        else if (is_op(tok.type)) {
            while (!st.empty() && is_op(st[st.size()-1].type) &&
                   prec(st[st.size()-1].type) >= prec(tok.type)) {
                out.push_back(st[st.size()-1]);
                st.pop_back();
            }
            st.push_back(tok);
        } else if (tok.type == TT_LP) st.push_back(tok);
        else if (tok.type == TT_RP) {
            while (!st.empty() && st[st.size()-1].type != TT_LP) {
                out.push_back(st[st.size()-1]);
                st.pop_back();
            }
            if (!st.empty() && st[st.size()-1].type == TT_LP) st.pop_back();
        }
    }
    while (!st.empty()) {
        out.push_back(st[st.size()-1]);
        st.pop_back();
    }
}

static Vector<uint32_t> load_postings(std::ifstream& bin, uint64_t off, uint32_t df) {
    Vector<uint32_t> r;
    r.reserve(df);
    bin.clear();
    bin.seekg((std::streamoff)off, std::ios::beg);

    uint32_t cur = 0;
    for (uint32_t i = 0; i < df; ++i) {
        uint32_t gap = read_varint(bin);
        cur = (i == 0) ? gap : (cur + gap);
        r.push_back(cur);
    }
    return r;
}

static void usage() {
    std::cout << "Usage: boolsearch --index_dir out_bool/index --query \"A AND (B OR C)\" [--topk 10]\n";
}

int main(int argc, char** argv) {
    std::string index_dir = "out_bool/index";
    std::string query;
    int topk = 10;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--index_dir" && i + 1 < argc) index_dir = argv[++i];
        else if (a == "--query" && i + 1 < argc) query = argv[++i];
        else if (a == "--topk" && i + 1 < argc) topk = std::stoi(argv[++i]);
        else if (a == "-h" || a == "--help") { usage(); return 0; }
    }
    if (query.empty()) { usage(); return 1; }

    Vector<std::string> doc_paths;
    {
        std::ifstream in(fs::path(index_dir) / "docs.tsv", std::ios::binary);
        if (!in) { std::cerr << "Cannot open docs.tsv\n"; return 2; }
        std::string line;
        while (std::getline(in, line)) {
            size_t p1 = line.find('\t');
            size_t p2 = (p1==std::string::npos) ? std::string::npos : line.find('\t', p1+1);
            if (p2 == std::string::npos) continue;
            std::string path = line.substr(p2 + 1);
            doc_paths.push_back(path);
        }
    }
    uint32_t n_docs = (uint32_t)doc_paths.size();

    mystl::HashMap<TermInfo> dict;
    {
        std::ifstream in(fs::path(index_dir) / "dict.tsv", std::ios::binary);
        if (!in) { std::cerr << "Cannot open dict.tsv\n"; return 2; }
        std::string line;
        while (std::getline(in, line)) {
            size_t p1 = line.find('\t');
            size_t p2 = (p1==std::string::npos) ? std::string::npos : line.find('\t', p1+1);
            if (p2 == std::string::npos) continue;
            std::string term = line.substr(0, p1);
            uint64_t off = std::stoull(line.substr(p1 + 1, p2 - (p1 + 1)));
            uint32_t df = (uint32_t)std::stoul(line.substr(p2 + 1));
            TermInfo ti; ti.offset = off; ti.df = df;
            dict.get_or_insert(term, ti) = ti;
        }
    }

    std::ifstream bin(fs::path(index_dir) / "postings.bin", std::ios::binary);
    if (!bin) { std::cerr << "Cannot open postings.bin\n"; return 2; }

    Vector<QToken> qt, rpn;
    query_tokenize(query, qt);
    to_rpn(qt, rpn);

    Vector< Vector<uint32_t> > st;
    for (size_t i = 0; i < rpn.size(); ++i) {
        const QToken& t = rpn[i];
        if (t.type == TT_TERM) {
            const TermInfo* ti = dict.find(t.text);
            if (!ti) {
                Vector<uint32_t> empty;
                st.push_back(std::move(empty));
            } else {
                Vector<uint32_t> pl = load_postings(bin, ti->offset, ti->df);
                st.push_back(std::move(pl));
            }
        } else if (t.type == TT_NOT) {
            if (st.empty()) { std::cerr << "Bad query\n"; return 3; }
            Vector<uint32_t> a = std::move(st[st.size()-1]);
            st.pop_back();
            Vector<uint32_t> r = complement_sorted(a, n_docs);
            st.push_back(std::move(r));
        } else if (t.type == TT_AND || t.type == TT_OR) {
            if (st.size() < 2) { std::cerr << "Bad query\n"; return 3; }
            Vector<uint32_t> b = std::move(st[st.size()-1]); st.pop_back();
            Vector<uint32_t> a = std::move(st[st.size()-1]); st.pop_back();
            Vector<uint32_t> r = (t.type == TT_AND) ? intersect_sorted(a,b) : union_sorted(a,b);
            st.push_back(std::move(r));
        }
    }

    if (st.size() != 1) { std::cerr << "Bad query\n"; return 3; }
    Vector<uint32_t> res = std::move(st[0]);

    std::cout << "hits: " << res.size() << "\n";
    int shown = 0;
    for (size_t i = 0; i < res.size() && shown < topk; ++i) {
        uint32_t id = res[i];
        if (id < doc_paths.size()) {
            std::cout << id << "\t" << doc_paths[id] << "\n";
            ++shown;
        }
    }
    return 0;
}
