#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include "vector.hpp"

namespace mystl {

static inline uint64_t fnv1a_64(const char* s, size_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; ++i) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static inline uint64_t hash_str(const std::string& s) {
    return fnv1a_64(s.c_str(), s.size());
}

template <typename V>
class HashMap {
public:
    struct Bucket {
        bool used = false;
        bool tomb = false;
        uint64_t h = 0;
        std::string key;
        V value;
    };

    HashMap() : size_(0), tombs_(0) { rehash(1024); }

    size_t size() const { return size_; }

    Bucket* buckets() { return buckets_.data(); }
    const Bucket* buckets() const { return buckets_.data(); }
    size_t bucket_count() const { return buckets_.size(); }

    V* find(const std::string& key) {
        uint64_t h = hash_str(key);
        size_t m = buckets_.size();
        size_t idx = (size_t)(h % m);

        for (size_t step = 0; step < m; ++step) {
            Bucket& b = buckets_[idx];
            if (!b.used) {
                if (!b.tomb) return nullptr;
            } else if (b.h == h && b.key == key) {
                return &b.value;
            }
            idx = (idx + 1) % m;
        }
        return nullptr;
    }

    const V* find(const std::string& key) const {
        return const_cast<HashMap*>(this)->find(key);
    }

    V& get_or_insert(const std::string& key, const V& default_value) {
        maybe_grow();
        uint64_t h = hash_str(key);
        size_t m = buckets_.size();
        size_t idx = (size_t)(h % m);
        size_t first_tomb = (size_t)-1;

        for (size_t step = 0; step < m; ++step) {
            Bucket& b = buckets_[idx];
            if (!b.used) {
                if (b.tomb) {
                    if (first_tomb == (size_t)-1) first_tomb = idx;
                } else {
                    size_t put = (first_tomb != (size_t)-1) ? first_tomb : idx;
                    Bucket& nb = buckets_[put];
                    nb.used = true;
                    nb.tomb = false;
                    nb.h = h;
                    nb.key = key;
                    nb.value = default_value;
                    ++size_;
                    if (first_tomb != (size_t)-1) --tombs_;
                    return nb.value;
                }
            } else if (b.h == h && b.key == key) {
                return b.value;
            }
            idx = (idx + 1) % m;
        }
        rehash(m * 2);
        return get_or_insert(key, default_value);
    }

private:
    void maybe_grow() {
        size_t m = buckets_.size();
        double load = (double)(size_ + tombs_) / (double)m;
        if (load > 0.70) rehash(m * 2);
    }

    void rehash(size_t new_cap) {
        mystl::Vector<Bucket> old = std::move(buckets_);
        buckets_.clear();
        buckets_.reserve(new_cap);
        for (size_t i = 0; i < new_cap; ++i) buckets_.emplace_back();

        size_ = 0;
        tombs_ = 0;

        for (size_t i = 0; i < old.size(); ++i) {
            Bucket& b = old[i];
            if (!b.used) continue;
            insert_move(std::move(b));
        }
    }

    void insert_move(Bucket&& src) {
        size_t m = buckets_.size();
        size_t idx = (size_t)(src.h % m);
        for (size_t step = 0; step < m; ++step) {
            Bucket& b = buckets_[idx];
            if (!b.used && !b.tomb) {
                b.used = true;
                b.tomb = false;
                b.h = src.h;
                b.key = std::move(src.key);
                b.value = std::move(src.value);
                ++size_;
                return;
            }
            idx = (idx + 1) % m;
        }
    }

    mystl::Vector<Bucket> buckets_;
    size_t size_;
    size_t tombs_;
};

}
