#pragma once
#include <cstddef>
#include <utility>
#include <new>

namespace mystl {

template <typename T>
class Vector {
public:
    Vector() : data_(nullptr), size_(0), cap_(0) {}

    Vector(const Vector& other) : data_(nullptr), size_(0), cap_(0) {
        reserve(other.size_);
        for (size_t i = 0; i < other.size_; ++i) push_back(other[i]);
    }

    Vector& operator=(const Vector& other) {
        if (this == &other) return *this;
        clear();
        reserve(other.size_);
        for (size_t i = 0; i < other.size_; ++i) push_back(other[i]);
        return *this;
    }

    Vector(Vector&& other) noexcept
        : data_(other.data_), size_(other.size_), cap_(other.cap_) {
        other.data_ = nullptr;
        other.size_ = 0;
        other.cap_ = 0;
    }

    Vector& operator=(Vector&& other) noexcept {
        if (this == &other) return *this;
        destroy_storage();
        data_ = other.data_;
        size_ = other.size_;
        cap_ = other.cap_;
        other.data_ = nullptr;
        other.size_ = 0;
        other.cap_ = 0;
        return *this;
    }

    ~Vector() { destroy_storage(); }

    void reserve(size_t new_cap) {
        if (new_cap <= cap_) return;
        T* new_data = static_cast<T*>(::operator new(sizeof(T) * new_cap));
        for (size_t i = 0; i < size_; ++i) {
            new (&new_data[i]) T(std::move(data_[i]));
            data_[i].~T();
        }
        ::operator delete(data_);
        data_ = new_data;
        cap_ = new_cap;
    }

    void push_back(const T& v) {
        if (size_ == cap_) reserve(cap_ ? cap_ * 2 : 8);
        new (&data_[size_]) T(v);
        ++size_;
    }

    void push_back(T&& v) {
        if (size_ == cap_) reserve(cap_ ? cap_ * 2 : 8);
        new (&data_[size_]) T(std::move(v));
        ++size_;
    }

    template <class... Args>
    T& emplace_back(Args&&... args) {
        if (size_ == cap_) reserve(cap_ ? cap_ * 2 : 8);
        new (&data_[size_]) T(std::forward<Args>(args)...);
        ++size_;
        return data_[size_ - 1];
    }

    void pop_back() {
        if (!size_) return;
        data_[size_ - 1].~T();
        --size_;
    }

    void clear() {
        for (size_t i = 0; i < size_; ++i) data_[i].~T();
        size_ = 0;
    }

    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    T& operator[](size_t i) { return data_[i]; }
    const T& operator[](size_t i) const { return data_[i]; }

    T* data() { return data_; }
    const T* data() const { return data_; }

private:
    void destroy_storage() {
        clear();
        ::operator delete(data_);
        data_ = nullptr;
        cap_ = 0;
    }

    T* data_;
    size_t size_;
    size_t cap_;
};

}
