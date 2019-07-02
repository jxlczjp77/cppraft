#pragma once
#include <raft/raft.pb.h>
#include <boost/circular_buffer.hpp>
#include <vector>
#include <array>

namespace raft {
	using namespace raftpb;
	using namespace std;

	typedef boost::circular_buffer_space_optimized<Entry> EntryUnstableVec;
	typedef std::vector<Entry> EntryVec;

	class IEntrySlice {
	public:
		class iterator {
		public:
			IEntrySlice &m_c;
			size_t m_pos;

			typedef iterator self_type;
			typedef Entry value_type;
			typedef Entry& reference;
			typedef const Entry& const_reference;
			typedef Entry* pointer;
			typedef const Entry* const_pointer;
			typedef std::forward_iterator_tag iterator_category;
			typedef int difference_type;
			iterator(IEntrySlice &c, size_t pos = 0) : m_c(c), m_pos(pos) {}
			self_type operator++(int junk) { self_type i = *this; m_pos++; return i; }
			self_type &operator++() { m_pos++; return *this; }
			self_type operator+(size_t n) const { return self_type(m_c, m_pos + n); }
			pointer operator->() { return &m_c[m_pos]; }
			reference operator*() { return m_c[m_pos]; }
			bool operator==(const self_type& rhs) { return &m_c == &rhs.m_c && m_pos == rhs.m_pos; }
			bool operator!=(const self_type& rhs) { return !(*this == rhs); }
		};
		class const_iterator {
		public:
			const IEntrySlice &m_c;
			size_t m_pos;

			typedef const_iterator self_type;
			typedef Entry value_type;
			typedef Entry& reference;
			typedef const Entry& const_reference;
			typedef Entry* pointer;
			typedef const Entry* const_pointer;
			typedef std::forward_iterator_tag iterator_category;
			typedef int difference_type;
			const_iterator(const IEntrySlice &c, size_t pos = 0) : m_c(c), m_pos(pos) {}
			self_type operator++(int junk) { self_type i = *this; m_pos++; return i; }
			self_type &operator++() { m_pos++; return *this; }
			self_type operator+(size_t n) const { return self_type(m_c, m_pos + n); }
			const_pointer operator->() const { return &m_c[m_pos]; }
			const_reference operator*() const { return m_c[m_pos]; }
			bool operator==(const self_type& rhs) { return &m_c == &rhs.m_c && m_pos == rhs.m_pos; }
			bool operator!=(const self_type& rhs) { return !(*this == rhs); }
		};

		typedef Entry value_type;

		virtual ~IEntrySlice() {}
		virtual size_t size() const = 0;
		virtual bool empty() const = 0;
		virtual Entry &operator[](size_t i) = 0;
		virtual const Entry &operator[](size_t i) const = 0;
		Entry &at(size_t i) { return (*this)[i]; }
		virtual iterator begin() = 0;
		virtual iterator end() = 0;
		virtual const_iterator begin() const = 0;
		virtual const_iterator end() const = 0;
		virtual void truncate(size_t new_count) = 0;
	};

	template<class Container>
	struct EntrySlice : public IEntrySlice {
		size_t start;
		size_t count;
		const Container *container;
		EntrySlice(const Container &c) : EntrySlice(c, 0, 0) {}
		EntrySlice(const Container &c, size_t start_, size_t end_) : container(&c) {
			start = start_;
			count = (end_ == 0 ? c.size() : end_) - start_;
		}
		virtual ~EntrySlice() {}
		virtual size_t size() const { return count; }
		virtual bool empty() const { return count == 0; }
		virtual Entry &operator[](size_t i) { return const_cast<Entry&>((*container)[int(start + i)]); }
		virtual const Entry &operator[](size_t i) const { return (*container)[int(start + i)]; }
		virtual iterator begin() { return iterator(*this, start); }
		virtual iterator end() { return iterator(*this, start + count); }
		virtual const_iterator begin() const { return const_iterator(*this, start); }
		virtual const_iterator end() const { return const_iterator(*this, start + count); }
		virtual void truncate(size_t new_count) { if (new_count < count) count = new_count; }
	};

	typedef std::unique_ptr<IEntrySlice> IEntrySlicePtr;
	class EntryRange : public IEntrySlice {
	public:
		IEntrySlicePtr storage;
		IEntrySlicePtr unstable;

		EntryRange(IEntrySlicePtr &&storage_ = IEntrySlicePtr(), IEntrySlicePtr &&unstable_ = IEntrySlicePtr());
		EntryRange(EntryRange &&r);
		EntryRange &operator= (EntryRange &&r);
		virtual size_t size() const;
		virtual bool empty() const;
		virtual Entry &operator[](size_t i);
		virtual const Entry &operator[](size_t i) const;
		virtual iterator begin();
		virtual iterator end();
		virtual const_iterator begin() const;
		virtual const_iterator end() const;
		virtual void truncate(size_t new_count);
	};

	namespace detail {
		template<class T>
		T make_slice_impl(const T &c, size_t start_, size_t count_, std::true_type) {
			return T(*c.container, start_ + c.start, count_);
		}

		template<class T>
		EntrySlice<T> make_slice_impl(const T &c, size_t start_, size_t count_, std::false_type) {
			return EntrySlice<T>(c, start_, count_);
		}
	}

	template<class T>
	typename std::conditional<std::is_base_of<IEntrySlice, T>::value, T, EntrySlice<T>>::type
		make_slice(const T &c, size_t start_ = 0, size_t count_ = 0) {
		return detail::make_slice_impl(c, start_, count_, std::conditional_t<std::is_base_of<IEntrySlice, T>::value, std::true_type, std::false_type>());
	}
	inline const IEntrySlice &make_slice(const IEntrySlice &c) {
		return c;
	}
	inline EntrySlice<IEntrySlice> make_slice(const IEntrySlice &c, size_t start_, size_t count_ = 0) {
		return EntrySlice<IEntrySlice>(c, start_, count_);
	}
	inline const EntryRange &make_slice(const EntryRange &c) {
		return c;
	}
	inline EntrySlice<EntryRange> make_slice(const EntryRange &c, size_t start_, size_t count_ = 0) {
		return EntrySlice<EntryRange>(c, start_, count_);
	}
}
