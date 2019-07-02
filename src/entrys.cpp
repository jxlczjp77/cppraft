#include <raft/entrys.hpp>

namespace raft {
	EntryRange::EntryRange(IEntrySlicePtr &&storage_, IEntrySlicePtr &&unstable_)
		: storage(std::move(storage_)), unstable(std::move(unstable_)) {
	}

	EntryRange::EntryRange(EntryRange &&r)
		: storage(std::move(r.storage)), unstable(std::move(r.unstable)) {
	}

	EntryRange &EntryRange::operator= (EntryRange &&r) {
		unstable = std::move(r.unstable);
		storage = std::move(r.storage);
		return *this;
	}

	size_t EntryRange::size() const {
		size_t c1 = storage ? storage->size() : 0;
		size_t c2 = unstable ? unstable->size() : 0;
		return c1 + c2;
	}

	bool EntryRange::empty() const {
		return size() == 0;
	}

	Entry &EntryRange::operator[](size_t i) {
		if (storage) {
			size_t storage_count = storage->size();
			if (i < storage_count) {
				return storage->at(i);
			}
			i -= storage_count;
		}
		return unstable->at(i);
	}

	const Entry &EntryRange::operator[](size_t i) const {
		return (*const_cast<EntryRange*>(this))[i];
	}

	EntryRange::iterator EntryRange::begin() {
		return iterator(*this, 0);
	}

	EntryRange::iterator EntryRange::end() {
		return iterator(*this, size());
	}

	EntryRange::const_iterator EntryRange::begin() const {
		return const_iterator(*this, 0);
	}

	EntryRange::const_iterator EntryRange::end() const {
		return const_iterator(*this, size());
	}

	void EntryRange::truncate(size_t new_count) {
		if (storage) {
			size_t storage_count = storage->size();
			if (new_count <= storage_count) {
				unstable.reset();
				storage->truncate(new_count);
				return;
			}
			new_count -= storage_count;
		}
		if (unstable) {
			unstable->truncate(new_count);
		}
	}
}
