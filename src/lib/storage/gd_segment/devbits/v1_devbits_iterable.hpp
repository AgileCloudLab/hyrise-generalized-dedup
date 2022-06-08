#pragma once

#include <type_traits>

#include "storage/base_gd_segment.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/gd_segment_v1_fwd.hpp"

#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class GdSegmentV1Iterable : public PointAccessibleSegmentIterable<GdSegmentV1Iterable<T>> {
 private:
	GdSegmentV1<T> const * segment_ptr;

 public:
	using ValueType = T;

	explicit GdSegmentV1Iterable(const GdSegmentV1<T>& segment)
			: segment_ptr(&segment)
			{ }

	template <typename Functor>
	void _on_with_iterators(const Functor& functor) const {

		auto begin = Iterator{segment_ptr, ChunkOffset{0u}};
		auto end = Iterator{segment_ptr, segment_ptr->size()};

		functor(begin, end);
	}

	template <typename Functor, typename PosListType>
	void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
		
		using PosListIteratorType = decltype(position_filter->cbegin());

		auto begin = PointAccessIterator<PosListIteratorType>{
			segment_ptr, position_filter->cbegin(), position_filter->cbegin()
		};
		auto end = PointAccessIterator<PosListIteratorType>{
			segment_ptr, position_filter->cbegin(), position_filter->cend()
		};
		functor(begin, end);
	}

	size_t _on_size() const { return static_cast<size_t>(segment_ptr->size()); }

 private:
	class Iterator
		: public AbstractSegmentIterator<Iterator, SegmentPosition<T>> {
	private:
		BaseGdSegment const * segment_ptr;
		ChunkOffset _chunk_offset;

	 public:
		using ValueType = T;
		using IterableType = GdSegmentV1Iterable<T>;

		Iterator(BaseGdSegment *segment_ptr, ChunkOffset chunk_offset)
				: segment_ptr{segment_ptr},
				_chunk_offset{chunk_offset} 
				{}

	 private:
		friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

		void increment() {
			++_chunk_offset;
		}

		void decrement() {
			--_chunk_offset;
		}

		void advance(std::ptrdiff_t n) {
			_chunk_offset += n;
		}

		bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

		std::ptrdiff_t distance_to(const Iterator& other) const { return other._chunk_offset - _chunk_offset; }

		SegmentPosition<T> dereference() const {
			const auto value = (*segment_ptr)[_chunk_offset];
			return SegmentPosition<T>{ boost::get<T>(value), false, _chunk_offset};
			/*
			const size_t base_idx = *recon_list_it;

			const T base = *(bases_begin_it + base_idx);
			const unsigned dev = *(devs_begin_it + _chunk_offset);
			const T value = gdd_lsb::rt::reconstruct_value<T>(base, dev, dev_bits);
			
			return SegmentPosition<T>{ value, false, _chunk_offset};
			*/
		}

	};

	template<typename PosListIteratorType>
	class PointAccessIterator
		: public AbstractPointAccessSegmentIterator<
									PointAccessIterator<PosListIteratorType>, 
									SegmentPosition<T>, 
									PosListIteratorType
								> {
	private:
		BaseGdSegment const * segment_ptr;

	 public:
		using ValueType = T;
		using IterableType = GdSegmentV1Iterable<T>;

		PointAccessIterator(
			BaseGdSegment const * segment_ptr, 
			PosListIteratorType position_filter_begin,
			PosListIteratorType position_filter_it) : 
			AbstractPointAccessSegmentIterator<
							PointAccessIterator<PosListIteratorType>, SegmentPosition<T>,
							PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
				segment_ptr(segment_ptr)
			{}

	 private:
		friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface
		
		SegmentPosition<T> dereference() const {
			const auto& chunk_offsets = this->chunk_offsets();
			
			const auto value = (*segment_ptr)[chunk_offsets.offset_in_referenced_chunk];
			return SegmentPosition<T>{ boost::get<T>(value), false, chunk_offsets.offset_in_poslist};

			/*
			const size_t base_idx = *(recon_list_it + chunk_offsets.offset_in_referenced_chunk);
			const T base = *(bases_begin_it + base_idx);
			const unsigned dev = *(devs_begin_it + chunk_offsets.offset_in_referenced_chunk);
			const T value = gdd_lsb::rt::reconstruct_value<T>(base, dev, dev_bits);

			return SegmentPosition<T>{ value, false, chunk_offsets.offset_in_poslist};
			*/
		}

	};


};

template <typename T>
struct is_gd_segment_v1_iterable {
	static constexpr auto value = false;
};

template <template <typename T> typename Iterable, typename T>
struct is_gd_segment_v1_iterable<Iterable<T>> {
	static constexpr auto value = std::is_same_v<GdSegmentV1Iterable<T>, Iterable<T>>;
};

template <typename T>
inline constexpr bool is_gd_segment_v1_iterable_v = is_gd_segment_v1_iterable<T>::value;

}  // namespace opossum
