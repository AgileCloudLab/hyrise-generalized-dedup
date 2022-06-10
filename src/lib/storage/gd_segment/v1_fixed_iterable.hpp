#pragma once

#include <type_traits>

#include "storage/base_gd_segment.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/gd_segment_v1_fixed.hpp"

#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class GdSegmentV1FixedIterable : public PointAccessibleSegmentIterable<GdSegmentV1FixedIterable<T>> {
 private:

 	constexpr static unsigned DEV_BITS = 8u;
    constexpr static unsigned BASE_BITS = (sizeof(T)*8) - 8u;

	const BaseGdSegment& _segment;
	std::shared_ptr<const compact::vector<T, BASE_BITS>> bases;
	std::shared_ptr<const compact::vector<unsigned, DEV_BITS>> deviations;
	std::shared_ptr<const compact::vector<size_t>> reconstruction_list;

 public:
	using ValueType = T;

	explicit GdSegmentV1FixedIterable(const GdSegmentV1Fixed<T>& segment)
			: _segment{segment}, 
			bases{segment.get_bases()},
			deviations{segment.get_deviations()},
			reconstruction_list{segment.get_reconstruction_list()}
			{ }

	template <typename Functor>
	void _on_with_iterators(const Functor& functor) const {

      	using BasesIteratorType = decltype(bases->cbegin());
      	using DevsIteratorType = decltype(deviations->cbegin());
      	using ReconListIteratorType = decltype(reconstruction_list->cbegin());

		auto begin = Iterator<BasesIteratorType, DevsIteratorType, ReconListIteratorType>{
				bases->cbegin(), deviations->cbegin(), reconstruction_list->cbegin(),
				_segment.null_value_id(), ChunkOffset{0u}
		};

		auto end = Iterator<BasesIteratorType, DevsIteratorType, ReconListIteratorType>{
				bases->cbegin(), deviations->cbegin(), reconstruction_list->cend(),
				_segment.null_value_id(), _segment.size()
		};

		functor(begin, end);
	}

	template <typename Functor, typename PosListType>
	void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
		
		using BasesIteratorType = decltype(bases->cbegin());
      	using DevsIteratorType = decltype(deviations->cbegin());
      	using ReconListIteratorType = decltype(reconstruction_list->cbegin());
		using PosListIteratorType = decltype(position_filter->cbegin());

		auto begin = PointAccessIterator<PosListIteratorType, BasesIteratorType, DevsIteratorType, ReconListIteratorType>{
			bases->cbegin(), deviations->cbegin(), reconstruction_list->cbegin(),
			_segment.null_value_id(), 
			position_filter->cbegin(), position_filter->cbegin()
		};
		auto end = PointAccessIterator<PosListIteratorType, BasesIteratorType, DevsIteratorType, ReconListIteratorType>{
			bases->cbegin(), deviations->cbegin(), reconstruction_list->cbegin(),
			_segment.null_value_id(), 
			position_filter->cbegin(), position_filter->cend()
		};
		functor(begin, end);
	}

	size_t _on_size() const { return static_cast<size_t>(_segment.size()); }

 private:
	template<typename BasesIteratorType, typename DevsIteratorType, typename ReconListIteratorType>
	class Iterator
		: public AbstractSegmentIterator<Iterator<BasesIteratorType, DevsIteratorType, ReconListIteratorType>, SegmentPosition<T>> {
	private:
		BasesIteratorType bases_begin_it;
		DevsIteratorType devs_begin_it;
		ReconListIteratorType recon_list_it;
		ChunkOffset _chunk_offset;
		ValueID null_value_id;

	 public:
		using ValueType = T;
		using IterableType = GdSegmentV1FixedIterable<T>;

		Iterator(BasesIteratorType bases_it, DevsIteratorType devs_it, ReconListIteratorType recon_it,
					ValueID null_value_id, ChunkOffset chunk_offset)
				: bases_begin_it{std::move(bases_it)},
					devs_begin_it{std::move(devs_it)},
					recon_list_it{std::move(recon_it)},
					null_value_id(null_value_id),
					_chunk_offset{chunk_offset} 
					{}

	 private:
		friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

		void increment() {
			++_chunk_offset;
			++recon_list_it;
		}

		void decrement() {
			--_chunk_offset;
			--recon_list_it;
		}

		void advance(std::ptrdiff_t n) {
			_chunk_offset += n;
			recon_list_it += n;
		}

		bool equal(const Iterator& other) const { return recon_list_it == other.recon_list_it; }

		std::ptrdiff_t distance_to(const Iterator& other) const { return other.recon_list_it - recon_list_it; }

		SegmentPosition<T> dereference() const {
			const size_t base_idx = *recon_list_it;

			//std::cout << "Linear Iterator at #"+std::to_string(_chunk_offset)+", base_idx: " + std::to_string(base_idx) + ". Null Value ID: " + std::to_string(null_value_id) << std::endl;

			if(static_cast<ValueID>(base_idx) == null_value_id){
				// NULL
				return SegmentPosition<T>{ T{}, true, _chunk_offset}; 
			}

			const T base = *(bases_begin_it + base_idx);
			const unsigned dev = *(devs_begin_it + _chunk_offset);
			const T value = gdd_lsb::ct::reconstruct_value<T, DEV_BITS>(base, dev);
			
			return SegmentPosition<T>{ value, false, _chunk_offset};
		}

	};

	template<typename PosListIteratorType, typename BasesIteratorType, typename DevsIteratorType, typename ReconListIteratorType>
	class PointAccessIterator
		: public AbstractPointAccessSegmentIterator<
									PointAccessIterator<PosListIteratorType, BasesIteratorType, DevsIteratorType, ReconListIteratorType>, 
									SegmentPosition<T>, 
									PosListIteratorType
								> {
	private:
		BasesIteratorType bases_begin_it;
		DevsIteratorType devs_begin_it;
		ReconListIteratorType recon_list_begin_it;
		ValueID null_value_id;

	 public:
		using ValueType = T;
		using IterableType = GdSegmentV1FixedIterable<T>;

		PointAccessIterator(
			BasesIteratorType bases_it, DevsIteratorType devs_it, ReconListIteratorType recon_it,
				ValueID null_value_id,
			PosListIteratorType position_filter_begin,
			PosListIteratorType position_filter_it) : 
			AbstractPointAccessSegmentIterator<
							PointAccessIterator<PosListIteratorType, BasesIteratorType, DevsIteratorType, ReconListIteratorType>, SegmentPosition<T>,
							PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
				bases_begin_it{std::move(bases_it)},
				devs_begin_it{std::move(devs_it)},
				recon_list_begin_it{std::move(recon_it)},
				null_value_id(null_value_id)
			{}

	 private:
		friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface
		
		SegmentPosition<T> dereference() const {
			const auto& chunk_offsets = this->chunk_offsets();
			
			const size_t base_idx = *(recon_list_begin_it + chunk_offsets.offset_in_referenced_chunk);
			if(static_cast<ValueID>(base_idx) == null_value_id){
				// NULL
				return SegmentPosition<T>{ T{}, true, chunk_offsets.offset_in_poslist}; 
			}

			const T base = *(bases_begin_it + base_idx);
			const unsigned dev = *(devs_begin_it + chunk_offsets.offset_in_referenced_chunk);
			const T value = gdd_lsb::ct::reconstruct_value<T, DEV_BITS>(base, dev);

			return SegmentPosition<T>{ value, false, chunk_offsets.offset_in_poslist};
		}

	};


};

template <typename T>
struct is_gd_segment_v1_fixed_iterable {
	static constexpr auto value = false;
};

template <template <typename T> typename Iterable, typename T>
struct is_gd_segment_v1_fixed_iterable<Iterable<T>> {
	static constexpr auto value = std::is_same_v<GdSegmentV1FixedIterable<T>, Iterable<T>>;
};

template <typename T>
inline constexpr bool is_gd_segment_v1_fixed_iterable_v = is_gd_segment_v1_fixed_iterable<T>::value;

}  // namespace opossum
