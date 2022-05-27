#pragma once

#include <type_traits>

#include "storage/abstract_segment.hpp"
#include "storage/gd_segment_v1.hpp"

#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class GdSegmentV1Iterable : public PointAccessibleSegmentIterable<GdSegmentV1Iterable<T>> {
 private:
	const GdSegmentV1<T>& _segment;
	std::shared_ptr<const compact::vector<T>> bases;
	std::shared_ptr<const compact::vector<unsigned>> deviations;
	std::shared_ptr<const compact::vector<size_t>> reconstruction_list;

 public:
	using ValueType = T;

	explicit GdSegmentV1Iterable(const GdSegmentV1<T>& segment)
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
				_segment.get_dev_bits(), ChunkOffset{0u}
		};

		auto end = Iterator<BasesIteratorType, DevsIteratorType, ReconListIteratorType>{
				bases->cbegin(), deviations->cbegin(), reconstruction_list->cbegin(),
				_segment.get_dev_bits(), static_cast<ChunkOffset>(_segment.size())
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
			_segment.get_dev_bits(), position_filter->cbegin(), 
			position_filter->cbegin()
		};
		auto end = PointAccessIterator<PosListIteratorType, BasesIteratorType, DevsIteratorType, ReconListIteratorType>{
			bases->cbegin(), deviations->cbegin(), reconstruction_list->cbegin(),
			_segment.get_dev_bits(), position_filter->cbegin(), 
			position_filter->cend()
		};
		functor(begin, end);
	}

	size_t _on_size() const { return _segment.size(); }

 private:
	template<typename BasesIteratorType, typename DevsIteratorType, typename ReconListIteratorType>
	class Iterator
		: public AbstractSegmentIterator<Iterator<BasesIteratorType, DevsIteratorType, ReconListIteratorType>, SegmentPosition<T>> {
	private:
		BasesIteratorType bases_begin_it;
		DevsIteratorType devs_begin_it;
		ReconListIteratorType recon_list_it;
		ChunkOffset _chunk_offset;
		unsigned dev_bits;

	 public:
		using ValueType = T;
		using IterableType = GdSegmentV1Iterable<T>;

		Iterator(BasesIteratorType bases_it, DevsIteratorType devs_it, ReconListIteratorType recon_it,
					unsigned dev_bits, ChunkOffset chunk_offset)
				: bases_begin_it{std::move(bases_it)},
					devs_begin_it{std::move(devs_it)},
					recon_list_it{std::move(recon_it)},
					dev_bits(dev_bits),
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

		bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

		std::ptrdiff_t distance_to(const Iterator& other) const { return other._chunk_offset - _chunk_offset; }

		SegmentPosition<T> dereference() const {
			const size_t base_idx = *recon_list_it;

			const T base = *(bases_begin_it + base_idx);
			const unsigned dev = *(devs_begin_it + _chunk_offset);
			const T value = gdd_lsb::rt::reconstruct_value<T>(base, dev, dev_bits);
			
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
		ReconListIteratorType recon_list_it;
		unsigned dev_bits;

	 public:
		using ValueType = T;
		using IterableType = GdSegmentV1Iterable<T>;

		PointAccessIterator(
			BasesIteratorType bases_it, DevsIteratorType devs_it, ReconListIteratorType recon_it,
				unsigned dev_bits, 
			PosListIteratorType position_filter_begin,
			PosListIteratorType position_filter_it) : 
			AbstractPointAccessSegmentIterator<
							PointAccessIterator<PosListIteratorType, BasesIteratorType, DevsIteratorType, ReconListIteratorType>, SegmentPosition<T>,
							PosListIteratorType>{std::move(position_filter_begin), std::move(position_filter_it)},
				bases_begin_it{std::move(bases_it)},
				devs_begin_it{std::move(devs_it)},
				recon_list_it{std::move(recon_it)},
				dev_bits(dev_bits)
			{}

	 private:
		friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface
		
		SegmentPosition<T> dereference() const {
			const auto& chunk_offsets = this->chunk_offsets();
			
			//std::cout << "  GdV1 PAI #"+std::to_string(chunk_offsets.offset_in_referenced_chunk)+"\n";

			const size_t base_idx = *(recon_list_it + chunk_offsets.offset_in_referenced_chunk);
			const T base = *(bases_begin_it + base_idx);
			const unsigned dev = *(devs_begin_it + chunk_offsets.offset_in_referenced_chunk);
			const T value = gdd_lsb::rt::reconstruct_value<T>(base, dev, dev_bits);

			return SegmentPosition<T>{ value, false, chunk_offsets.offset_in_poslist};
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
