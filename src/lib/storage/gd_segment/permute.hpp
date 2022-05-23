
#include <vector>
#include <algorithm>
#include <numeric>
#include <functional>

namespace permute
{

template <typename T>
std::vector<std::size_t> sort_permutation(const std::vector<T>& vec)
{
    std::vector<std::size_t> p(vec.size());
    std::iota(p.begin(), p.end(), 0);
    std::sort(p.begin(), p.end(), 
        [&](const auto& i, const auto& j){ return vec[i] < vec[j]; });
    return p;
}

template <typename T>
void apply_permutation_in_place(
    std::vector<T>& vec,
    const std::vector<std::size_t>& p)
{
    T tmp;
    std::vector<bool> done(vec.size());
    std::size_t prev_j, j;

    for (std::size_t i = 0; i < vec.size(); ++i)
    {
        if (done[i])
        {
            continue;
        }
        done[i] = true;
        prev_j = i;
        j = p[i];
        while (i != j)
        {   
            std::swap(vec[prev_j], vec[j]);
            done[j] = true;
            prev_j = j;
            j = p[j];
        }
    }
}

}