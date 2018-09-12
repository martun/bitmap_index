#ifndef _VALUE_DECOMPOSER_H__
#define _VALUE_DECOMPOSER_H__

#include <vector>
#include <stdint.h>

// Decomposes a value into multiple parts using given basis.
class ValueDecomposer
{
public:
	/** @brief Constructor.
	 *  @param[in] basis The basis to use for decomposition.
	 */
	ValueDecomposer(const std::vector<uint32_t>& basis);

	/** @brief Decomposes given value using the basis.
	 *  @param[out] result The result of decomposition.
	 *  @param[in] value the value to be decomposed.
	 *  @returns True, if the decomposition was possible. In case value > product of basis, returns false.
	 */
	bool decompose(std::vector<uint32_t>& result, uint64_t value);

	/// Returns index-th base value.
	uint32_t get_base(uint32_t index) const;

	/// Returns the basis vector.
	const std::vector<uint32_t>& get_basis() const;

private:
	// The basis for decomposition.
	std::vector<uint32_t> basis;

  // If set to true, then all the values in basis are equal to 2.
  bool is_base_2;

};

#endif  // _VALUE_DECOMPOSER_H__

