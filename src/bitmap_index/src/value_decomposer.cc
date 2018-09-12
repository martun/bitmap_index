#include "value_decomposer.h"

ValueDecomposer::ValueDecomposer(const std::vector<uint32_t>& basis) 
	: basis(basis) {
  is_base_2 = true;
  for (uint32_t base : basis) {
    if (base != 2) {
      is_base_2 = false;
      break;
    }
  }
}

bool ValueDecomposer::decompose(std::vector<uint32_t>& result, uint64_t value) {
	result.resize(basis.size());
  if (is_base_2) {
  	for (int i = basis.size() - 1; i >= 0; --i) {
  		result[i] = value & 1;
      value >>= 1;
    }
  } else {
  	for (int i = basis.size() - 1; i >= 0; --i) {
  		result[i] = value % basis[i];
  		value /= basis[i];
  	}
  }
	// If we did not reduce the input to 0, then it was greater than the 
	// product of basis.
	if (value)
		return false;
	return true;
}

uint32_t ValueDecomposer::get_base(uint32_t index) const {
	return basis[index];
}

const std::vector<uint32_t>& ValueDecomposer::get_basis() const {
	return basis;
}
