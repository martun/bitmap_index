#include "lmdb_values.h"
#include <string>

namespace LMDB {

    OffsetRange::OffsetRange(uint32_t start_offset, uint32_t end_offset)
            : start_offset(start_offset)
            , end_offset(end_offset)
    {
    }

    uint32_t OffsetRange::byte_length() const {
      return sizeof(start_offset) + sizeof(end_offset);
    }

    void OffsetRange::to_byte_array(char* buffer) const {
      std::memcpy(buffer, &start_offset, sizeof(start_offset));
      std::memcpy(buffer + sizeof(start_offset), &end_offset, sizeof(end_offset));
    }

    void OffsetRange::from_byte_array(uint32_t size, const char* buffer) {
      std::memcpy(&start_offset, buffer, sizeof(start_offset));
      std::memcpy(&end_offset, buffer + sizeof(start_offset), sizeof(end_offset));
    }

    BitmapLMDBID::BitmapLMDBID(
            uint32_t rg_id,
            const std::string& column_dotted_path,
            uint16_t bitmap_number)
            : rg_id(rg_id)
            , column_dotted_path(column_dotted_path)
            , bitmap_number(bitmap_number)
    {
    }

    uint32_t BitmapLMDBID::byte_length() const {
      return sizeof(rg_id) + column_dotted_path.size() + 1 + sizeof(bitmap_number);
    }

    void BitmapLMDBID::to_byte_array(char* buffer) const {
      // This way of serialization makes sure values from the same row
      // group and column are forming a consecutive range.
      std::memcpy(buffer, &rg_id, sizeof(rg_id));
      std::memcpy(buffer + sizeof(rg_id), column_dotted_path.c_str(),
              column_dotted_path.size());
      // This ensures we know where the dotted_path finished.
      buffer[sizeof(rg_id) + column_dotted_path.size()] = 0;
      std::memcpy(
              buffer + sizeof(rg_id) + column_dotted_path.size() + 1,
              &bitmap_number,
              sizeof(bitmap_number)
                 );
    }

    void BitmapLMDBID::from_byte_array(uint32_t size, const char* buffer) {
      std::memcpy(&rg_id, buffer, sizeof(rg_id));
      // Read until first \0.
      column_dotted_path = std::string(buffer + sizeof(rg_id));
      std::memcpy(
              &bitmap_number,
              buffer + sizeof(rg_id) + column_dotted_path.size() + 1,
              sizeof(bitmap_number)
                 );
    }

    bool BitmapLMDBID::operator==(const BitmapLMDBID& other) const {
      return rg_id == other.rg_id &&
             column_dotted_path == other.column_dotted_path &&
             bitmap_number == other.bitmap_number;
    }

    bool BitmapLMDBID::operator!=(const BitmapLMDBID& other) const {
      return rg_id != other.rg_id ||
             column_dotted_path != other.column_dotted_path ||
             bitmap_number != other.bitmap_number;
    }

    BitmapStorageLMDBID::BitmapStorageLMDBID(
            uint32_t rg_id,
            const std::string& column_dotted_path)
            : rg_id(rg_id)
            , column_dotted_path(column_dotted_path)
    {
    }

    uint32_t BitmapStorageLMDBID::byte_length() const {
      return sizeof(rg_id) + column_dotted_path.size() + 1;
    }

    void BitmapStorageLMDBID::to_byte_array(char* buffer) const {
      // This way of serialization makes sure values from the same row
      // group and column are forming a consecutive range.
      std::memcpy(buffer, &rg_id, sizeof(rg_id));
      std::memcpy(buffer + sizeof(rg_id), column_dotted_path.c_str(),
              column_dotted_path.size());
      // This ensures we know where the dotted_path finished.
      buffer[sizeof(rg_id) + column_dotted_path.size()] = 0;
    }

    void BitmapStorageLMDBID::from_byte_array(uint32_t size, const char* buffer) {
      std::memcpy(&rg_id, buffer, sizeof(rg_id));
      // Read until first \0.
      column_dotted_path = std::string(buffer + sizeof(rg_id));
    }

    bool BitmapStorageLMDBID::operator==(const BitmapStorageLMDBID& other) const {
      return rg_id == other.rg_id && column_dotted_path == other.column_dotted_path;
    }

    bool BitmapStorageLMDBID::operator!=(const BitmapStorageLMDBID& other) const {
      return rg_id != other.rg_id || column_dotted_path != other.column_dotted_path;
    }

    uint32_t BitmapIndexAuxData::byte_length() const {
      // The code of this function is intentionally similar to the code of "to_byte_array".
      // Write basis.
      const std::vector<uint32_t>& basis = vd->get_basis();
      uint32_t current_offset = 0;
      uint32_t basis_size = basis.size();
      current_offset += sizeof(basis_size);
      current_offset += basis_size * sizeof(uint32_t);

      uint32_t bitmap_counts_size = bitmap_counts.size();
      current_offset += sizeof(bitmap_counts_size);
      current_offset += bitmap_counts_size * sizeof(uint32_t);

      // Write encoding type in 1 byte.
      current_offset += sizeof(char);

      // Write cardinality.
      current_offset += sizeof(cardinality);

      current_offset += sizeof(enc_type);
      current_offset += sizeof(use_value_mapping);
      current_offset += sizeof(min_mapped_value);
      current_offset += sizeof(max_mapped_value);
      return current_offset;
    }

    void BitmapIndexAuxData::to_byte_array(char* buffer) const {
      // Write basis.
      const std::vector<uint32_t>& basis = vd->get_basis();
      uint32_t current_offset = 0;
      uint32_t basis_size = basis.size();
      std::memcpy(buffer, &basis_size, sizeof(basis_size));
      current_offset += sizeof(basis_size);
      for (size_t i = 0; i < basis_size; ++i) {
        std::memcpy(buffer + current_offset, &basis[i], sizeof(basis[i]));
        current_offset += sizeof(basis[i]);
      }

      uint32_t bitmap_counts_size = bitmap_counts.size();
      std::memcpy(buffer, &bitmap_counts_size, sizeof(bitmap_counts_size));
      current_offset += sizeof(bitmap_counts_size);
      for (size_t i = 0; i < bitmap_counts_size; ++i) {
        std::memcpy(buffer + current_offset, &bitmap_counts[i], sizeof(bitmap_counts[i]));
        current_offset += sizeof(bitmap_counts[i]);
      }

      // Write encoding type in 1 byte.
      char c = static_cast<char>(enc_type);
      std::memcpy(buffer + current_offset, &c, sizeof(c));
      current_offset += sizeof(c);

      // Write cardinality.
      std::memcpy(buffer + current_offset, &cardinality, sizeof(cardinality));
      current_offset += sizeof(cardinality);

      // Write enc_type.
      std::memcpy(buffer + current_offset, &enc_type, sizeof(enc_type));
      current_offset += sizeof(enc_type);

      std::memcpy(buffer + current_offset, &use_value_mapping, sizeof(use_value_mapping));
      current_offset += sizeof(use_value_mapping);

      std::memcpy(buffer + current_offset, &min_mapped_value, sizeof(min_mapped_value));
      current_offset += sizeof(min_mapped_value);

      std::memcpy(buffer + current_offset, &max_mapped_value, sizeof(max_mapped_value));
      current_offset += sizeof(max_mapped_value);
    }

    void BitmapIndexAuxData::from_byte_array(uint32_t size, const char* buffer) {
      // Read the basis.
      std::vector<uint32_t> basis;
      uint32_t current_offset = 0;
      uint32_t basis_size;
      std::memcpy(&basis_size, buffer, sizeof(basis_size));
      current_offset += sizeof(basis_size);
      basis.resize(basis_size);
      for (size_t i = 0; i < basis_size; ++i) {
        std::memcpy(&basis[i], buffer + current_offset, sizeof(basis[i]));
        current_offset += sizeof(basis[i]);
      }

      uint32_t bitmap_counts_size;
      std::memcpy(&bitmap_counts_size, buffer, sizeof(bitmap_counts_size));
      current_offset += sizeof(bitmap_counts_size);
      bitmap_counts.resize(bitmap_counts_size);
      for (size_t i = 0; i < bitmap_counts_size; ++i) {
        std::memcpy(&bitmap_counts[i], buffer + current_offset, sizeof(bitmap_counts[i]));
        current_offset += sizeof(bitmap_counts[i]);
      }

      // Create a value decomposer with the basis read.
      vd.reset(new ValueDecomposer(basis));

      // Read encoding type in 1 byte.
      char c;
      std::memcpy(&c, buffer + current_offset, sizeof(c));
      current_offset += sizeof(c);
      enc_type = static_cast<BitmapIndexEncodingType>(c);

      // Read cardinality.
      std::memcpy(&cardinality, buffer + current_offset, sizeof(cardinality));
      current_offset += sizeof(cardinality);

      std::memcpy(&enc_type, buffer + current_offset, sizeof(enc_type));
      current_offset += sizeof(enc_type);

      std::memcpy(&use_value_mapping, buffer + current_offset, sizeof(use_value_mapping));
      current_offset += sizeof(use_value_mapping);

      std::memcpy(&min_mapped_value, buffer + current_offset, sizeof(min_mapped_value));
      current_offset += sizeof(min_mapped_value);

      std::memcpy(&max_mapped_value, buffer + current_offset, sizeof(max_mapped_value));
      current_offset += sizeof(max_mapped_value);
    }

} // namespace LMDB