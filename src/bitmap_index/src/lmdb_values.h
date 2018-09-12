// This file must contain all the data structures which can be used as a key or a value  for LMDB dictionary.
#ifndef _LMDB_VALUES_H__
#define _LMDB_VALUES_H__

#include <memory>
#include <cstring>
#include "common.h"
#include "index_utils.h"
#include "value_decomposer.h"

namespace LMDB {

// An abstract class to derive from for all LMDB key/values.
    class LMDBType {
    public:
        // Returns byte length of the current element.
        virtual uint32_t byte_length() const = 0;

        // Writes current value into a pre-allocated byte array.
        virtual void to_byte_array(char* buffer) const = 0;

        // Reads the value from the byte array.
        virtual void from_byte_array(uint32_t size, const char* buffer) = 0;
    };

// Used to store offset ranges for bitmap or BitmapStorage in LMDB dictionary.
    class OffsetRange : public LMDBType {
    public:
        OffsetRange() = default;
        OffsetRange(uint32_t start_offset, uint32_t end_offset);

        // Returns byte length of the current element.
        uint32_t byte_length() const override;

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override;

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override;

        uint32_t start_offset;
        uint32_t end_offset;
    };

// To be used as a key when mapping the bitmap to it's offset range in a file.
    class BitmapLMDBID : public LMDBType {
    public:
        BitmapLMDBID() = default;
        BitmapLMDBID(
                uint32_t rg_id,
                const std::string& column_dotted_path,
                uint16_t bitmap_number);

        // Returns byte length of the current element.
        uint32_t byte_length() const override;

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override;

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override;

        // Equality operator is required for the key data structures.
        bool operator==(const BitmapLMDBID& other) const;

        // Equality operator is required for the key data structures.
        bool operator!=(const BitmapLMDBID& other) const;


        // The row_group id.
        uint32_t rg_id;

        // Column which is indexed by current bitmap index.
        std::string column_dotted_path;

        // Created from atribute number and number of the bitmap in the atribute.
        uint16_t bitmap_number;

    };

// To be used as a key when mapping the bitmap Storage to it's offset range in a file.
    class BitmapStorageLMDBID : public LMDBType {
    public:
        BitmapStorageLMDBID() = default;
        BitmapStorageLMDBID(uint32_t rg_id, const std::string& column_dotted_path);

        // Returns byte length of the current element.
        uint32_t byte_length() const override;

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override;

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override;

        // Equality operator is required for the key data structures.
        bool operator==(const BitmapStorageLMDBID& other) const;

        // Equality operator is required for the key data structures.
        bool operator!=(const BitmapStorageLMDBID& other) const;

        // The row_group id.
        uint32_t rg_id;

        // Column which is indexed by current bitmap index.
        std::string column_dotted_path;

    };


// To be used as a key for attribute value mapping, when mapping the value to [0..cardinality].
// T must be a primitive type.
    template<class T>
    class AttributeValue : public LMDBType {
    public:
        AttributeValue() = default;
        AttributeValue(
                uint32_t rg_id,
                const std::string& column_dotted_path,
                const T& value)
                : rg_id(rg_id)
                , column_dotted_path(column_dotted_path)
                , value(value) {

        }

        const T& get() const {
            return value;
        }

        // Returns byte length of the current element.
        uint32_t byte_length() const override {
            return sizeof(rg_id) + column_dotted_path.size() + 1 + sizeof(value);
        }

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override {
            // This way of serialization makes sure values from the same row
            // group and column are forming a consecutive range.
            std::memcpy(buffer, &rg_id, sizeof(rg_id));
            std::memcpy(buffer + sizeof(rg_id), column_dotted_path.c_str(),
                    column_dotted_path.size());
            // This ensures we know where the dotted_path finished.
            buffer[sizeof(rg_id) + column_dotted_path.size()] = 0;
            std::memcpy(
                    buffer + sizeof(rg_id) + column_dotted_path.size() + 1,
                    &value,
                    sizeof(value)
                       );
        }

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override {
            std::memcpy(&rg_id, buffer, sizeof(rg_id));
            // Read until first \0.
            column_dotted_path = std::string(buffer + sizeof(rg_id));
            std::memcpy(
                    &value,
                    buffer + sizeof(rg_id) + column_dotted_path.size() + 1,
                    sizeof(value)
                       );
        }

        // Equality operator is required for the key data structures.
        bool operator==(const AttributeValue<T>& other) const {
            return rg_id == other.rg_id &&
                   column_dotted_path == other.column_dotted_path &&
                   value == other.value;
        }

        bool operator!=(const AttributeValue<T>& other) const {
            return rg_id != other.rg_id ||
                   column_dotted_path != other.column_dotted_path ||
                   value != other.value;
        }
        // The row_group id.
        uint32_t rg_id;

        // Column which is indexed by current bitmap index.
        std::string column_dotted_path;

        T value;
    };

// Specialization for std::string.
    template<>
    class AttributeValue<std::string> : public LMDBType {
    public:
        AttributeValue() = default;
        AttributeValue(
                uint32_t rg_id,
                const std::string& column_dotted_path,
                const std::string& value)
                : rg_id(rg_id)
                , column_dotted_path(column_dotted_path)
                , value(value) {

        }

        const std::string& get() const {
            return value;
        }

        // Returns byte length of the current element.
        uint32_t byte_length() const override {
            return sizeof(rg_id) + column_dotted_path.size() + 1 + value.size() + 1;
        }

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override {
            // This way of serialization makes sure values from the same row
            // group and column are forming a consecutive range.
            std::memcpy(buffer, &rg_id, sizeof(rg_id));
            std::memcpy(buffer + sizeof(rg_id), column_dotted_path.c_str(),
                    column_dotted_path.size());
            // This ensures we know where the dotted_path finished.
            buffer[sizeof(rg_id) + column_dotted_path.size()] = 0;
            std::memcpy(
                    buffer + sizeof(rg_id) + column_dotted_path.size() + 1,
                    value.c_str(),
                    value.size()
                       );
            buffer[sizeof(rg_id) + column_dotted_path.size() + 1 + value.size()] = 0;
        }

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override {
            std::memcpy(&rg_id, buffer, sizeof(rg_id));
            // Read until first \0.
            column_dotted_path = std::string(buffer + sizeof(rg_id));
            // Read until next \0.
            value = std::string(buffer + sizeof(rg_id) + column_dotted_path.size() + 1);
        }

        // Equality operator is required for the key data structures.
        bool operator==(const AttributeValue<std::string>& other) const {
            return rg_id == other.rg_id &&
                   column_dotted_path == other.column_dotted_path &&
                   value == other.value;
        }

        bool operator!=(const AttributeValue<std::string>& other) const {
            return rg_id != other.rg_id ||
                   column_dotted_path != other.column_dotted_path ||
                   value != other.value;
        }

        // The row_group id.
        uint32_t rg_id;

        // Column which is indexed by current bitmap index.
        std::string column_dotted_path;

        std::string value;

    };


// Used to store built-in simple types in LMDB.
    template<class T>
    class LMDBValue : public LMDBType {
    public:
        LMDBValue() = default;

        LMDBValue(const T& value)
                : value_(value) {
        }

        const T& get() const {
            return value_;
        }

        // Returns byte length of the current element.
        uint32_t byte_length() const override {
            return sizeof(value_);
        }

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override {
            std::memcpy(buffer, &value_, sizeof(value_));
        }

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override {
            std::memcpy(&value_, buffer, sizeof(value_));
        }

        bool operator==(const LMDBValue<T>& other) const {
            return value_ == other.value_;
        }

        bool operator!=(const LMDBValue<T>& other) const {
            return value_ != other.value_;
        }

    private:
        T value_;

    };

// Class to store all auxiliary data related to bitmap index.
// The class must be serializable to be stored in LMDB.
    class BitmapIndexAuxData : public LMDBType {
    public:

        BitmapIndexAuxData() = default;
        BitmapIndexAuxData(BitmapIndexAuxData&& ) = default;
        BitmapIndexAuxData(const BitmapIndexAuxData&) = default;

        // Returns byte length of the current element.
        uint32_t byte_length() const override;

        // Writes current value into a pre-allocated byte array.
        void to_byte_array(char* buffer) const override;

        // Reads the value from the byte array.
        void from_byte_array(uint32_t size, const char* buffer) override;

        // Used to decompose integer values.
        std::shared_ptr<ValueDecomposer> vd;

        // bitmap_counts[i] shows the number of bitmaps for attribute value i.
        std::vector<uint32_t> bitmap_counts;

        // Cardinality of the column which is indexed, or it's upper bound estimate.
        uint32_t cardinality;

        // Encoding type used for the current index.
        BitmapIndexEncodingType enc_type;

        // If set to true, will create an LMDB mapping from the values in DB to
        // integers from range [0..cardinality].
        bool use_value_mapping;

        // The minimal value you can get after attribute value mapping, in case it's used,
        // or just the minimal value in the index.
        int64_t min_mapped_value;

        // The maximal value you can get after attribute value mapping, in case it's used,
        // or just the maximal value in the index.
        int64_t max_mapped_value;

    };

} // namespace LMDB

#endif // _LMDB_VALUES_H__