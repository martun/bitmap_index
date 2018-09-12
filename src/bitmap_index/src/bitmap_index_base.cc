#include "bitmap_index_base.h"

BitmapIndexBase::BitmapIndexBase(
        std::shared_ptr<RowGroupInfo>& rg_info,
        std::shared_ptr<ColumnReference>& column_ref,
        std::shared_ptr<BitMapStorage>&& storage)
    : rg_info_(rg_info)
    , column_ref_(column_ref)
    , storage_(std::move(storage)) 
{
}

std::string BitmapIndexBase::get_stats() const {
    // TODO(martun): implement this.
    return "";
}

// Returns a reference to the storage.
BitMapStorage& BitmapIndexBase::storage() const {
	return *storage_;
}

