/// Implementations of template functions from BitMapIndex.h.

namespace {

template <class T>
int64_t get_lmdb_attr_value_with_mapping(
    const T& value, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb) {
  auto txn = attr_values_lmdb->new_txn();
  auto iter = attr_values_lmdb->find(AttributeValue<T>(
        rg_info->id, column_ref->dotted_path, value), txn);
  if (iter == attr_values_lmdb->end(txn)) {
    throw std::invalid_argument("Unable to find given value in LMDB map.");
  }
  return iter->second.get();
}

// Looks up given value in attr_values_lmdb_.
template <class T>
int64_t get_lmdb_attr_value(
    typename std::enable_if<std::is_integral<T>::value, const T&>::type value, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb) {
  // If we don't want to use value mapping, return the same value.
  if (!aux_data->use_value_mapping) {
    return (int64_t)value - aux_data->min_mapped_value;
  }
  return get_lmdb_attr_value_with_mapping(value, rg_info, column_ref, aux_data, attr_values_lmdb);
}

template <class T>
int64_t get_lmdb_attr_value(
    typename std::enable_if<!std::is_integral<T>::value, const T&>::type value,
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb) {
  return get_lmdb_attr_value_with_mapping(value, rg_info, column_ref, aux_data, attr_values_lmdb);
}

template <class T>
int64_t lookup_min_value_with_mapping(
    const T& min, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    const BitmapIndexBase::IntervalFlags interval_flags) {
  typename BitmapIndex<T>::LMDBIterator min_iter;
  auto txn = attr_values_lmdb->new_txn();
  if (interval_flags == BitmapIndexBase::INCLUDE_LEFT || 
      interval_flags == BitmapIndexBase::CLOSED) {
    min_iter = attr_values_lmdb->lower_bound(AttributeValue<T>(
      rg_info->id, column_ref->dotted_path, min), txn);
  } else {
    // Look for the values strictly greater than min.
    min_iter = attr_values_lmdb->upper_bound(AttributeValue<T>(
      rg_info->id, column_ref->dotted_path, min), txn);
  }
  // Make sure we don't access values for the next row group.
  // TODO(martun): not very nice design over here, fix later.
  if (min_iter == attr_values_lmdb->end(txn) || 
      (min_iter->first.rg_id != rg_info->id) || 
      (min_iter->first.column_dotted_path != column_ref->dotted_path)) {
    return aux_data->max_mapped_value;
  }
  return min_iter->second.get();
}

template <class T>
int64_t lookup_min_value(
    typename std::enable_if<!std::is_integral<T>::value, const T&>::type min, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    const BitmapIndexBase::IntervalFlags interval_flags) {
  return lookup_min_value_with_mapping(
      min, rg_info, column_ref, aux_data, attr_values_lmdb, interval_flags);
}

template <class T>
int64_t lookup_min_value(
    typename std::enable_if<std::is_integral<T>::value, const T&>::type min, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    const BitmapIndexBase::IntervalFlags interval_flags) {
  if (aux_data->use_value_mapping) {
    return lookup_min_value_with_mapping(
        min, rg_info, column_ref, aux_data, attr_values_lmdb, interval_flags);
  }

  // Convert to int64_t, may have been bool before.
  int64_t min_value = min;
  if (interval_flags == BitmapIndexBase::INCLUDE_RIGHT || 
      interval_flags == BitmapIndexBase::OPEN) {
    min_value++;
  }
  if (min_value < aux_data->min_mapped_value) {
    return 0;
  }
  return min_value - aux_data->min_mapped_value;
}

template <class T>
int64_t lookup_max_value_with_mapping(
    const T& max, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    const BitmapIndexBase::IntervalFlags interval_flags) {
  auto txn = attr_values_lmdb->new_txn();
  AttributeValue<T> max_value(rg_info->id, column_ref->dotted_path, max);
  typename BitmapIndex<T>::LMDBIterator max_iter = std::move(
      attr_values_lmdb->lower_bound(max_value, txn));
  if (max_iter == attr_values_lmdb->end(txn) || 
      max_iter->first.rg_id != max_value.rg_id || 
      max_iter->first.column_dotted_path != max_value.column_dotted_path) {
    // Set max to the maximal value in the map.
    return aux_data->cardinality - 1;
  }
  // If lower_bound is not the value itself, or we don't want to include values equal to max,
  // then we need to take the previous value.
  if (max_iter->first != max_value || 
      interval_flags == BitmapIndexBase::INCLUDE_LEFT || 
      interval_flags == BitmapIndexBase::OPEN) {
    if (max_iter == attr_values_lmdb->begin(txn)) {
      // All the values are more than max.
      return -1;
    }    
    --max_iter;
    if (max_iter->first.rg_id != rg_info->id || 
        max_iter->first.column_dotted_path != column_ref->dotted_path) {
      // All the values are more than max.
      return -1;
    }
  }
  return max_iter->second.get();
}

template <class T>
int64_t lookup_max_value(
    typename std::enable_if<!std::is_integral<T>::value, const T&>::type min, 
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    const BitmapIndexBase::IntervalFlags interval_flags) {
  return lookup_max_value_with_mapping(
      min, rg_info, column_ref, aux_data, attr_values_lmdb, interval_flags);
}
  
template <class T>
int64_t lookup_max_value(
    typename std::enable_if<std::is_integral<T>::value, const T&>::type max,
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitmapIndexAuxData> aux_data,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    const BitmapIndexBase::IntervalFlags interval_flags) {
  if (aux_data->use_value_mapping) {
    return lookup_max_value_with_mapping(
        max, rg_info, column_ref, aux_data, attr_values_lmdb, interval_flags);
  }
  // Convert to int64_t, may have been bool before.
  int64_t max_value = max;
  if (interval_flags == BitmapIndexBase::INCLUDE_LEFT || 
      interval_flags == BitmapIndexBase::OPEN) {
    --max_value;
  }
  if (max_value > aux_data->max_mapped_value) {
    max_value = aux_data->max_mapped_value;
  }
  return max_value - aux_data->min_mapped_value;
}

} // end of unonymus namespace

template <class T>
std::shared_ptr<BitmapIndexAuxData> BitmapIndex<T>::get_aux_data(
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb) {
  // Load aux_data from LMDB.
  auto txn = aux_data_lmdb->new_txn();
  auto iter = aux_data_lmdb->find(BitmapStorageLMDBID(rg_info->id, column_ref->dotted_path), txn);
  if (iter == aux_data_lmdb->end(txn)) {
    throw std::runtime_error("Bitmap Index auxiliary data not found in the LMDB.");
  }

  return std::make_shared<BitmapIndexAuxData>(std::move(iter->second));
}

template <class T>
std::shared_ptr<BitmapIndex<T>> BitmapIndex<T>::load(
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    std::shared_ptr<BitMapStorage>&& storage,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb) {
  std::shared_ptr<BitmapIndexAuxData> aux_data = get_aux_data(rg_info, column_ref, aux_data_lmdb);

  auto bmp = std::make_shared<BitmapIndex>(
    rg_info, 
    column_ref, 
    std::move(storage), 
    attr_values_lmdb, 
    aux_data
  );
  
  return bmp;
}

template <class T>
Future<std::shared_ptr<BitmapIndex<T>>> BitmapIndex<T>::load(
    std::shared_ptr<RowGroupInfo> rg_info,
    std::shared_ptr<ColumnReference> column_ref,
    Future<std::shared_ptr<BitMapStorage>>&& storage,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
    std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>> aux_data_lmdb) {
  
  return storage.then([=](std::shared_ptr<BitMapStorage>& storage) {  
    std::shared_ptr<BitmapIndexAuxData> aux_data = get_aux_data(rg_info, column_ref, aux_data_lmdb);

    auto bmp = std::make_shared<BitmapIndex>(
      rg_info, 
      column_ref, 
      std::move(storage), 
      attr_values_lmdb, 
      aux_data
    );
  
    return bmp;
  });
}

template <class T>
void BitmapIndex<T>::insert_values_mapping_into_lmdb(
    const std::vector<std::pair<DocumentID, T>>& values, 
    std::unordered_map<T, uint32_t>* values_mapping_out) {
  std::vector<T> attr_values;
  attr_values.reserve(values.size());
  for (auto& pair : values) {
    attr_values.emplace_back(pair.second);
  }
  // TODO(martun): Optimize the sort unique sometime later.
  std::sort(attr_values.begin(), attr_values.end());
  attr_values.erase( std::unique( attr_values.begin(), attr_values.end() ), attr_values.end() );

  std::vector<std::pair<AttributeValue<T>, LMDBValue<uint32_t>>> lmdb_values_mapping;
  for (size_t i = 0; i < attr_values.size(); ++i) {
    values_mapping_out->insert(std::make_pair(attr_values[i], i));
    lmdb_values_mapping.push_back(std::make_pair(
        AttributeValue<T>(rg_info_->id, column_ref_->dotted_path, attr_values[i]), 
        LMDBValue<uint32_t>(i)));
  }

  auto txn = attr_values_lmdb_->new_txn();

  // Insert attribute value mappings into LMDB.
  attr_values_lmdb_->insert(lmdb_values_mapping, txn);
}

template <class T>
std::shared_ptr<BitmapIndex<T>> BitmapIndex<T>::create(
    std::shared_ptr<RowGroupInfo>& rg_info,
    std::shared_ptr<ColumnReference>& column_ref,
    std::shared_ptr<BitMapStorage>&& storage,
    std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>>& attr_values_lmdb,
    std::shared_ptr<LMDBDictionary<BitmapStorageLMDBID, BitmapIndexAuxData>>& aux_data_lmdb,
    std::shared_ptr<BitmapIndexAuxData>& aux_data,
    const std::vector<std::pair<DocumentID, T>>& values)  {
  
  auto index = std::make_shared<BitmapIndex>(
    rg_info, column_ref, std::move(storage), attr_values_lmdb, aux_data);

  index->insert(values);
  
  auto txn_2 = aux_data_lmdb->new_txn(); 

  // Save aux_data in LMDB.
  aux_data_lmdb->insert(BitmapStorageLMDBID(rg_info->id, column_ref->dotted_path), *aux_data, txn_2);

  return index;
}

template <class T>
BitmapIndex<T>::BitmapIndex(
        std::shared_ptr<RowGroupInfo> rg_info,
        std::shared_ptr<ColumnReference> column_ref,
        std::shared_ptr<BitMapStorage>&& storage,
        std::shared_ptr<LMDBDictionary<AttributeValue<T>, LMDBValue<uint32_t>>> attr_values_lmdb,
        std::shared_ptr<BitmapIndexAuxData> aux_data)
  : BitmapIndexBase(rg_info, column_ref, std::move(storage))
  , attr_values_lmdb_(attr_values_lmdb)
  , aux_data_(aux_data)
  , modified_(false) {

}

template <class T>
BitmapIndex<T>::~BitmapIndex() {
}


template <class T>
void BitmapIndex<T>::insert(const DocumentID& row_id, const T& value) {
  insert_mapped_value(
      row_id, get_lmdb_attr_value(value, rg_info_, column_ref_, aux_data_, attr_values_lmdb_));
}

template <class T>
void BitmapIndex<T>::insert_mapped_values(
    std::vector<std::pair<DocumentID, uint64_t>> mapped_values) {
  // Mark that a change has happened.
  modified_ = true;

  std::vector<std::vector<std::vector<DocumentID>>> storage_cache;
  std::vector<DocumentID> all_values_bitmap_cache;
  storage_cache.resize(aux_data_->bitmap_counts.size());
  for (int i = 0; i < aux_data_->bitmap_counts.size(); ++i) {
    storage_cache[i].resize(aux_data_->bitmap_counts[i]);
  }
  
  DocumentID row_id;
  uint64_t mapped_value;
  std::vector<uint32_t> decomposed;
  
  std::vector<uint32_t> basis = aux_data_->vd->get_basis();
  std::vector<uint32_t> m_values;
  uint32_t basis_size = basis.size();
  for (uint32_t i = 0; i < basis_size; ++i) {
        m_values.push_back(basis[i] / 2 - 1);
  }
  uint32_t mapped_values_size = mapped_values.size();
  for (int k = 0; k < mapped_values_size; ++k) {
    row_id = mapped_values[k].first;

    aux_data_->vd->decompose(decomposed, mapped_values[k].second);
    all_values_bitmap_cache.emplace_back(row_id);

    if (aux_data_->enc_type == BitmapIndexEncodingType::EQUALITY) {
      for (size_t i = 0; i < basis_size; ++i) {
        storage_cache[i][decomposed[i]].emplace_back(row_id);
      }
    }
    else if (aux_data_->enc_type == BitmapIndexEncodingType::INTERVAL) {
      uint32_t start_of_range;
      uint32_t end_of_range;
      for (uint32_t i = 0; i < basis_size; ++i) {
        start_of_range = 0;
        if (decomposed[i] > m_values[i]) {
          start_of_range = decomposed[i] - m_values[i];
        }
        end_of_range = std::min(decomposed[i], aux_data_->bitmap_counts[i] - 1);
        for (uint32_t j = start_of_range; j <= end_of_range; ++j) {
          storage_cache[i][j].emplace_back(row_id);
        }
      }
    }
    else if (aux_data_->enc_type == BitmapIndexEncodingType::RANGE) {
      for (uint32_t i = 0; i < basis_size; ++i) {
        const uint32_t base = aux_data_->vd->get_base(i);
        for (uint32_t j = decomposed[i]; j + 2 <= base; ++j) {
          storage_cache[i][j].emplace_back(row_id);
        }
      }
    }
  }
  
  storage_->add_to_all_values_bitmap(all_values_bitmap_cache);

  // Move the values from storage_cache into storage_.
  for (int i = 0; i < aux_data_->bitmap_counts.size(); ++i) {
    for (int j = 0; j < aux_data_->bitmap_counts[i]; ++j) {
      storage_->add_to_bitmap(i, j, storage_cache[i][j]);
    }
  }
}

template <class T>
void BitmapIndex<T>::insert_mapped_value(const DocumentID& row_id, uint64_t mapped_value) {
  // Mark that a change has happened.
  modified_ = true;
  std::vector<uint32_t> decomposed;
  
  aux_data_->vd->decompose(decomposed, mapped_value);
  storage_->add_to_all_values_bitmap(row_id);

  if (aux_data_->enc_type == BitmapIndexEncodingType::EQUALITY) {
    for (size_t i = 0; i < decomposed.size(); ++i) {
      storage_->add_to_bitmap(i, decomposed[i], row_id);
    }
  }
  else if (aux_data_->enc_type == BitmapIndexEncodingType::INTERVAL) {
    for (uint32_t i = 0; i < decomposed.size(); ++i) {
      const uint32_t base = aux_data_->vd->get_base(i);
      const uint32_t m = base / 2 - 1;
      const uint32_t start_of_range = std::max(decomposed[i] - m, (uint32_t)0);
      const uint32_t end_of_range = std::min(decomposed[i], base - m - 1);
      for (uint32_t j = start_of_range; j <= end_of_range; ++j) {
        storage_->add_to_bitmap(i, j, row_id);
      }
    }
  }
  else if (aux_data_->enc_type == BitmapIndexEncodingType::RANGE) {
    for (uint32_t i = 0; i < decomposed.size(); ++i) {
      const uint32_t base = aux_data_->vd->get_base(i);
      for (uint32_t j = decomposed[i]; j + 2 <= base; ++j) {
        storage_->add_to_bitmap(i, j, row_id);
      }
    }
  }
}

template <class T>
void BitmapIndex<T>::insert(const std::vector<std::pair<DocumentID, T>>& values) {
  std::unordered_map<T, uint32_t> values_mapping;
  if (aux_data_->use_value_mapping) {
    insert_values_mapping_into_lmdb(values, &values_mapping);
  }
  // All the values mapped as necessary.
  std::vector<std::pair<DocumentID, uint64_t>> mapped_values;
  mapped_values.reserve(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    if (!aux_data_->use_value_mapping) {
      mapped_values.emplace_back(
        values[i].first, 
        // Next line is not actually looking-up LMDB, I call it just for compiling 
        // reasons for T=std::string.
        get_lmdb_attr_value(
            values[i].second, rg_info_, column_ref_, aux_data_, attr_values_lmdb_));
    } else {
      auto iter = values_mapping.find(values[i].second);
      if (iter == values_mapping.end()) {
        throw "Did not find the proper mapping value for given attribute value.";
      }

      mapped_values.emplace_back(values[i].first, iter->second);
    }
  }
  insert_mapped_values(mapped_values);
}

template <class T>
FUTURE_BMP BitmapIndex<T>::lookup(const T& value) const {
  try {
    int64_t mapped_value = get_lmdb_attr_value(
        value, rg_info_, column_ref_, aux_data_, attr_values_lmdb_);
    return std::move(lookup_mapped_value(mapped_value));
  } catch (std::invalid_argument not_found_exception) {
    // If the looked-up values was not found, return an empty bitmap.
    return makeFuture(std::make_shared<BitMap>());
  }
}

template <class T>
FUTURE_BMP BitmapIndex<T>::not_equals(const T& value) const {
  std::vector<FUTURE_CONST_BMP> bitmaps_needed;
  bitmaps_needed.push_back(lookup(value));
  bitmaps_needed.push_back(storage_->load_all_values_bitmap_const());
  return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
    std::shared_ptr<BitMap> res(new BitMap(*bmps[1]));
    *res -= *bmps[0];
    return res;
  });
}

template <class T>
FUTURE_BMP BitmapIndex<T>::get_not_null() const {
  return storage_->load_all_values_bitmap();
}

template <class T>
FUTURE_BMP BitmapIndex<T>::lookup_mapped_value(int64_t value) const {
  if (value < 0 || value > aux_data_->max_mapped_value - aux_data_->min_mapped_value) {
    return std::move(makeFuture(std::make_shared<BitMap>()));
  }
  std::vector<uint32_t> decomposed;
  aux_data_->vd->decompose(decomposed, value);

  std::vector<FUTURE_BMP> futures;
  for (size_t i = 0; i < decomposed.size(); ++i) {
    futures.push_back(std::move(get_equality_bitmap(i, decomposed[i])));
  }

  // NOTE(martun): synchronous version used to keep in memory no more than 2 bitmaps at a time.
  // Hopefully we have enough memory to keep them all.
  return collect(futures).then([=](SharedBitmapVector& bitmaps) {
    auto result = bitmaps[0];
    for (uint32_t i = 1; i < bitmaps.size(); ++i) {
      // Take intersection of equality bitmaps for each component.
      *result &= *bitmaps[i];
    }
    return result;
  });
}

template <class T>
FUTURE_BMP BitmapIndex<T>::get_equality_bitmap(uint32_t i, uint32_t attribute_i) const {
  if (aux_data_->enc_type == BitmapIndexEncodingType::EQUALITY) {
    return storage_->load_bitmap(i, attribute_i);
  }
  else if (aux_data_->enc_type == BitmapIndexEncodingType::INTERVAL) {
    return get_equality_bitmap_interval_encoding(i, attribute_i);
  }
  else if (aux_data_->enc_type == BitmapIndexEncodingType::RANGE) {
    return get_equality_bitmap_range_encoding(i, attribute_i);
  }
  throw std::invalid_argument("Invalid encoding type for loading equality bitmap.");
}

template <class T>
FUTURE_BMP BitmapIndex<T>::get_equality_bitmap_range_encoding(uint32_t i, uint32_t attribute_i) const {
  const uint32_t base = aux_data_->vd->get_base(i);
  if (base == 1 && attribute_i == 0) {
    // Return a full bitmap, all the values are equal.
    return storage_->load_all_values_bitmap();
  } else if (attribute_i == base - 1) {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, base - 2));
    bitmaps_needed.push_back(storage_->load_all_values_bitmap_const());
    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[1]));
      *res -= *bmps[0];
      return res;
    });
  } else if (attribute_i == 0) {
    return storage_->load_bitmap(i, 0);
  } else {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i - 1));
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i));
    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[1]));
      *res -= *bmps[0];
      return res;
    });
  }
}

template <class T>
FUTURE_BMP BitmapIndex<T>::get_equality_bitmap_interval_encoding(uint32_t i, uint32_t attribute_i) const {
  const uint32_t base = aux_data_->vd->get_base(i);
  const uint32_t m = base / 2 - 1;

  // This will happen only if cardinality = 1, I.E. all element in the DB are equal.
  if (base == 1) { 
    if (attribute_i == 0) {
      // Return a full bitmap.
      return storage_->load_all_values_bitmap();
    } else {
      // Return an empty bitmap.
      return makeFuture(std::make_shared<BitMap>());
    } 
  }
  // Following rules are according to "sigmod99_pp215-226.pdf".
  if (attribute_i == 0 && m == 0) {
    return storage_->load_bitmap(i, 0);
  } else if (base == 2 && attribute_i == 1) {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, 0));
    bitmaps_needed.push_back(storage_->load_all_values_bitmap_const());
    
    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[1]));
      *res -= *bmps[0];
      return res;
    });
  }
  else if (base == 3 && attribute_i == 1) {
    return storage_->load_bitmap(i, 1);
  }
  else if (attribute_i < m) {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i));
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i + 1));
    
    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
      *res -= *bmps[1];
      return res;
    });
  }
  else if (attribute_i == m && m > 0) {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i));
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, 0));
    
    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
      *res &= *bmps[1];
      return res;
    });
  }
  else if (attribute_i > m && attribute_i < base - 1 && m > 0) {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i - m));
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i - m - 1));
    
    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
      *res -= *bmps[1];
      return res;
    });
  }
  else if (attribute_i == base - 1) {
    std::vector<FUTURE_CONST_BMP> bitmaps_needed;
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, base / 2 + base % 2 - 1));
    bitmaps_needed.push_back(storage_->load_const_bitmap(i, 0));
    bitmaps_needed.push_back(storage_->load_all_values_bitmap_const());

    return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
      *res |= *bmps[1];

      // Flip bitmap res.
      std::shared_ptr<BitMap> all_vals(new BitMap(*bmps[2]));
      *all_vals -= *res;
      return all_vals;
    });
  }
  // This must never happen, all the cases are covered.
  throw "System error in function LookUp while trying to LookUp a value in bitmap index.";
}

template <class T>
FUTURE_BMP BitmapIndex<T>::get_lesser_bitmap(int i, uint32_t attribute_i) const {
  if (attribute_i == 0) {
    // return an empty bitmap.
    return makeFuture(std::make_shared<BitMap>());
  }
  // Decrement value of "attribute_i", so we can run less or equal query.
  attribute_i--;

  if (aux_data_->enc_type == BitmapIndexEncodingType::RANGE) {
    return storage_->load_bitmap(i, attribute_i);
  } else if (aux_data_->enc_type == BitmapIndexEncodingType::INTERVAL) {
    const uint32_t base = aux_data_->vd->get_base(i); 
    const uint32_t m = base / 2 - 1;
  
    // Following rules are according to "sigmod99_pp215-226.pdf".
    if (attribute_i == 0) {
      return get_equality_bitmap(i, attribute_i);
    } else if (attribute_i < m) {
      std::vector<FUTURE_CONST_BMP> bitmaps_needed;
      bitmaps_needed.push_back(storage_->load_const_bitmap(i, 0));
      bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i + 1));

      return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
        std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
        *res -= *bmps[1];
        return res;
      });
    }
    else if (attribute_i == m) {
      return storage_->load_bitmap(i, 0);
    }
    else if (attribute_i > m && attribute_i < base - 1) {
      std::vector<FUTURE_CONST_BMP> bitmaps_needed;
      bitmaps_needed.push_back(storage_->load_const_bitmap(i, 0));
      bitmaps_needed.push_back(storage_->load_const_bitmap(i, attribute_i - m));

      return collect(bitmaps_needed).then([=](SharedConstBitmapVector& bmps) {
        std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
        *res |= *bmps[1];
        return res;
      });
    }
    else if (attribute_i >= base - 1) {
      // Return a full bitmap.
      return storage_->load_all_values_bitmap();
    }
  }
  // This must never happen, all the cases are covered.
  throw "System error in function LookUp while trying to LookUp a value in bitmap index.";
}

template <class T>
FUTURE_BMP BitmapIndex<T>::range_search(
    const T& min, 
    const T& max,
    const IntervalFlags interval_flags) {
  if (min > max) {
    throw std::invalid_argument("Invalid range for LookUp in the Bitmap Index.");
  }
  int64_t v1 = lookup_min_value(
      min, rg_info_, column_ref_, aux_data_, attr_values_lmdb_, interval_flags);
  if (aux_data_->use_value_mapping && v1 == aux_data_->cardinality) {
    // All the values in map are less than min, so return an empty bitmap.
    return std::move(makeFuture(std::make_shared<BitMap>()));
  }
  int64_t v2 = lookup_max_value(
      max, rg_info_, column_ref_, aux_data_, attr_values_lmdb_, interval_flags);
  if (aux_data_->use_value_mapping && v2 == -1) {
    // All the values in map are > than max, so return an empty bitmap.
    return std::move(makeFuture(std::make_shared<BitMap>()));
  }

  // Now the range to look for is [v1, v2].
  if (v1 > v2) {
    // Return an empty bitmap.
    return std::move(makeFuture(std::shared_ptr<BitMap>(new BitMap())));
  }
  else if (v1 == v2) {
    return std::move(lookup_mapped_value(v1));
  }
  else {
    if (aux_data_->enc_type == BitmapIndexEncodingType::EQUALITY) {
      throw std::invalid_argument(
        "Unable to perform range query over an equality Bitmap Index.");
    } else {
      return std::move(range_search_internal(v1, v2));
    }
  }
}

template <class T>
FUTURE_BMP BitmapIndex<T>::lesser(const T& max_value,	const IntervalFlags interval_flags) {
  int64_t v2 = lookup_max_value(
      max_value, rg_info_, column_ref_, aux_data_, attr_values_lmdb_, interval_flags);
  if (aux_data_->use_value_mapping && v2 == -1) {
    // All the values in map are > than max_value, so return an empty bitmap.
    return std::move(makeFuture(std::make_shared<BitMap>()));
  }
  if (aux_data_->enc_type == BitmapIndexEncodingType::EQUALITY) {
      throw std::invalid_argument(
        "Unable to perform lesser query over an equality Bitmap Index.");
  } 
  return std::move(less_or_equal_query(v2));
}

template <class T>
FUTURE_BMP BitmapIndex<T>::greater(const T& min,	const IntervalFlags interval_flags) {
  if (aux_data_->enc_type == BitmapIndexEncodingType::EQUALITY) {
      throw std::invalid_argument(
        "Unable to perform lesser query over an equality Bitmap Index.");
  }

  int64_t v1 = lookup_min_value(
      min, rg_info_, column_ref_, aux_data_, attr_values_lmdb_, interval_flags);
  if (v1 == aux_data_->cardinality) {
    // All the values in map are less than min, so return an empty bitmap.
    return std::move(makeFuture(std::make_shared<BitMap>()));
  }

  FUTURE_BMP less_v1 = std::move(makeFuture(std::make_shared<BitMap>()));
  if (v1 != 0) {
    less_v1 = std::move(less_or_equal_query(v1 - 1));
  }
  std::vector<FUTURE_CONST_BMP> futures;
  futures.push_back(storage_->load_all_values_bitmap_const());
  futures.push_back(std::move(less_v1));
  return collect(futures).then([=](SharedConstBitmapVector& bmps) {
    std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
    *res -= *bmps[1];
    return res;
  });
}

template <class T>
FUTURE_BMP BitmapIndex<T>::range_search_internal(int64_t v1, int64_t v2) {
  FUTURE_BMP less_v2 = less_or_equal_query(v2);
  if (v1 == 0) {
    return std::move(less_v2);
  }
  FUTURE_BMP less_v1 = less_or_equal_query(v1 - 1);
  std::vector<FUTURE_BMP> futs;
  futs.push_back(std::move(less_v2));
  futs.push_back(std::move(less_v1));
  return std::move(
    collect(futs).then([=](
      SharedBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
      *res -= *bmps[1];
      // TODO(martun): Optimize for the cases where for example v1 and v2 
      // have the same first decomposed value.
      return res;
    }));
}

template <class T>
FUTURE_BMP BitmapIndex<T>::less_or_equal_query(int64_t upper_bound) {
  if (upper_bound < 0) // We always map the minimal value to 0.
    return std::move(makeFuture(std::make_shared<BitMap>()));
  if (upper_bound >= aux_data_->max_mapped_value - aux_data_->min_mapped_value) 
    return std::move(storage_->load_all_values_bitmap());

  std::vector<uint32_t> decomposed;
  aux_data_->vd->decompose(decomposed, upper_bound);

  std::vector<FUTURE_BMP> bitmaps_needed;
  for (size_t i = 0; i < decomposed.size(); ++i) {
    bitmaps_needed.push_back(get_lesser_bitmap(i, decomposed[i]));
    bitmaps_needed.push_back(get_equality_bitmap(i, decomposed[i]));
  }
  return collect(bitmaps_needed).then([=](SharedBitmapVector& bmps) {
      std::shared_ptr<BitMap> res(new BitMap(*bmps[0]));
      std::shared_ptr<BitMap> eq(new BitMap(*bmps[1]));
      for (size_t i = 1; i < decomposed.size(); ++i) {
        std::shared_ptr<BitMap> less(new BitMap(*bmps[2 * i]));
        *less &= *eq;
        *res |= *less;
        *eq &= *bmps[2 * i + 1];
      }
      // Add the equality.
      *res |= *eq;
      return res;
    });
}

template <class T>
void BitmapIndex<T>::reset_usage_frequencies() {
  storage_->reset_usage_frequencies();
}


template <class T>
uint32_t BitmapIndex<T>::get_total_bitmaps_number() const {
  uint32_t total = 0;
  for (uint32_t& size : aux_data_->bitmap_counts) {
    total += size;
  }
  return total;
}
