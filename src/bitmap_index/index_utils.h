// This file will contain utility classes/enums, etc, used from different parts of the bitmap index.
#ifndef _INDEX_UTILS_H__
#define _INDEX_UTILS_H__

enum BitmapIndexEncodingType {
    EQUALITY = 0x00,
    INTERVAL = 0x01,
    RANGE = 0x02
};

enum BitmapIndexType {
  BITMAP = 1, 
  BITSLICED = 2
};

#endif  // _INDEX_UTILS_H__
