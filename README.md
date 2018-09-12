## bitmap_index
Implementation of bitmap indexes for document databases with different encodings. We support EQUALITY, RANGE and INTERVAL indexes with attribute value decomposition in a custom basis(I.E. you can use it as a BITSLICE index as it's done in Pilosa or as a simple BITMAP index.). Uses LMDB as a cache.

## Building
Build project:

    cd /path/to/project
    mkdir build
    cd build
    cmake ..
    make -j4

Debug Build: 

    cd /path/to/project
    mkdir build_debug
    cd build_debug
    cmake -DCMAKE_BUILD_TYPE=Debug ..
    make -j4

# Third parties
All the necessary third party libraries are included, code is tested on Ubuntu 16.04 only for now. For another system you may need to compile with folly yourself.

## Testing
We use [Google Test](https://github.com/google/googletest) for unit testing.

To run the tests:
    make test

## Contact e-mail
martun.karapetyan@gmail.com

## Articles

Below is the list of relevant articles and papers: \
[Description of interval encoding for bitmap indexes.](http://www.madgik.di.uoa.gr/sites/default/files/sigmod99_pp215-226.pdf) \
[Roaring bitmaps for better compression.](https://arxiv.org/pdf/1402.6407.pdf) \
[Keynote: About Bitmap Indexes](https://www.iaria.org/conferences2013/filesDBKDA13/keynote-dbkda-2013-final.pdf) \
[Description of what a bitmap index is.](http://dsl.cds.iisc.ac.in/~course/DBMS/class/bitmap/bitmap.pdf) \
[LMDB database, good for a memory cache.](https://github.com/LMDB/lmdb)
