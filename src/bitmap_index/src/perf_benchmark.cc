#include <iostream>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <random>
#include <string>
#include <math.h>
#include <sstream>
#include <fstream>
#include <memory>
#include <unistd.h>
#include <chrono>

// Included for getrusage.
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "bitmap_index.h"
#include "roaring/roaring.h"

enum Distribution {
	UNIFORM = 0,
	GAUS = 1,
	LOG = 2
};

static const std::string benchmark_dir = "./benchmarks";

/*
// Generates some random values, later on these will be the values used in the index.
void generate_values(uint32_t cardinality, std::vector<uint32_t>& values) {
	int new_value = 0;
	// Using a fixed seed value for random value generation, such that the same
	// values are generated on each call.
	std::mt19937 gen(12345678);

	for (uint32_t i = 0; i < cardinality; ++i) {
		new_value += gen() % 10000;
		values.push_back(new_value);
	}
}

void generate_and_insert_row_id_value_pairs(
		std::unique_ptr<BitmapIndex<uint32_t>>& index, 
		const std::vector<uint32_t>& values, 
		uint32_t number_of_values, 
		Distribution dist,
		uint32_t next_row_id_distance) {
	// Using a fixed seed value for random value generation, such that the same
	// values are generated on each call.
	std::mt19937 gen(12345678);
	uint32_t next_row_id = 1;
	switch (dist) {
		case UNIFORM:
			{
				std::uniform_int_distribution<uint32_t> d(0, values.size() - 1);
				for (uint32_t i = 0; i < number_of_values; ++i) {
					next_row_id += gen() % next_row_id_distance;
					index->insert(values[d(gen)], next_row_id);
				}
				break;
			}
		case GAUS:
			{
				std::normal_distribution<> d(values.size() / 2, values.size() / 6);
				for (uint32_t i = 0; i < number_of_values; ++i) {
					int32_t value = std::round(d(gen));
					if (value < 0) {
						value = 0;
					}
					if ((uint32_t)value >= values.size()) {
						value = values.size() - 1;
					}
					next_row_id += gen() % next_row_id_distance;
					index->insert(values[value], next_row_id);
				}
				break;
			}
		case LOG:
			{
				std::geometric_distribution<> d(0.0005);
				for (uint32_t i = 0; i < number_of_values; ++i) {
					int32_t value = std::round(d(gen));
					if (value < 0) {
						value = 0;
					}
					if ((uint32_t)value >= values.size()) {
						value = values.size() - 1;
					}
					next_row_id += gen() % next_row_id_distance;
					index->insert(values[value], next_row_id);
				}
				break;
			}
	}
}

std::string distribution_to_string(Distribution dist) {
	switch (dist) {
		case UNIFORM:
			return "UNIFORM";
		case GAUS:
			return "GAUSS";
		case LOG:
			return "LOG";
	}
	throw "Unable to convert distribution value to string.";
}

std::string encoding_type_to_string(BitmapIndex<uint32_t>::EncodingType enc) {
	switch (enc) {
		case BitmapIndex<uint32_t>::EncodingType::EQUALITY:
			return "EQUALITY";
		case BitmapIndex<uint32_t>::EncodingType::INTERVAL:
			return "INTERVAL";
		case BitmapIndex<uint32_t>::EncodingType::RANGE:
			return "RANGE";
	}
	throw "Unable to convert encoding type to string.";
}

std::string get_folder_size(const std::string& folder_path) {
	// Get total file size of all the bitmaps, I.E. size of the folder containing them all.
	std::ostringstream command;
	command << "du -ch " << folder_path << " | grep total";
	FILE* pf = popen(command.str().c_str(), "r");
	if (!pf) {
		std::ostringstream res;
		res << "ERROR: " << strerror(errno);
		return res.str(); 
	}
	char buffer[100];
	if (!fgets(buffer, 100, pf)) {
		std::ostringstream res;
		res << "ERROR: " << strerror(errno);
		return res.str(); 
	}
	fclose(pf);	
	std::stringstream ss;
	ss.write(buffer, strlen(buffer));
	std::string res;
	ss >> res;
	return res;
}

void run_perf_test_create_bitmap_index(
		std::ofstream& file, int number_of_values, int cardinality, 
		uint32_t bitmap_cache_size, Distribution distribution, 
		BitmapIndex<uint32_t>::EncodingType enc_type, 
		const std::string& path, 
		uint32_t& total_bitmap_number_out,
		std::vector<uint32_t>& values_out,
		uint32_t& cpu_time_out,
		uint32_t& wall_time_out, 
		uint32_t& disk_blocks_read,
		uint32_t& disk_blocks_written) {
	struct rusage usage_start;
	{
		int ret = getrusage(RUSAGE_SELF, &usage_start);
		assert(ret == 0);
		(void) ret;
	}

	uint64_t start_cpu_time = clock();
	auto start_wall_time = std::chrono::high_resolution_clock::now();

	generate_values(cardinality, values_out);

	// Keep all the bitmaps in memory for now, if we use the
	// value of bitmap_cache_size, creation is too slow.
	// This can be optimized by storing a few last values to add
	// to each bitmap, and add them in batches. 
	// TODO(martun): optimize insertion.
	auto index = std::make_unique<BitmapIndex<uint32_t>>(
			path, values_out, INT_MAX, enc_type);
	
	generate_and_insert_row_id_value_pairs(
		index, values_out, number_of_values, distribution, 4);

	// Reset bitmap usage frequencies, because we want to 
	// store in memory only those bitmaps, that are 
	// most frequently accessed during reads, not while 
	// creating the bitmap.
	index->reset_usage_frequencies();

	total_bitmap_number_out = index->get_total_bitmaps_number();

	// Force write all the bitmaps to disk
	index.reset(nullptr);
	
	cpu_time_out = clock() - start_cpu_time; 
	std::chrono::duration<double, std::micro> diff = std::chrono::high_resolution_clock::now() - start_wall_time;
	wall_time_out = diff.count();
	struct rusage usage_end;
	{
		int ret = getrusage(RUSAGE_SELF, &usage_end);
		assert(ret == 0);
		(void) ret;
	}
	disk_blocks_read = usage_end.ru_inblock - usage_start.ru_inblock;
	disk_blocks_written = usage_end.ru_oublock - usage_start.ru_oublock;
}

void run_perf_test_for_range_queries(
		const std::string& path,
		std::vector<uint32_t>& values, 
		uint32_t bitmap_cache_size,
		uint32_t& cpu_time_out,
		uint32_t& wall_time_out,
		uint32_t& disk_blocks_read,
		uint32_t& disk_blocks_written) {
	struct rusage usage_start;
	{
		int ret = getrusage(RUSAGE_SELF, &usage_start);
		assert(ret == 0);
		(void)ret;
	}

	uint64_t start_cpu_time = clock();
	auto start_wall_time = std::chrono::high_resolution_clock::now();

	auto index = std::make_unique<BitmapIndex<uint32_t>>(path, bitmap_cache_size);

	// Total cardinality is computed, to make sure compiler does not optimize out this part of code.
	uint32_t total_cardinality = 0;

	// Using a fixed seed value for random value generation, such that the same
	// values are generated on each call.
	std::mt19937 gen(12345678);
	std::uniform_int_distribution<uint32_t> d(0, values.size() - 1);
	uint32_t query_count = 1000;
	for (uint32_t i = 0; i < query_count; ++i) {
		uint32_t range_start = values[d(gen)];
		uint32_t range_end = values[d(gen)];
		if (range_end < range_start) {
			std::swap(range_start, range_end);
		}
		std::shared_ptr<BitMap> res_bmp = index->rangeSearch(
				range_start, range_end,
				BitmapIndex<uint32_t>::IntervalFlags::CLOSED).get();
		total_cardinality += res_bmp->cardinality();
	}
	cpu_time_out = (clock() - start_cpu_time) / query_count;
	std::chrono::duration<double, std::micro> diff = 
		std::chrono::high_resolution_clock::now() - start_wall_time;
	wall_time_out = diff.count() / query_count;
	
	struct rusage usage_end;
	{
		int ret = getrusage(RUSAGE_SELF, &usage_end);
		assert(ret == 0);
		(void)ret;
	}
	disk_blocks_read = usage_end.ru_inblock - usage_start.ru_inblock;
	disk_blocks_written = usage_end.ru_oublock - usage_start.ru_oublock;
}

void run_perf_test_for_equality_queries(
		const std::string& path,
		std::vector<uint32_t>& values, 
		uint32_t bitmap_cache_size,
		uint32_t& cpu_time_out,
		uint32_t& wall_time_out,
		uint32_t& disk_blocks_read,
		uint32_t& disk_blocks_written) {
	struct rusage usage_start;
	{
		int ret = getrusage(RUSAGE_SELF, &usage_start);
		assert(ret == 0);
		(void)ret;
	}

	auto index = std::make_unique<BitmapIndex<uint32_t>>(path, bitmap_cache_size);
	// Total cardinality is computed, to make sure compiler does not optimize out this part of code.
	uint32_t total_cardinality = 0;

	// Using a fixed seed value for random value generation, such that the same
	// values are generated on each call.
	std::mt19937 gen(12345678);
	std::uniform_int_distribution<uint32_t> d(0, values.size() - 1);

	// Perform some equality queries.
	uint32_t start_cpu_time = clock();
	auto start_wall_time = std::chrono::high_resolution_clock::now();
	uint32_t query_count = 1000;
	for (uint32_t i = 0; i < query_count; ++i) {
		uint32_t search_value = values[d(gen)];
		std::shared_ptr<BitMap> res_bmp = index->lookup(search_value).get();
		total_cardinality += res_bmp->cardinality();
	}

	cpu_time_out = (clock() - start_cpu_time) / query_count;
	std::chrono::duration<double, std::micro> diff = 
		std::chrono::high_resolution_clock::now() - start_wall_time;
	wall_time_out = diff.count() / query_count;
	std::cout << "total_cardinality=" << total_cardinality << "\n";

	struct rusage usage_end;
	{
		int ret = getrusage(RUSAGE_SELF, &usage_end);
		assert(ret == 0);
		(void)ret;
	}
	disk_blocks_read = usage_end.ru_inblock - usage_start.ru_inblock;
	disk_blocks_written = usage_end.ru_oublock - usage_start.ru_oublock;
}
	
void run_perf_test(
		std::ofstream& file, int number_of_values, int cardinality, 
		uint32_t bitmap_cache_size, Distribution distribution, 
		BitmapIndex<uint32_t>::EncodingType enc_type) {
	std::ostringstream ss;
	ss << benchmark_dir + "/benchmark_";
	ss << number_of_values << "_" << cardinality << "_" 
		<< distribution_to_string(distribution) << "_" 
		<< encoding_type_to_string(enc_type) << "_" 
		<< bitmap_cache_size;

	std::string folder_path = ss.str();

	struct stat st = {0};
	// Create the folder.
	if (stat(folder_path.c_str(), &st) == -1) {
		mkdir(folder_path.c_str(), 0700);
	}
	ss << "/bitmap_";
	uint32_t cpu_time, wall_time;
	uint32_t disk_blocks_read, disk_blocks_written;
	std::vector<uint32_t> values;
	uint32_t total_bitmap_number;
	run_perf_test_create_bitmap_index(file, number_of_values, cardinality, 
			bitmap_cache_size, distribution, enc_type, ss.str(), 
			total_bitmap_number, values, cpu_time, wall_time, 
			disk_blocks_read, disk_blocks_written);

	file << number_of_values << ", " 
		<< cardinality << ", "
		<< total_bitmap_number << ", "
		<< distribution_to_string(distribution) << ", "
		<< encoding_type_to_string(enc_type) << ", "
		<< bitmap_cache_size << ", ";

	file << cpu_time << ", " << wall_time << ", " 
		<< disk_blocks_read << ", " << disk_blocks_written << ", ";
	if (enc_type != BitmapIndex<uint32_t>::EncodingType::EQUALITY) {
		run_perf_test_for_range_queries(
			ss.str(), values, bitmap_cache_size, cpu_time, wall_time, 
			disk_blocks_read, disk_blocks_written);
		file << cpu_time << ", " << wall_time << ", "
			<< disk_blocks_read << ", " << disk_blocks_written << ", ";
	} else {
		file << "0, 0, ";
	}
	run_perf_test_for_equality_queries(
		ss.str(), values, bitmap_cache_size, cpu_time, wall_time, 
		disk_blocks_read, disk_blocks_written);
	file << cpu_time << ", " << wall_time << ", "
		<< disk_blocks_read << ", " << disk_blocks_written << ", ";
	file << "'" << get_folder_size(folder_path) << "'\n";
	file.flush();
}

int main()
{
	{
		// create the top-level folder if it does not exist
		struct stat st = {0};
		if (stat(benchmark_dir.c_str(), &st) == -1) {
			int ret = mkdir(benchmark_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH);
			assert(ret == 0);
			(void) ret;
		}
	}
	std::vector<uint32_t> number_of_entries = {8000, 16000, 32000};
	std::vector<uint32_t> cardinalities = {2, 8, 16, 64, 128, 256};
	std::vector<uint32_t> cache_sizes = {0, 2, 4, 8, 16};
	std::vector<Distribution> dists = {UNIFORM, GAUS, LOG};
	std::vector<BitmapIndex<uint32_t>::EncodingType> encodings = {
		BitmapIndex<uint32_t>::EncodingType::INTERVAL,
		BitmapIndex<uint32_t>::EncodingType::RANGE,
		BitmapIndex<uint32_t>::EncodingType::EQUALITY
	};
	
	std::ofstream file("PerfTestResult.csv", std::ofstream::out);
	file << "Row count, Cardinality, Number of bitmaps, Distribution, Encoding, # of bitmaps cached, "
		<< "Creation CPU time, Creation WALL time, Disk Blocks Read on create, Disk Block Written on Create, "
		<< "Range Query CPU Time, Range Query Wall Time, Disk Blocks Read during range queries, Disk Block Written during range queries, "
		<< "Equality Query CPU Time, Equality Query WALL Time, Disk Blocks Read during EQ queries, Disk Block Written during EQ queries, "
	        << "Total File Size \n";

	for (uint32_t i1 = 0; i1 < number_of_entries.size(); ++i1) {
		for (uint32_t i2 = 0; i2 < cardinalities.size(); ++i2) {
			for (uint32_t i3 = 0; i3 < dists.size(); ++i3) {
				for (uint32_t i4 = 0; i4 < encodings.size(); ++i4) {
					for (uint32_t i5 = 0; i5 < cache_sizes.size(); ++i5) {
						try {
							std::cout << "Starting performance test for " 
								<< number_of_entries[i1] << " " << cardinalities[i2] << " "
								<< cache_sizes[i5] << " " << dists[i3] << " " 
								<< encodings[i4] << ".\n";

							run_perf_test(file, number_of_entries[i1], cardinalities[i2], 
									cache_sizes[i5], dists[i3], encodings[i4]);
						} catch (const std::exception& e) {
							std::cout << "Performance test " 
								<< number_of_entries[i1] << " " << cardinalities[i2] << " "
								<< cache_sizes[i5] << " " << dists[i3] << " " 
								<< encodings[i4] << " failed with error="
								<< e.what() << std::endl;
						}
					}
				}
			}
		}
	}
	file.close();
	return 0;
}
*/

int main ()
{
    return 0;
}
