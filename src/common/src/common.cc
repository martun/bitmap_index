#include <iosfwd>
#include <fstream>
#include <arpa/inet.h>

#include "common.h"
#include "toolkit.h"



/************************* SnapshotRange *************************/
SnapshotRange::SnapshotRange()
        : min_snapshot_id(0)
        , max_snapshot_id(0)
{ }

SnapshotRange::SnapshotRange(SnapshotID min_snapshot_id, SnapshotID max_snapshot_id)
        : min_snapshot_id(min_snapshot_id)
        , max_snapshot_id(max_snapshot_id)
{ }

bool SnapshotRange::intersects(const SnapshotRange& other) const
{
    return (min_snapshot_id >= other.min_snapshot_id && min_snapshot_id <= other.max_snapshot_id) ||
           (max_snapshot_id >= other.min_snapshot_id && max_snapshot_id <= other.max_snapshot_id);
}

bool operator == (const SnapshotRange& lhs, const SnapshotRange& rhs) {
    return lhs.min_snapshot_id == rhs.min_snapshot_id && lhs.max_snapshot_id == rhs.max_snapshot_id;
}

/************************* RowGroupInfo *************************/
RowGroupInfo::RowGroupInfo()
        : id(-1)
        , num_docs(0)
{ }

RowGroupInfo::RowGroupInfo(RowGroupID id, int32_t num_docs)
        : id(id)
        , num_docs(num_docs)
{ }

/************************* BatchInfo *************************/
BatchInfo::BatchInfo()
        : id(-1)
        , batch_size(0)
{ }

BatchInfo::BatchInfo(BatchID id,
        SnapshotRange range,
        const std::vector<RowGroupInfo>& rg_infos)
        : id(id)
        , snapshot_range(range)
        , rg_info(rg_infos)
{ }

/************************* MetabatchInfo *************************/
MetabatchInfo::MetabatchInfo(const std::string& metabatch_path,
        MetabatchID id,
        const std::vector<BatchInfo>& batch_infos)
        : metabatch_path(metabatch_path)
        , id(id)
        , state(MetabatchInfo::State::New)
        , index_state(MetabatchInfo::IndexState::NotIndexed)
        , leaf_id(0)
        , batch_infos(batch_infos)
{ }

const BatchInfo* MetabatchInfo::find_batch_info(BatchID batch_id) const
{
    if (batch_id < batch_infos.size()) {
        return &batch_infos[batch_id];
    }
    return NULL;
}

void MetabatchInfo::save() const
{
    std::ofstream out;
    std::string metabatch_info_path = MetabatchInfo::metabatch_info_path(metabatch_path);
    out.open(metabatch_info_path, std::ios::binary);

    out.write((const char*)(&id), sizeof(id));
    // Number of batches in meta-batch
    size_t batch_count = batch_infos.size();
    out.write((const char*)(&batch_count), sizeof(batch_count));

    // Dump each batch
    for (const BatchInfo& batch : batch_infos) {
        // Dump batch info
        out.write((const char*)(&batch.id), sizeof(batch.id));
        out.write((const char*)(&batch.batch_size), sizeof(batch.batch_size));
        out.write((const char*)(&batch.snapshot_range.min_snapshot_id),
                sizeof(batch.snapshot_range.min_snapshot_id));
        out.write((const char*)(&batch.snapshot_range.max_snapshot_id),
                sizeof(batch.snapshot_range.max_snapshot_id));

        size_t rg_count = batch.rg_info.size();
        out.write((const char*)(&rg_count), sizeof(rg_count));

        // Dump rowgroup info
        for (const RowGroupInfo& rg : batch.rg_info) {
            out.write((const char*)(&rg.id), sizeof(rg.id));
            out.write((const char*)(&rg.num_docs), sizeof(rg.num_docs));
        }
    }
    out.close();
}

std::shared_ptr<MetabatchInfo> MetabatchInfo::load(const std::string& metabatch_path)
{
    std::ifstream in;
    {
        std::string metabatch_info_path = MetabatchInfo::metabatch_info_path(metabatch_path);
        in.open(metabatch_info_path, std::ios::binary);
        if (!in.is_open()) {
            throw "Couldn't open %s"+ metabatch_info_path;
        }
    }

    std::shared_ptr<MetabatchInfo> metabatch_info(new MetabatchInfo());
    metabatch_info->metabatch_path = metabatch_path;

    in.read((char*)(&metabatch_info->id), sizeof(metabatch_info->id));
    // Number of batches in meta-batch
    size_t batch_count;
    in.read((char*)(&batch_count), sizeof(batch_count));
    metabatch_info->batch_infos.resize(batch_count);

    // Read each batch
    for (BatchInfo& batch : metabatch_info->batch_infos) {
        // Read batch info
        in.read((char*)(&batch.id), sizeof(batch.id));
        in.read((char*)(&batch.batch_size), sizeof(batch.batch_size));
        in.read((char*)(&batch.snapshot_range.min_snapshot_id), sizeof(batch.snapshot_range.min_snapshot_id));
        in.read((char*)(&batch.snapshot_range.max_snapshot_id), sizeof(batch.snapshot_range.max_snapshot_id));

        size_t rg_count;
        in.read((char*)(&rg_count), sizeof(rg_count));
        batch.rg_info.resize(rg_count);

        // read rowgroup info
        for (RowGroupInfo& rg : batch.rg_info) {
            in.read((char*)(&rg.id), sizeof(rg.id));
            in.read((char*)(&rg.num_docs), sizeof(rg.num_docs));
        }
    }
    in.close();
    return metabatch_info;
}

std::shared_ptr<MetabatchInfo> MetabatchInfo::empty_metabatch()
{
    return std::shared_ptr<MetabatchInfo>(new MetabatchInfo());
}

std::string MetabatchInfo::construct_db_path(const std::string& prefix,
        CustomerID customer_id,
        const std::string& table_name,
        PartitionID partition_id) {

    std::string path = FORMAT("/%s/%d/%s/%d", prefix, customer_id, table_name, partition_id);
    return path;
}

std::string MetabatchInfo::metabatch_info_path(const std::string& path) {
    return FORMAT("%s/metabatch.info", path);
}

std::shared_ptr<MetabatchInfo> MetabatchInfo::make(const std::string& path,
        MetabatchID id,
        const std::vector<BatchInfo>& batch_infos)
{
    return std::shared_ptr<MetabatchInfo>(new MetabatchInfo(path, id, batch_infos));
}

bool is_valid_ip_address(const char *ip_address) {
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ip_address, &(sa.sin_addr));
    return result != 0;
}