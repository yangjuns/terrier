#include <iostream>
#include <memory>
#include <shared_mutex>  //NOLINT lint thinks it's a c header which should be before c++ headers. Conflict with formatter.
#include <utility>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "loggers/main_logger.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/multithread_test_util.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"

namespace terrier {

// This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
// reference to other fast data structures (indexes) to compare against. We are interested in the SqlTable's raw
// performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
// of the Google Tests.

class SqlTableBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    common::WorkerPool thread_pool(num_threads_, {});
    commited_txns_.resize(num_threads_);
    // create schema
    catalog::col_oid_t col_oid(0);
    for (uint32_t i = 0; i < column_num_; i++) {
      columns_.emplace_back("", type::TypeId::BIGINT, false, col_oid++);
    }
    schema_ = new catalog::Schema(columns_, storage::layout_version_t(0));
    table_ = new storage::SqlTable(&sql_block_store_, *schema_, catalog::table_oid_t(123));

    std::vector<catalog::col_oid_t> all_col_oids;
    for (auto &col : schema_->GetColumns()) all_col_oids.emplace_back(col.GetOid());
    auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(0));

    initializer_ = new storage::ProjectedRowInitializer(std::get<0>(pair));
    map_ = new storage::ProjectionMap(std::get<1>(pair));
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_->ProjectedRowSize());
    redo_ = initializer_->InitializeRow(redo_buffer_);
    CatalogTestUtil::PopulateRandomRow(redo_, *schema_, pair.second, &generator_);

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_->ProjectedRowSize());
    read_ = initializer_->InitializeRow(read_buffer_);

    // generate a vector of ProjectedRow buffers for concurrent reads
    for (uint32_t i = 0; i < num_threads_; ++i) {
      // Create read buffer
      byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_->ProjectedRowSize());
      storage::ProjectedRow *read = initializer_->InitializeRow(read_buffer);
      read_buffers_.emplace_back(read_buffer);
      reads_.emplace_back(read);
    }
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    delete[] read_buffer_;
    delete schema_;
    delete version_schema_;
    delete initializer_;
    delete version_initializer_;
    delete map_;
    delete version_map_;
    delete table_;
    delete version_table_;
    for (auto p : read_buffers_) delete[] p;
    for (auto p : version_read_buffers_) delete[] p;
    columns_.clear();
    read_buffers_.clear();
    reads_.clear();
  }

  // Add a column of type integer to the given schema
  catalog::Schema AddColumn(const catalog::Schema &schema, catalog::col_oid_t *oid) {
    std::vector<catalog::Schema::Column> columns = schema.GetColumns();
    columns.emplace_back("", type::TypeId::BIGINT, false, (*oid)++);
    return catalog::Schema(columns, schema.GetVersion() + 1);
  }

  catalog::Schema DropColumn(const catalog::Schema &schema) {
    std::vector<catalog::Schema::Column> columns = schema.GetColumns();
    // If there is only one column, we don't drop it
    if (columns.size() == 1) return catalog::Schema(columns, schema.GetVersion() + 1);
    // randomly remove an element
    std::uniform_int_distribution<> dist(0, static_cast<int>(columns.size() - 1));
    columns.erase(columns.begin() + dist(generator_));
    return catalog::Schema(columns, schema.GetVersion() + 1);
  }

  catalog::Schema ChangeSchema(const catalog::Schema &schema, catalog::col_oid_t *oid) {
    std::bernoulli_distribution d(0.5);
    return d(generator_) ? AddColumn(schema, oid) : DropColumn(schema);
  }

  // This function generates a new schema but set the schema version number to be 0
  catalog::Schema GetNewSchema(const catalog::Schema &schema, catalog::col_oid_t *oid) {
    // create new schema
    std::vector<catalog::Schema::Column> new_columns(schema.GetColumns().begin(), schema.GetColumns().end() - 1);
    new_columns.emplace_back("", type::TypeId::BIGINT, false, (*oid)++);
    catalog::Schema new_schema(new_columns, storage::layout_version_t(0));
    return new_schema;
  }

  // Migrate tuples from t1 to t2. These tables have different schema. t2 is empty
  void MigrateTables(transaction::TransactionContext *txn, storage::SqlTable *t1, const catalog::Schema &t1_schema,
                     storage::SqlTable *t2, const catalog::Schema &t2_schema, std::vector<storage::TupleSlot> *slots) {
    //    printf("txn_start_time: %lu\n", !txn->StartTime());
    //    printf("migrating from %p to %p\n", t1, t2);
    // create a buffer to read from t1
    //    printf("read col_oids: ");
    std::vector<catalog::col_oid_t> all_oids;
    for (auto &c : t1_schema.GetColumns()) {
      all_oids.emplace_back(c.GetOid());
      //      printf(" %d ", !c.GetOid());
    }
    //    printf("\n");
    auto t1_pair = t1->InitializerForProjectedRow(all_oids, storage::layout_version_t(0));
    byte *read_buffer = common::AllocationUtil::AllocateAligned(t1_pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = t1_pair.first.InitializeRow(read_buffer);

    // create a buffer to write to t2
    all_oids.clear();
    //    printf("insert col_oids: ");
    for (auto &c : t2_schema.GetColumns()) {
      all_oids.emplace_back(c.GetOid());
      //      printf(" %d ", !c.GetOid());
    }
    //    printf("\n");
    auto t2_pair = t2->InitializerForProjectedRow(all_oids, storage::layout_version_t(0));
    byte *insert_buffer = common::AllocationUtil::AllocateAligned(t2_pair.first.ProjectedRowSize());
    storage::ProjectedRow *insert = t2_pair.first.InitializeRow(insert_buffer);
    for (size_t i = 0; i < slots->size(); i++) {
      //      printf("reading slot (%p,%d) \n", slots[i].GetBlock(),slots[i].GetOffset());
      bool succ = t1->Select(txn, (*slots)[i], read, t1_pair.second, storage::layout_version_t(0));
      if (!succ) LOG_INFO("buggggg {}", i);
      // insert that into t2 with new schema
      storage::StorageUtil::CopyProjectionIntoProjection(
          *read, t1_pair.second, t1->GetBlockLayout(storage::layout_version_t(0)), insert, t2_pair.second);
      storage::TupleSlot new_slot = t2->Insert(txn, *insert, storage::layout_version_t(0));
      //      printf("new slot (%p,%d) \n", new_slot.GetBlock(),new_slot.GetOffset());
      (*slots)[i] = new_slot;
    }
    // LOG_INFO("done... {} migrated", slots->size());
  }

  void CreateVersionTable() {
    // create a version table acting like catalog
    catalog::col_oid_t col_oid(100);
    std::vector<catalog::Schema::Column> columns;
    columns.emplace_back("table_oid", type::TypeId::INTEGER, false, col_oid++);
    columns.emplace_back("version", type::TypeId::INTEGER, false, col_oid++);
    version_schema_ = new catalog::Schema(columns, storage::layout_version_t(0));
    version_table_ = new storage::SqlTable(&version_block_store_, *version_schema_, catalog::table_oid_t(1));

    // insert (123, 0) into version_table
    std::vector<catalog::col_oid_t> all_oids(columns.size());
    for (size_t i = 0; i < all_oids.size(); i++) all_oids[i] = columns[i].GetOid();

    auto init_pair = version_table_->InitializerForProjectedRow(all_oids, storage::layout_version_t(0));
    version_initializer_ = new storage::ProjectedRowInitializer(std::get<0>(init_pair));
    version_map_ = new storage::ProjectionMap(std::get<1>(init_pair));

    byte *init_buffer = common::AllocationUtil::AllocateAligned(version_initializer_->ProjectedRowSize());
    storage::ProjectedRow *init_row = version_initializer_->InitializeRow(init_buffer);
    // fill in table_oid
    init_row->SetNotNull(version_map_->at(catalog::col_oid_t(100)));
    byte *table_oid_ptr = init_row->AccessWithNullCheck(version_map_->at(catalog::col_oid_t(100)));
    *reinterpret_cast<uint32_t *>(table_oid_ptr) = 123;
    // fill in version
    init_row->SetNotNull(version_map_->at(catalog::col_oid_t(101)));
    byte *version_ptr = init_row->AccessWithNullCheck(version_map_->at(catalog::col_oid_t(101)));
    *reinterpret_cast<uint32_t *>(version_ptr) = 0;

    auto init_txn = txn_manager_.BeginTransaction();
    //    printf("------ begin version insert txn (start_ts: %lu, id : %lu, addr: %p)\n", !init_txn->StartTime(),
    //           !init_txn->TxnId().load(), init_txn);
    version_slot_ = version_table_->Insert(init_txn, *init_row, storage::layout_version_t(0));
    txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);
    delete[] init_buffer;

    // generate a vector of ProjectedRow buffers for concurrent reads
    for (uint32_t i = 0; i < num_threads_; ++i) {
      // Create read buffer
      byte *read_buffer = common::AllocationUtil::AllocateAligned(version_initializer_->ProjectedRowSize());
      storage::ProjectedRow *read = version_initializer_->InitializeRow(read_buffer);
      version_read_buffers_.emplace_back(read_buffer);
      version_reads_.emplace_back(read);
    }
  }

  storage::layout_version_t GetVersion(transaction::TransactionContext *txn, uint32_t id) {
    // need to know which version is it
    // printf("version slot %p, %d\n", version_slot_.GetBlock(), version_slot_.GetOffset());
    bool succ =
        version_table_->Select(txn, version_slot_, version_reads_[id], *version_map_, storage::layout_version_t(0));
    if (!succ) {
      LOG_INFO("WTF??? SELECT FAILed!!");
    }
    byte *version_ptr = version_reads_[id]->AccessWithNullCheck(version_map_->at(catalog::col_oid_t(101)));
    storage::layout_version_t my_version(0);
    if (version_ptr == nullptr) {
      LOG_INFO("{}, null version!! supprising", id);
    } else {
      my_version = storage::layout_version_t(*reinterpret_cast<uint32_t *>(version_ptr));
    }
    return my_version;
  }

  bool UpdateVersionTable(transaction::TransactionContext *txn, storage::layout_version_t new_ver) {
    // TODO(yangjuns): can speed up
    // create update redo
    std::vector<catalog::col_oid_t> cols;
    // update the version column
    cols.emplace_back(version_schema_->GetColumn(1).GetOid());
    auto pair = version_table_->InitializerForProjectedRow(cols, storage::layout_version_t(0));
    byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *update_row = pair.first.InitializeRow(update_buffer);
    auto *version = reinterpret_cast<uint32_t *>(update_row->AccessForceNotNull(pair.second.at(cols[0])));
    *version = !new_ver;
    auto result = version_table_->Update(txn, version_slot_, *update_row, pair.second, storage::layout_version_t(0));
    delete[] update_buffer;
    return result.first;
  }

  /**
   * Return the index of the slots
   * @param slots
   * @return
   */
  std::pair<uint32_t, storage::TupleSlot> GetHotSpotSlot(const std::vector<storage::TupleSlot> &slots, double hs_ratio,
                                                         double hs_prob) {
    uint32_t index = 0;
    auto hot_spot_range = static_cast<uint32_t>(num_inserts_ * hs_ratio);
    std::uniform_real_distribution<> real_dist(0, 1);
    if (real_dist(generator_) < hs_prob) {
      std::uniform_int_distribution<> int_dist(0, hot_spot_range - 1);
      index = int_dist(generator_);
    } else {
      std::uniform_int_distribution<> int_dist(hot_spot_range, num_inserts_ - 1);
      index = int_dist(generator_);
    }
    storage::TupleSlot result;
    {
      common::SpinLatch::ScopedSpinLatch guard(&slot_latch_);
      result = slots[index];
    }
    return {index, result};
  }

  void StartGC(transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  // schema_latch_
  common::SpinLatch schema_latch_;
  common::SpinLatch slot_latch_;

  // Sql Table
  storage::SqlTable *table_ = nullptr;
  storage::SqlTable *version_table_ = nullptr;

  // Version Read buffers pointers for concurrent reads
  std::vector<byte *> version_read_buffers_;
  std::vector<storage::ProjectedRow *> version_reads_;

  // Tuple properties
  const storage::ProjectedRowInitializer *initializer_ = nullptr;
  const storage::ProjectionMap *map_;
  const storage::ProjectedRowInitializer *version_initializer_ = nullptr;
  const storage::ProjectionMap *version_map_;

  // Version Slot
  storage::TupleSlot version_slot_;

  // Workload
  const uint32_t num_txns_ = 100000;
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_deletes_ = 10000000;
  const uint32_t num_reads_ = 10000000;
  const uint32_t num_updates_ = 10000000;
  const uint32_t num_threads_ = 4;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;
  const uint32_t scan_buffer_size_ = 1000;  // maximum number of tuples in a buffer

  // Count
  std::vector<uint32_t> commited_txns_;
  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore sql_block_store_{1000000, 1000};
  storage::BlockStore version_block_store_{1000000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{10000000, buffer_pool_reuse_limit_};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  // Schema
  const uint32_t column_num_ = 10;
  std::vector<catalog::Schema::Column> columns_;
  catalog::Schema *schema_ = nullptr;
  catalog::Schema *version_schema_ = nullptr;

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;

  // Read buffers pointers for concurrent reads
  std::vector<byte *> read_buffers_;
  std::vector<storage::ProjectedRow *> reads_;

  // read-write locks
  mutable std::shared_timed_mutex mutex_;

  // GC
  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  uint32_t schema_txn_wake_up_interval_ = 100;  // milliseconds

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentWorkloadBlocking)(benchmark::State &state) {
  // Populate table_ by inserting tuples
  LOG_INFO("inserting tuples ...");

  auto init_txn = txn_manager_.BeginTransaction();
  std::vector<storage::TupleSlot> slots;
  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    slots.emplace_back(table_->Insert(init_txn, *redo_, storage::layout_version_t(0)));
  }
  txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);

  // pre-generate new schemas
  std::vector<catalog::Schema> new_schemas;
  new_schemas.emplace_back(*schema_);
  catalog::col_oid_t change_start_oid(1000);

  int schema_index = 0;
  bool stopped = false;
  // create 4 workloads
  // bool try_migrating = false;
  // Insert workload
  auto insert = [&](uint32_t id) {
    // try to get the lock because we are going to access table_
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);

    auto txn = txn_manager_.BeginTransaction();

    // Create a redo buffer
    std::vector<catalog::col_oid_t> all_oids;
    catalog::Schema *my_schema = nullptr;

    {
      common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
      my_schema = new catalog::Schema(new_schemas[schema_index]);
    }
    for (auto &c : my_schema->GetColumns()) {
      all_oids.emplace_back(c.GetOid());
    }
    auto pair = table_->InitializerForProjectedRow(all_oids, storage::layout_version_t(0));
    byte *redo_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *redo = pair.first.InitializeRow(redo_buffer);
    CatalogTestUtil::PopulateRandomRow(redo, *my_schema, pair.second, &generator_);

    // Insert the tuple
    storage::TupleSlot slot = table_->Insert(txn, *redo, storage::layout_version_t(0));
    {
      common::SpinLatch::ScopedSpinLatch guard(&slot_latch_);
      slots.emplace_back(slot);
    }
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    delete my_schema;
    delete[] redo_buffer;
    return true;
  };

  // Read Workload
  auto read = [&](uint32_t id) {
    // try to get the lock because we are going to access table_
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto txn = txn_manager_.BeginTransaction();
    bool commited;

    std::vector<catalog::col_oid_t> all_oids;
    {
      common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
      for (auto &c : new_schemas[schema_index].GetColumns()) {
        all_oids.emplace_back(c.GetOid());
      }
    }

    auto pair = table_->InitializerForProjectedRow(all_oids, storage::layout_version_t(0));
    byte *read_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = pair.first.InitializeRow(read_buffer);

    // Select never fails
    auto slot_pair = GetHotSpotSlot(slots, 0.05, 0.8);
    bool succ = table_->Select(txn, slot_pair.second, read, pair.second, storage::layout_version_t(0));
    if (succ) {
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      commited = true;
    } else {
      txn_manager_.Abort(txn);
      commited = false;
      LOG_INFO("select failed.. Impossible")
    }

    // free memory
    delete[] read_buffer;
    return commited;
  };

  // Update workload
  auto update = [&](uint32_t id) {
    // try to get the lock because we are going to access table_
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto txn = txn_manager_.BeginTransaction();

    bool commited;
    catalog::Schema *my_schema = nullptr;
    {
      common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
      my_schema = new catalog::Schema(new_schemas[schema_index]);
    }
    // Create a redo buffer
    std::vector<catalog::col_oid_t> all_oids;
    for (auto &c : my_schema->GetColumns()) {
      all_oids.emplace_back(c.GetOid());
    }
    auto pair = table_->InitializerForProjectedRow(all_oids, storage::layout_version_t(0));
    byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *update_row = pair.first.InitializeRow(update_buffer);

    CatalogTestUtil::PopulateRandomRow(update_row, *my_schema, pair.second, &generator_);

    // Update the tuple
    auto slot_pair = GetHotSpotSlot(slots, 0.05, 0.8);
    auto update_pair = table_->Update(txn, slot_pair.second, *update_row, pair.second, storage::layout_version_t(0));
    if (update_pair.first) {
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      commited = true;
    } else {
      // write-write conflict
      txn_manager_.Abort(txn);
      commited = false;
    }
    delete[] update_buffer;
    delete my_schema;
    return commited;
  };

  auto schema_change = [&]() {
    while (true) {
      // sleep for 5 seconds
      std::this_thread::sleep_for(std::chrono::milliseconds(schema_txn_wake_up_interval_));
      if (stopped) break;
      std::lock_guard<std::shared_timed_mutex> lock(mutex_);
      auto txn = txn_manager_.BeginTransaction();
      // now we have full control

      // Create a new schema

      catalog::Schema new_schema = GetNewSchema(new_schemas[schema_index], &change_start_oid);

      {
        common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
        new_schemas.emplace_back(new_schema);
      }
      schema_index++;

      // 1. create a new table of the schema

      storage::SqlTable *new_table =
          new storage::SqlTable(&sql_block_store_, new_schemas[schema_index], catalog::table_oid_t(123));

      // 2. Migrate data over
      LOG_INFO("migrating from {} to {}", schema_index - 1, schema_index);
      MigrateTables(txn, table_, new_schemas[schema_index - 1], new_table, new_schemas[schema_index], &slots);
      // 3. change the table_ pointer
      table_ = new_table;  // potential memory leak but will do for the benchmarks
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      if (stopped) break;
    }
  };
  // std::atomic<int> txn_run = 0;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_txns_ / num_threads_; ++i) {
        uint32_t commited = RandomTestUtil::InvokeWorkloadWithDistribution({read, insert, update}, {id, id, id},
                                                                           {0.7, 0.2, 0.1}, &generator_);
        commited_txns_[id] += commited;
        // txn_run++;
        // LOG_INFO("txn_run : {}", txn_run);
      }
    };
    uint64_t elapsed_ms = 0;
    common::WorkerPool thread_pool(num_threads_, {});
    std::thread t2(schema_change);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    stopped = true;
    t2.join();
    LOG_INFO("howmany inserts? {}", slots.size());
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  // sum of commited
  uint32_t sum = 0;
  for (auto c : commited_txns_) {
    sum += c;
  }
  state.SetItemsProcessed(sum);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentWorkload)(benchmark::State &state) {
  CreateVersionTable();
  // Populate table_ by inserting tuples
  LOG_INFO("inserting tuples ...");

  auto init_txn = txn_manager_.BeginTransaction();
  //  printf("------ begin insert txn (start_ts: %lu, id : %lu, addr: %p)\n", !init_txn->StartTime(),
  //         !init_txn->TxnId().load(), init_txn);
  std::vector<storage::TupleSlot> slots;
  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    slots.emplace_back(table_->Insert(init_txn, *redo_, storage::layout_version_t(0)));
  }
  txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);

  // pre-generate new schemas
  LOG_INFO("Pregenerating schemas ...");
  std::vector<catalog::Schema> new_schemas;
  new_schemas.emplace_back(*schema_);
  catalog::col_oid_t change_start_oid(1000);
  LOG_INFO("Finished schema size {}", new_schemas.size());
  // create 4 workloads
  bool stopped = false;
  // Insert workload
  auto insert = [&](uint32_t id) {
    auto txn = txn_manager_.BeginTransaction();
    storage::layout_version_t my_version = GetVersion(txn, id);

    // Create a redo buffer
    std::vector<catalog::col_oid_t> all_oids;
    catalog::Schema *my_schema = nullptr;

    {
      common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
      my_schema = new catalog::Schema(new_schemas[!my_version]);
    }
    for (auto &c : my_schema->GetColumns()) {
      all_oids.emplace_back(c.GetOid());
    }
    auto pair = table_->InitializerForProjectedRow(all_oids, my_version);
    byte *redo_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *redo = pair.first.InitializeRow(redo_buffer);
    CatalogTestUtil::PopulateRandomRow(redo, *my_schema, pair.second, &generator_);

    // Insert the tuple
    table_->Insert(txn, *redo, my_version);
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    delete my_schema;
    delete[] redo_buffer;
    return true;
  };

  // Read Workload
  auto read = [&](uint32_t id) {
    auto txn = txn_manager_.BeginTransaction();
    storage::layout_version_t my_version = GetVersion(txn, id);
    bool aborted;
    // Create a projected row buffer to reads
    // create read buffer
    // printf("reading version %d\n", !my_version);
    std::vector<catalog::col_oid_t> all_oids;
    {
      common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
      for (auto &c : new_schemas[!my_version].GetColumns()) {
        all_oids.emplace_back(c.GetOid());
      }
    }

    auto pair = table_->InitializerForProjectedRow(all_oids, my_version);
    byte *read_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = pair.first.InitializeRow(read_buffer);

    // Select never fails
    auto slot_pair = GetHotSpotSlot(slots, 0.05, 0.8);
    bool succ = table_->Select(txn, slot_pair.second, read, pair.second, my_version);
    if (succ) {
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      aborted = false;
    } else {
      txn_manager_.Abort(txn);
      aborted = true;
    }

    // free memory
    delete[] read_buffer;
    return aborted;
  };

  // Update workload
  auto update = [&](uint32_t id) {
    auto txn = txn_manager_.BeginTransaction();
    storage::layout_version_t my_version = GetVersion(txn, id);
    bool aborted;
    catalog::Schema *my_schema = nullptr;
    {
      common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
      my_schema = new catalog::Schema(new_schemas[!my_version]);
    }
    // Create a redo buffer
    std::vector<catalog::col_oid_t> all_oids;
    for (auto &c : my_schema->GetColumns()) {
      all_oids.emplace_back(c.GetOid());
    }
    auto pair = table_->InitializerForProjectedRow(all_oids, my_version);
    byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *update_row = pair.first.InitializeRow(update_buffer);

    CatalogTestUtil::PopulateRandomRow(update_row, *my_schema, pair.second, &generator_);

    // Update the tuple
    auto slot_pair = GetHotSpotSlot(slots, 0.05, 0.8);
    auto update_pair = table_->Update(txn, slot_pair.second, *update_row, pair.second, my_version);
    if (update_pair.first) {
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      aborted = false;
    } else {
      // write-write conflict
      txn_manager_.Abort(txn);
      aborted = true;
    }
    delete[] update_buffer;
    delete my_schema;
    return aborted;
  };
  std::atomic<int> succ_schema_change_count = 0;
  auto schema_change = [&](uint32_t id) {
    while (true) {
      // sleep for 5 seconds
      std::this_thread::sleep_for(std::chrono::milliseconds(schema_txn_wake_up_interval_));
      if (stopped) break;
      auto txn = txn_manager_.BeginTransaction();
      storage::layout_version_t my_version = GetVersion(txn, id);
      // update the version table
      bool succ = UpdateVersionTable(txn, my_version + 1);
      if (succ) {
        // Create a schema to update
        // there will be only one thread reaching here.
        {
          common::SpinLatch::ScopedSpinLatch guard(&schema_latch_);
          new_schemas.emplace_back(ChangeSchema(*(new_schemas.end() - 1), &change_start_oid));
        }

        // update schema
        TERRIER_ASSERT((!(new_schemas[!my_version + 1].GetVersion())) == (!my_version + 1),
                       "the version of the schema should by 1 bigger");
        table_->UpdateSchema(*(new_schemas.end() - 1));

        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        succ_schema_change_count++;
      } else {
        // someone else is updating the schema, abort
        txn_manager_.Abort(txn);
      }
      if (stopped) break;
    }
  };

  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_txns_ / num_threads_; ++i) {
        uint32_t commited = RandomTestUtil::InvokeWorkloadWithDistribution({read, insert, update}, {id, id, id},
                                                                           {0.7, 0.2, 0.1}, &generator_);
        commited_txns_[id] += commited;
      }
    };
    uint64_t elapsed_ms = 0;
    common::WorkerPool thread_pool(num_threads_, {});
    std::thread t2(schema_change, 0);
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
    stopped = true;
    t2.join();
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  // sum of commited
  uint32_t sum = 0;
  for (auto c : commited_txns_) {
    sum += c;
  }
  LOG_INFO("schema updates count: {}", succ_schema_change_count);
  state.SetItemsProcessed(sum + succ_schema_change_count);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ThroughputChangeSelect)(benchmark::State &state) {
  auto init_txn = txn_manager_.BeginTransaction();
  std::vector<storage::TupleSlot> slots;
  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    slots.emplace_back(table_->Insert(init_txn, *redo_, storage::layout_version_t(0)));
  }
  txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));

  // throughput vector
  std::vector<double> throughput;
  uint32_t version = 0;
  bool finished = false;
  // Select Thread
  auto read = [&]() {
    uint64_t committed_txns_count = 0;
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
      auto txn = txn_manager_.BeginTransaction();
      // get my version
      storage::layout_version_t my_version(version);

      // get columns to read
      std::vector<catalog::col_oid_t> all_oids;
      if (!my_version == 0) {
        for (auto &c : schema_->GetColumns()) {
          all_oids.emplace_back(c.GetOid());
        }
      } else {
        for (auto &c : new_schema.GetColumns()) {
          all_oids.emplace_back(c.GetOid());
        }
      }

      auto pair = table_->InitializerForProjectedRow(all_oids, my_version);
      byte *read_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
      storage::ProjectedRow *read = pair.first.InitializeRow(read_buffer);

      // Select never fails
      auto slot_pair = GetHotSpotSlot(slots, 0.05, 0.8);
      bool succ = table_->Select(txn, slot_pair.second, read, pair.second, my_version);
      if (succ) {
        committed_txns_count++;
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      } else {
        txn_manager_.Abort(txn);
      }
      // free memory
      delete[] read_buffer;
      std::chrono::duration<double, std::milli> diff = std::chrono::high_resolution_clock::now() - start;
      if (diff.count() > 1000) {
        throughput.emplace_back(static_cast<double>(committed_txns_count) / (diff.count() / 1000));
        committed_txns_count = 0;
        start = std::chrono::high_resolution_clock::now();
      }
      if (finished) break;
    }
  };

  auto schema_change = [&]() {
    // sleep for 5 seconds
    std::this_thread::sleep_for(std::chrono::seconds(60));

    // change the schema
    auto txn = txn_manager_.BeginTransaction();
    table_->UpdateSchema(new_schema);
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    version = 1;
  };

  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    std::thread t1(read);
    std::thread t2(schema_change);
    // sleep for 30 seconds
    std::this_thread::sleep_for(std::chrono::seconds(180));
    // stop all threads
    finished = true;
    t1.join();
    t2.join();
    // print throughput
    for (size_t i = 0; i < throughput.size(); i++) {
      printf("(%zu, %d)\n", i + 1, static_cast<int>(throughput[i]));
    }
    EndGC();
  }
  state.SetItemsProcessed(0);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ThroughputChangeUpdate)(benchmark::State &state) {
  auto init_txn = txn_manager_.BeginTransaction();
  std::vector<storage::TupleSlot> slots;
  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    slots.emplace_back(table_->Insert(init_txn, *redo_, storage::layout_version_t(0)));
  }
  txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));

  // throughput vector
  uint32_t version = 0;

  bool finished = false;

  int committed_txns_count = 0;

  // Throughput Compute Thread
  auto compute = [&]() {
    // let the system warm up for 10 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));
    // checks throughput every second
    int prev = committed_txns_count;
    int seconds = 1;
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      int cur = committed_txns_count;
      printf("(%d, %d)\n", seconds, cur - prev);
      prev = cur;
      seconds++;
      if (finished) break;
    }
  };
  // Update Thread
  auto update = [&]() {
    while (true) {
      auto txn = txn_manager_.BeginTransaction();
      // get my version
      storage::layout_version_t my_version(version);

      // get correct columns
      std::vector<catalog::col_oid_t> all_oids;
      if (!my_version == 0) {
        for (auto &c : schema_->GetColumns()) {
          all_oids.emplace_back(c.GetOid());
        }
      } else {
        for (auto &c : new_schema.GetColumns()) {
          all_oids.emplace_back(c.GetOid());
        }
      }

      auto pair = table_->InitializerForProjectedRow(all_oids, my_version);
      byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
      storage::ProjectedRow *update = pair.first.InitializeRow(update_buffer);
      if (!my_version == 0) {
        CatalogTestUtil::PopulateRandomRow(update, *schema_, pair.second, &generator_);
      } else {
        CatalogTestUtil::PopulateRandomRow(update, new_schema, pair.second, &generator_);
      }

      // Update never fails
      auto slot_pair = GetHotSpotSlot(slots, 1, 1);
      auto result = table_->Update(txn, slot_pair.second, *update, pair.second, my_version);
      if (result.first) {
        committed_txns_count++;
        // check if the tuple slot got updated
        if (result.second != slot_pair.second) {
          {
            common::SpinLatch::ScopedSpinLatch guard(&slot_latch_);
            slots[slot_pair.first] = result.second;
          }
        }
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      } else {
        LOG_INFO("impossible")
        txn_manager_.Abort(txn);
      }
      // free memory
      delete[] update_buffer;
      if (finished) break;
    }
  };

  auto schema_change = [&]() {
    // let the system warm up for 10 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));
    // sleep for 5 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // change the schema
    auto txn = txn_manager_.BeginTransaction();
    table_->UpdateSchema(new_schema);
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    version = 1;
  };

  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    std::thread t1(update);
    std::thread t2(schema_change);
    std::thread t3(compute);
    // sleep for 30 seconds
    std::this_thread::sleep_for(std::chrono::seconds(130));
    // stop all threads
    finished = true;
    t1.join();
    t2.join();
    t3.join();
    EndGC();
  }
  state.SetItemsProcessed(0);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, BlockThroughputChangeUpdate)(benchmark::State &state) {
  auto init_txn = txn_manager_.BeginTransaction();
  std::vector<storage::TupleSlot> slots;
  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    slots.emplace_back(table_->Insert(init_txn, *redo_, storage::layout_version_t(0)));
  }
  txn_manager_.Commit(init_txn, TestCallbacks::EmptyCallback, nullptr);

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(0));

  bool finished = false;
  bool new_version = false;
  common::SpinLatch update_latch;

  // new table
  storage::SqlTable *new_table = nullptr;
  int committed_txns_count = 0;
  // Throughput Compute Thread
  auto compute = [&]() {
    // let the system warm up for 10 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));
    // checks throughput every second
    int prev = committed_txns_count;
    int seconds = 1;
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      int cur = committed_txns_count;
      printf("(%d, %d)\n", seconds, cur - prev);
      prev = cur;
      seconds++;
      if (finished) break;
    }
  };
  // Update Thread
  auto update = [&]() {
    while (true) {
      {
        common::SpinLatch::ScopedSpinLatch update_guard(&update_latch);
        auto txn = txn_manager_.BeginTransaction();
        // get my version
        storage::layout_version_t my_version(0);

        // get correct columns
        std::vector<catalog::col_oid_t> all_oids;
        if (!new_version) {
          for (auto &c : schema_->GetColumns()) {
            all_oids.emplace_back(c.GetOid());
          }
        } else {
          for (auto &c : new_schema.GetColumns()) {
            all_oids.emplace_back(c.GetOid());
          }
        }

        storage::SqlTable *my_table = nullptr;
        if (!new_version) {
          my_table = table_;
        } else {
          my_table = new_table;
        }
        auto pair = my_table->InitializerForProjectedRow(all_oids, my_version);
        byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
        storage::ProjectedRow *update = pair.first.InitializeRow(update_buffer);
        if (!new_version) {
          CatalogTestUtil::PopulateRandomRow(update, *schema_, pair.second, &generator_);
        } else {
          CatalogTestUtil::PopulateRandomRow(update, new_schema, pair.second, &generator_);
        }

        // Update never fails
        auto slot_pair = GetHotSpotSlot(slots, 0.05, 0.8);
        auto result = my_table->Update(txn, slot_pair.second, *update, pair.second, my_version);
        if (result.first) {
          committed_txns_count++;
          // check if the tuple slot got updated
          if (result.second != slot_pair.second) {
            slots[slot_pair.first] = result.second;
          }
          txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        } else {
          LOG_INFO("impossible")
          txn_manager_.Abort(txn);
        }
        // free memory
        delete[] update_buffer;
        if (finished) break;
      }
    }
  };

  auto schema_change = [&]() {
    // let the system warm up for 10 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));
    // sleep for 5 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // change the schema
    {
      common::SpinLatch::ScopedSpinLatch update_guard(&update_latch);
      auto txn = txn_manager_.BeginTransaction();
      // create new table
      new_table = new storage::SqlTable(&sql_block_store_, new_schema, catalog::table_oid_t(456));

      MigrateTables(txn, table_, *schema_, new_table, new_schema, &slots);
      txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      new_version = true;
    }
  };

  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    std::thread t1(update);
    std::thread t2(schema_change);
    std::thread t3(compute);
    // sleep for 30 seconds
    std::this_thread::sleep_for(std::chrono::seconds(130));
    // stop all threads
    finished = true;
    t1.join();
    t2.join();
    t3.join();
    //    // print throughput
    //    for (size_t i = 0; i < throughput.size(); i++) {
    //      printf("(%zu, %d)\n", i + 1, static_cast<int>(throughput[i]));
    //    }
    EndGC();
    delete new_table;
  }
  state.SetItemsProcessed(0);
}

// Insert the num_inserts_ of tuples into a SqlTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Create a sql_table
    storage::SqlTable table(&sql_block_store_, *schema_, catalog::table_oid_t(0));
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                        LOGGING_DISABLED);
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table.Insert(&txn, *redo_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Insert the num_inserts_ of tuples into a SqlTable of two versions in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Create a sql_table
    storage::SqlTable table(&sql_block_store_, *schema_, catalog::table_oid_t(0));
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                        LOGGING_DISABLED);

    // create new schema
    catalog::col_oid_t col_oid(column_num_);
    std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
    new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
    catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
    table.UpdateSchema(new_schema);

    // create a new insert buffer
    std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
    for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
    auto pair = table.InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
    byte *insert_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
    storage::ProjectedRow *insert_pr = pair.first.InitializeRow(insert_buffer);
    CatalogTestUtil::PopulateRandomRow(insert_pr, new_schema, pair.second, &generator_);

    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_inserts_; ++i) {
        table.Insert(&txn, *insert_pr, storage::layout_version_t(1));
      }
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Insert the num_inserts_ of tuples into a SqlTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Create a sql_table
    storage::SqlTable table(&sql_block_store_, *schema_, catalog::table_oid_t(0));

    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                          LOGGING_DISABLED);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; ++i) {
        table.Insert(&txn, *redo_, storage::layout_version_t(0));
      }
    };
    common::WorkerPool thread_pool(num_threads_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a SqlTable in a single thread
// The SqlTable has only one schema version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionSequentialRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_, *map_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a SqlTable in a single thread
// The SqlTable has only one schema version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionSequentialDelete)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    auto txn = txn_manager_.BeginTransaction();
    std::vector<storage::TupleSlot> delete_order;
    for (uint32_t i = 0; i < num_deletes_; ++i) {
      delete_order.emplace_back(table_->Insert(txn, *redo_, storage::layout_version_t(0)));
    }
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_deletes_; ++i) {
        table_->Delete(txn, delete_order[i], storage::layout_version_t(0));
      }
    }
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    EndGC();
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(state.iterations() * num_deletes_);
}

// Read the num_reads_ of tuples in a sequential order from a SqlTable in a single thread
// The SqlTable has multiple schema versions and the read version mismatches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchSequentialRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new read buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *read_pr = pair.first.InitializeRow(buffer);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a SqlTable in a single thread
// The SqlTable has multiple schema versions and the read version matches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMatchSequentialRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new insert buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert_pr = pair.first.InitializeRow(insert_buffer);
  CatalogTestUtil::PopulateRandomRow(insert_pr, new_schema, pair.second, &generator_);

  // insert tuples
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *insert_pr, storage::layout_version_t(1)));
  }
  delete[] insert_buffer;

  // create a new read buffer
  byte *read_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *read_pr = pair.first.InitializeRow(read_buffer);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] read_buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable in a single thread
// The SqlTable has only one schema version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionRandomRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_, *map_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable in a single thread
// The SqlTable has multiple schema versions and the read version mismatches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchRandomRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new read buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *read_pr = pair.first.InitializeRow(buffer);

  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable in a single thread
// The SqlTable has multiple schema versions and the read version matches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMatchRandomRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new insert buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert_pr = pair.first.InitializeRow(insert_buffer);
  CatalogTestUtil::PopulateRandomRow(insert_pr, new_schema, pair.second, &generator_);

  // insert tuples
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *insert_pr, storage::layout_version_t(1)));
  }
  delete[] insert_buffer;

  // create a new read buffer
  byte *read_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *read_pr = pair.first.InitializeRow(read_buffer);

  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] read_buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable concurrently
// The SqlTable has only a single versions
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentSingleVersionRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                          LOGGING_DISABLED);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; ++i) {
        table_->Select(&txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], reads_[id], *map_,
                       storage::layout_version_t(0));
      }
    };
    common::WorkerPool thread_pool(num_threads_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable concurrently
// The SqlTable has multiple schema versions and the read version mismatches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentMultiVersionRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // update the vector of ProjectedRow buffers for concurrent reads
  for (uint32_t i = 0; i < num_threads_; ++i) delete[] read_buffers_[i];
  read_buffers_.clear();
  reads_.clear();
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create read buffer
    byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_->ProjectedRowSize());
    storage::ProjectedRow *read = initializer_->InitializeRow(read_buffer);
    read_buffers_.emplace_back(read_buffer);
    reads_.emplace_back(read);
  }

  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                          LOGGING_DISABLED);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; ++i) {
        table_->Select(&txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], reads_[id], *map_,
                       storage::layout_version_t(1));
      }
    };
    common::WorkerPool thread_pool(num_threads_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Update a tuple in a single-version SqlTable num_updates_ times in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionUpdate)(benchmark::State &state) {
  // Insert a tuple to the table
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  storage::TupleSlot slot = table_->Insert(&txn, *redo_, storage::layout_version_t(0));
  // Populate with random values for updates
  CatalogTestUtil::PopulateRandomRow(redo_, *schema_, *map_, &generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_updates_; ++i) {
      // update the tuple with the  for benchmark purpose
      table_->Update(&txn, slot, *redo_, *map_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_updates_);
}

// Insert num_deletes_ tuples and then delete them in sequential order
// The SqlTable has multiple schema versions and and the transaction version matches the tuple version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMatchDelete)(benchmark::State &state) {
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new insert buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert_pr = pair.first.InitializeRow(insert_buffer);
  CatalogTestUtil::PopulateRandomRow(insert_pr, new_schema, pair.second, &generator_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    auto txn = txn_manager_.BeginTransaction();
    // insert tuples
    std::vector<storage::TupleSlot> delete_order;
    for (uint32_t i = 0; i < num_deletes_; ++i) {
      delete_order.emplace_back(table_->Insert(txn, *insert_pr, storage::layout_version_t(1)));
    }
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_deletes_; ++i) {
        table_->Delete(txn, delete_order[i], storage::layout_version_t(1));
      }
    }
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    EndGC();
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  delete[] insert_buffer;
  state.SetItemsProcessed(state.iterations() * num_deletes_);
}

// Insert num_deletes_ tuples and then delete them in sequential order
// The SqlTable has multiple schema versions and and the transaction version does not match the tuple version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchDelete)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    // Create a sql_table
    storage::SqlTable table(&sql_block_store_, *schema_, catalog::table_oid_t(0));
    auto txn = txn_manager_.BeginTransaction();
    // insert tuples
    std::vector<storage::TupleSlot> delete_order;
    for (uint32_t i = 0; i < num_deletes_; ++i) {
      delete_order.emplace_back(table.Insert(txn, *redo_, storage::layout_version_t(0)));
    }

    // create new schema
    catalog::col_oid_t col_oid(column_num_);
    std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
    new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
    catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
    table.UpdateSchema(new_schema);

    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_deletes_; ++i) {
        table.Delete(txn, delete_order[i], storage::layout_version_t(1));
      }
    }
    txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    EndGC();
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_deletes_);
}

// Update a tuple in a SqlTable num_updates_ times in a single thread
// The SqlTable has multiple schema versions and the redo can be updated in place
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMatchUpdate)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new insert buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert_pr = pair.first.InitializeRow(insert_buffer);
  CatalogTestUtil::PopulateRandomRow(insert_pr, new_schema, pair.second, &generator_);

  // insert a tuple
  storage::TupleSlot slot = table_->Insert(&txn, *insert_pr, storage::layout_version_t(1));

  delete[] insert_buffer;

  // create a new update buffer
  byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *update_pr = pair.first.InitializeRow(update_buffer);
  CatalogTestUtil::PopulateRandomRow(update_pr, new_schema, pair.second, &generator_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_updates_; ++i) {
      table_->Update(&txn, slot, *update_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] update_buffer;
  state.SetItemsProcessed(state.iterations() * num_updates_);
}

// Update a tuple in a SqlTable num_updates_ times in a single thread
// The SqlTable has multiple schema versions and the redo cannot be updated in place
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchUpdate)(benchmark::State &state) {
  auto txn = txn_manager_.BeginTransaction();
  // insert a bunch of tuples
  std::vector<storage::TupleSlot> update_slots;
  for (uint32_t i = 0; i < num_updates_; ++i) {
    update_slots.emplace_back(table_->Insert(txn, *redo_, storage::layout_version_t(0)));
  }

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a update buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *update_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *update_pr = pair.first.InitializeRow(update_buffer);
  CatalogTestUtil::PopulateRandomRow(update_pr, new_schema, pair.second, &generator_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_updates_; ++i) {
      table_->Update(txn, update_slots[i], *update_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] update_buffer;
  txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  delete txn;
  state.SetItemsProcessed(state.iterations() * num_updates_);
}

// Scan the num_insert_ of tuples from a SqlTable in a single thread
// The SqlTable has only one schema version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionScan)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    table_->Insert(&txn, *redo_, storage::layout_version_t(0));
  }

  // create a scan buffer
  std::vector<catalog::col_oid_t> all_col_oids;
  for (auto &col : schema_->GetColumns()) all_col_oids.emplace_back(col.GetOid());
  auto pair = table_->InitializerForProjectedColumns(all_col_oids, scan_buffer_size_, storage::layout_version_t(0));
  byte *buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedColumnsSize());
  storage::ProjectedColumns *scan_pr = pair.first.Initialize(buffer);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto start_pos = table_->begin(storage::layout_version_t(0));
    while (start_pos != table_->end()) {
      table_->Scan(&txn, &start_pos, scan_pr, pair.second, storage::layout_version_t(0));
      scan_pr = pair.first.Initialize(buffer);
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Scan the num_insert_ of tuples from a SqlTable in a single thread
// The SqlTable has two schema versions
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMatchScan)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new insert buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto row_pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *insert_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert_pr = row_pair.first.InitializeRow(insert_buffer);
  CatalogTestUtil::PopulateRandomRow(insert_pr, new_schema, row_pair.second, &generator_);

  // insert tuples into new schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    table_->Insert(&txn, *insert_pr, storage::layout_version_t(1));
  }

  // create a new read buffer
  auto col_pair = table_->InitializerForProjectedColumns(all_col_oids, scan_buffer_size_, storage::layout_version_t(1));
  byte *buffer = common::AllocationUtil::AllocateAligned(col_pair.first.ProjectedColumnsSize());
  storage::ProjectedColumns *scan_pr = col_pair.first.Initialize(buffer);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto start_pos = table_->begin(storage::layout_version_t(1));
    while (start_pos != table_->end()) {
      table_->Scan(&txn, &start_pos, scan_pr, col_pair.second, storage::layout_version_t(1));
      scan_pr = col_pair.first.Initialize(buffer);
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Scan the num_insert_ of tuples from a SqlTable in a single thread
// The SqlTable has two schema versions
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchScan)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);

  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_; ++i) {
    table_->Insert(&txn, *redo_, storage::layout_version_t(0));
  }

  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schema(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schema);

  // create a new read buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  auto col_pair = table_->InitializerForProjectedColumns(all_col_oids, scan_buffer_size_, storage::layout_version_t(1));
  byte *buffer = common::AllocationUtil::AllocateAligned(col_pair.first.ProjectedColumnsSize());
  storage::ProjectedColumns *scan_pr = col_pair.first.Initialize(buffer);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto start_pos = table_->begin(storage::layout_version_t(1));
    while (start_pos != table_->end()) {
      table_->Scan(&txn, &start_pos, scan_pr, col_pair.second, storage::layout_version_t(1));
      scan_pr = col_pair.first.Initialize(buffer);
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

//// Benchmarks for common cases
// BENCHMARK_REGISTER_F(SqlTableBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);

// BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionRandomRead)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionUpdate)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark,
// SingleVersionSequentialDelete)->Unit(benchmark::kMillisecond)->UseManualTime();
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionSequentialRead)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionScan)->Unit(benchmark::kMillisecond);

//// Benchmarks for version match
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionInsert)->Unit(benchmark::kMillisecond)->UseManualTime();

// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchRandomRead)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchUpdate)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchDelete)->Unit(benchmark::kMillisecond)->UseManualTime();
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchSequentialRead)->Unit(benchmark::kMillisecond);
//
//// Benchmarks for version mismatch
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchRandomRead)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchUpdate)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchDelete)->Unit(benchmark::kMillisecond)->UseManualTime();
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchSequentialRead)->Unit(benchmark::kMillisecond);

// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchScan)->Unit(benchmark::kMillisecond);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchScan)->Unit(benchmark::kMillisecond);

//// Benchmark for concurrent workload
// BENCHMARK_REGISTER_F(SqlTableBenchmark, ThroughputChangeSelect)->Unit(benchmark::kMillisecond)->Iterations(1);
//
//// Benchmark for concurrent workload
BENCHMARK_REGISTER_F(SqlTableBenchmark, ThroughputChangeUpdate)->Unit(benchmark::kMillisecond)->Iterations(1);

// BENCHMARK_REGISTER_F(SqlTableBenchmark, BlockThroughputChangeUpdate)->Unit(benchmark::kMillisecond)->Iterations(1);

// Limit the number of iterations to 4 because google benchmark can run multiple iterations. ATM sql table doesn't
// have implemented compaction so it will blow up memory
// BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentWorkload)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->Iterations(1);
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentWorkloadBlocking)
//    ->Unit(benchmark::kMillisecond)
//    ->UseManualTime()
//    ->Iterations(1);

// BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentSingleVersionRead)->Unit(benchmark::kMillisecond)->UseRealTime();
//
// BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentMultiVersionRead)->Unit(benchmark::kMillisecond)->UseRealTime();

}  // namespace terrier
