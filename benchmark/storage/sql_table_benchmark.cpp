#include <memory>
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

    CreateVersionTable();
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
    for (uint32_t i = 0; i < num_threads_; ++i) delete[] read_buffers_[i];
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

  void CreateVersionTable() {
    // create a version table acting like catalog
    LOG_INFO("Creating version table ...")
    catalog::col_oid_t col_oid(100);
    std::vector<catalog::Schema::Column> columns;
    columns.emplace_back("table_oid", type::TypeId::INTEGER, false, col_oid++);
    columns.emplace_back("version", type::TypeId::INTEGER, false, col_oid++);
    version_schema_ = new catalog::Schema(columns, storage::layout_version_t(0));
    version_table_ = new storage::SqlTable(&version_block_store_, *version_schema_, catalog::table_oid_t(1));

    // insert (123, 0) into version_table
    LOG_INFO("Inserting (123, 0) into version table ...")
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
    LOG_INFO("FINISHED");
  }

  storage::layout_version_t GetVersion(transaction::TransactionContext *txn, uint32_t id) {
    // need to know which version is it
    // TODO(yangjuns): we can avoid allocating space for your version potentially
    byte *version_buffer = common::AllocationUtil::AllocateAligned(version_initializer_->ProjectedRowSize());
    storage::ProjectedRow *version_row = version_initializer_->InitializeRow(version_buffer);
    // printf("version slot %p, %d\n", version_slot_.GetBlock(), version_slot_.GetOffset());
    bool succ = version_table_->Select(txn, version_slot_, version_row, *version_map_, storage::layout_version_t(0));
    if (!succ) {
      LOG_INFO("WTF??? SELECT FAILed!!");
    }
    byte *version_ptr = version_row->AccessWithNullCheck(version_map_->at(catalog::col_oid_t(101)));
    storage::layout_version_t my_version(0);
    if (version_ptr == nullptr) {
      LOG_INFO("{}, null version!! supprising", id);
    } else {
      my_version = storage::layout_version_t(*reinterpret_cast<uint32_t *>(version_ptr));
    }
    delete[] version_buffer;
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
  std::pair<uint32_t, storage::TupleSlot> GetHotSpotSlot(const std::vector<storage::TupleSlot> &slots) {
    common::SpinLatch::ScopedSpinLatch guard(&slot_latch_);
    // TODO(yangjuns): this hotspot index can be pre-generated
    double hot_spot_prob = 0.8;
    auto hot_spot_range = static_cast<uint32_t>(num_inserts_ * 0.2);
    uint32_t index = 0;
    std::uniform_real_distribution<> real_dist(0, 1);
    if (real_dist(generator_) < hot_spot_prob) {
      std::uniform_int_distribution<> int_dist(0, hot_spot_range - 1);
      index = int_dist(generator_);
    } else {
      std::uniform_int_distribution<> int_dist(hot_spot_range, num_inserts_ - 1);
      index = int_dist(generator_);
    }
    return {index, slots[index]};
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

  // slots lock
  common::SpinLatch slot_latch_;
  common::SpinLatch schema_latch_;
  // Sql Table
  storage::SqlTable *table_ = nullptr;
  storage::SqlTable *version_table_ = nullptr;

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
  const uint32_t column_num_ = 2;
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

  // GC
  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentWorkload)(benchmark::State &state) {
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
    bool aborted = false;
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
    auto slot_pair = GetHotSpotSlot(slots);
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
    auto slot_pair = GetHotSpotSlot(slots);
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

  auto schema_change = [&](uint32_t id) {
    auto txn = txn_manager_.BeginTransaction();
    storage::layout_version_t my_version = GetVersion(txn, id);
    bool aborted = false;
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
      aborted = false;
    } else {
      // someone else is updating the schema, abort
      txn_manager_.Abort(txn);
      aborted = true;
    }
    return aborted;
  };

  // NOLINTNEXTLINE
  for (auto _ : state) {
    StartGC(&txn_manager_);
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_txns_ / num_threads_; ++i) {
        uint32_t commited = RandomTestUtil::InvokeWorkloadWithDistribution(
            {read, insert, update, schema_change}, {id, id, id, id}, {0.7, 0.15, 0.1, 0.05}, &generator_);
        commited_txns_[id] += commited;
      }
    };
    common::WorkerPool thread_pool(num_threads_, {});
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
    }
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
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionScan)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);

  // insert tuples into old schema
  for (uint32_t i = 0; i < num_inserts_ / 2; ++i) {
    table_->Insert(&txn, *redo_, storage::layout_version_t(0));
  }

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
  for (uint32_t i = 0; i < num_inserts_ / 2; ++i) {
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

BENCHMARK_REGISTER_F(SqlTableBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionSequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchSequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchSequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionRandomRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchRandomRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchRandomRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentSingleVersionRead)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentMultiVersionRead)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionUpdate)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMatchUpdate)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchUpdate)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionScan)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionScan)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentWorkload)->Unit(benchmark::kMillisecond)->Repetitions(1);

}  // namespace terrier
