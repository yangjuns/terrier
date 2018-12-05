#include "catalog/catalog.h"
#include "catalog/database_handle.h"
#include "loggers/catalog_logger.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::catalog {

std::shared_ptr<Catalog> terrier_catalog;
std::atomic<uint32_t> oid_counter(0);

/* Initialization of catalog, including:
 * 1) create database_catalog.
 * create terrier database, create catalog tables, add them into
 * terrier database, insert columns into pg_attribute
 * 2) create necessary indexes, insert into pg_index
 * 3) insert terrier into pg_database, catalog tables into pg_table
 */
Catalog::Catalog() {
  CATALOG_LOG_TRACE("Initializing catalog ...");
  CATALOG_LOG_TRACE("Creating pg_database table ..,");
  // need to create schema
  // pg_database has {oid, datname}
  table_oid_t pg_database_oid(oid_counter);
  CATALOG_LOG_TRACE("pg_database_oid = {}", oid_counter);
  oid_counter++;

  // Columns
  std::vector<Schema::Column> cols;
  // oid
  cols.emplace_back("oid", type::TypeId::INTEGER, false, col_oid_t(oid_counter));
  CATALOG_LOG_TRACE("first col_oid = {}", oid_counter);
  oid_counter++;
  // datname
  cols.emplace_back("datname", type::TypeId::VARCHAR, false, col_oid_t(oid_counter));
  CATALOG_LOG_TRACE("second col_oid = {}", oid_counter);
  oid_counter++;

  // TODO: need to put this columns into pg_attribute for each database
  Schema schema(cols);
  pg_database_ = std::make_shared<storage::SqlTable>(&block_store_, schema, pg_database_oid);

  Bootstrap();
}

void EmptyCallback(void *){};

void Catalog::Bootstrap() {
  std::vector<col_oid_t> cols;
  for (uint32_t i = 0; i < pg_database_->GetSchema().GetColumns().size(); i++) {
    cols.emplace_back(pg_database_->GetSchema().GetColumns()[i].GetOid());
  }
  auto row_pair = pg_database_->InitializerForProjectedRow(cols);

  // need to create default database terrier

  // need to create a transaction

  // need to create record buffer segmentpool
  storage::RecordBufferSegmentPool buffer_pool{100, 100};

  // disable logging
  transaction::TransactionManager txn_manager(&buffer_pool, false, nullptr);
  auto txn_context = txn_manager.BeginTransaction();

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);

  // fill in the first attribute
  byte *first = insert->AccessForceNotNull(row_pair.second[cols[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = oid_counter++;

  // fill in the second attribute
  byte *second = insert->AccessForceNotNull(row_pair.second[cols[1]]);
  strcpy(reinterpret_cast<char *>(second), "terrier");

  pg_database_->Insert(txn_context, *insert);

  CATALOG_LOG_TRACE("inserted a row in pg_database");
  txn_manager.Commit(txn_context, EmptyCallback, nullptr);
  delete txn_context;
  delete[] row_buffer;
}
}  // namespace terrier::catalog