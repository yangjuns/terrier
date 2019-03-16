#pragma once

#include "output_schema.h"
#include "parser/parameter.h"
#include "parser/update_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {

namespace storage {
class SqlTable;
}
// TODO(Gus,Wen) Add back VisitParameters, SetParamaterValues, and PerformBinding

namespace plan_node {

class UpdatePlanNode : public AbstractPlanNode {
 public:
  UpdatePlanNode() = delete;

  /**
   * Instantiate an UpdatePlanNode
   * @param target_table the target table to operate on
   * @param output_schema the output columns and mapping information
   */
  UpdatePlanNode(std::shared_ptr<storage::SqlTable> target_table, std::shared_ptr<OutputSchema> output_schema);

  /**
   * Default destructor
   */
  ~UpdatePlanNode() override;

  /**
   * @return the target table to operate on
   */
  std::shared_ptr<storage::SqlTable> GetTargetTable() const { return target_table_; }

  /**
   * @return whether to update primary key
   */
  bool GetUpdatePrimaryKey() const { return update_primary_key_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UPDATE; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "UpdatePlanNode"; }

  /**
   * @return a unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new UpdatePlanNode(target_table_, GetOutputSchema()));
  }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  // The target table to operate on
  std::shared_ptr<storage::SqlTable> target_table_;

  // Whether to update primary key
  bool update_primary_key_;

 private:
  DISALLOW_COPY_AND_MOVE(UpdatePlanNode);
};

}  // namespace plan_node
}  // namespace terrier