from xdsl.dialects.builtin import ModuleOp
from xdsl.ir import Operation, SSAValue, Region, Block
from xdsl.dialects.builtin import IntegerAttr, StringAttr, ArrayAttr, IntAttr
from xdsl.pattern_rewriter import (GreedyRewritePatternApplier,
                                   PatternRewriter, PatternRewriteWalker,
                                   RewritePattern, op_type_rewrite_pattern,
                                   op_type_entry_pattern, op_type_exit_pattern)

from ftn.dialects import ftn_dag, ftn_type
from psy.dialects import psy_pgas
from util.visitor import Visitor
from enum import Enum

class CollectAccessedVariables(Visitor):
  def __init__(self):
    self.accessed_variables={}

  def clear(self):
    self.accessed_variables.clear()

  def traverse_call_expr(self, call_expr: ftn_dag.CallExpr):
    for op in call_expr.bound_function_instance.blocks[0].ops:
      self.traverse(op)

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)

  def traverse_array_access(self, member_access_expr: ftn_dag.ArrayAccess):
    var_handle=member_access_expr.var.blocks[0].ops[0]
    entry=[var_handle.clone()]
    accessors=[]
    for op in member_access_expr.accessors.blocks[0].ops:
      accessors.append(op.clone())
    entry.append(accessors)
    self.accessed_variables[var_handle.var.var_name.data]=entry

  def traverse_data_region(self, dataregion: psy_pgas.DataRegion):
    pass


class ApplyPGASRewriter(RewritePattern):
    def __init__(self):
      self.called_procedures=[]

    def findContainingStatement(self, block):
      while block is not None:
        if isinstance(block, ftn_dag.Assign): return block
        block = block.parent
      return None

    @op_type_rewrite_pattern
    def match_and_rewrite(  # type: ignore reportIncompatibleMethodOverride
            self, assignment: ftn_dag.Assign, rewriter: PatternRewriter):

        block = assignment.parent
        idx = block.ops.index(assignment)

        read_vars=[]
        write_vars=[]

        visitor = CollectAccessedVariables()
        for op in assignment.lhs.blocks[0].ops:
          visitor.traverse(op)
        for key, value in visitor.accessed_variables.items():
          write_vars.append(psy_pgas.SingleDataItem.get([value[0]], value[1]))

        visitor.clear()
        for op in assignment.rhs.blocks[0].ops:
          visitor.traverse(op)
        for key, value in visitor.accessed_variables.items():
          read_vars.append(psy_pgas.SingleDataItem.get([value[0]], value[1]))

        if len(read_vars) > 0 or len(write_vars) > 0:
          assignment.detach()
          data_region = psy_pgas.DataRegion.get([assignment], read_vars, write_vars)
          rewriter.insert_op_at_pos(data_region, block, idx)

class CollectIndexVariables(Visitor):
  def __init__(self):
    self.index_vars=[]

  def clear(self):
    self.index_vars.clear()

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)

  def traverse_expr_name(self, id_expr: ftn_dag.ExprName):
    self.index_vars.append(id_expr.var)

  def traverse_member_access(self, member_access):
    name=member_access.var.var_name.data
    for member in member_access.fields.data:
      name+="%"+member.data
    new_var=ftn_dag.Token([StringAttr(name), member_access.var.type])
    self.index_vars.append(new_var)

  def traverse_vector_data_item(self, vdi):
    for op in vdi.indexes.blocks[0].ops:
      self.traverse(op)

  def traverse_range_index(self, ri):
    for op in ri.index_from.blocks[0].ops:
      self.traverse(op)
    for op in ri.index_to.blocks[0].ops:
      self.traverse(op)

class CollectDataRegions(Visitor):
  def __init__(self, loop_iterator_var):
    self.applicable_input_items=[]
    self.applicable_output_items=[]

    self.loop_iterator_var=loop_iterator_var
    self.loop_iterator_var_names=[]
    for v in self.loop_iterator_var:
      self.loop_iterator_var_names.append(v.var_name.data)

  def has_some_data_items(self):
    return len(self.applicable_input_items) > 0 or len(self.applicable_output_items) > 0

  def traverse_call_expr(self, call_expr: ftn_dag.CallExpr):
    for op in call_expr.bound_function_instance.blocks[0].ops:
      self.traverse(op)

  def check_is_var_indexes_iterated_here(self, var):
    index_collector=CollectIndexVariables()
    for op in var.indexes.blocks[0].ops:
      index_collector.traverse(op)
    # For now just key on the name, might need to make more complex in future
    for index in index_collector.index_vars:
      if not index.var_name.data in self.loop_iterator_var_names: return False
    return True

  def traverse_data_region(self, data_region: psy_pgas.DataRegion):
    for var in data_region.inputs.blocks[0].ops:
      if self.check_is_var_indexes_iterated_here(var):
        self.applicable_input_items.append((data_region, var))
    for var in data_region.outputs.blocks[0].ops:
      if self.check_is_var_indexes_iterated_here(var):
        self.applicable_output_items.append((data_region, var))

class CollectPGASDataRegions(RewritePattern):
  def __init__(self):
    # Won't work with different loops not nested as needs to remove a loop when leaves scope
    self.processed_loop_vars=[]

  @op_type_rewrite_pattern
  def match_and_rewrite(  # type: ignore reportIncompatibleMethodOverride
            self, do: ftn_dag.Do, rewriter: PatternRewriter):

    block = do.parent
    idx = block.ops.index(do)

    self.processed_loop_vars.append(do.iterator.blocks[0].ops[0].var)
    visitor = CollectDataRegions(self.processed_loop_vars)
    for op in do.body.blocks[0].ops:
      visitor.traverse(op)

    read_vars=[]
    write_vars=[]
    data_regions_being_worked_with=set()
    if visitor.has_some_data_items():
      for entry in visitor.applicable_input_items:
        read_vars.append(entry[1])
        entry[1].detach()
        data_regions_being_worked_with.add(entry[0])
      for entry in visitor.applicable_output_items:
        write_vars.append(entry[1])
        entry[1].detach()
        data_regions_being_worked_with.add(entry[0])

      for dr in list(data_regions_being_worked_with):
        if len(dr.outputs.blocks[0].ops)==0 and len(dr.inputs.blocks[0].ops)==0:
          # The Data region is no longer needed as all data items will be promoted to this level, therefore remove
          parent=dr.parent
          parent_idx=parent.ops.index(dr)
          dr.detach()
          a=dr.contents.blocks[0].ops[0]
          dr.contents.blocks[0].ops[0].detach()
          rewriter.insert_op_at_pos(a, parent, parent_idx)

      do_loop_blocks=[]
      for block in do.body.blocks:
        do.body.detach_block(block)
        do_loop_blocks.append(block)
      combined_data_region = psy_pgas.DataRegion.get(do_loop_blocks, read_vars, write_vars)
      block=Block()
      block.add_op(combined_data_region)
      do.body.add_block(block)

class FindAppropriateLoop(Visitor):
  def __init__(self, single_data_item):
    self.single_data_item=single_data_item
    index_collector=CollectIndexVariables()
    for op in var.indexes.blocks[0].ops:
      index_collector.traverse(op)
    self.index_vars=index_collector.index_vars

class CollectIndexVariablesAndExpr(Visitor):
  def __init__(self):
    self.index_vars={}

  def clear(self):
    self.index_vars.clear()

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)

  def traverse_expr_name(self, id_expr: ftn_dag.ExprName):
    if not id_expr.var.var_name.data in self.index_vars.keys():
      self.index_vars[id_expr.var.var_name.data]=[]
    self.index_vars[id_expr.var.var_name.data].append(id_expr)

class CombineDataItemsIntoRanges(RewritePattern):
  def __init__(self):
    self.parent_loops={}
    self.parent_loops_ordered=[]

  @op_type_entry_pattern
  def entry_operation(self, do:ftn_dag.Do):
    self.parent_loops[do.iterator.blocks[0].ops[0].var.var_name.data]=do
    self.parent_loops_ordered.append(do)

  @op_type_exit_pattern
  def exit_operation(self, do:ftn_dag.Do):
    del self.parent_loops[do.iterator.blocks[0].ops[0].var.var_name.data]
    self.parent_loops_ordered.pop()

  def check_is_var_indexes_iterated_here(self, var):
    index_collector=CollectIndexVariables()
    for op in var.indexes.blocks[0].ops:
      index_collector.traverse(op)
    # For now just key on the name, might need to make more complex in future
    for index in index_collector.index_vars:
      if not index.var_name.data in self.parent_loops.keys(): return False
    return True

  def find_top_level_applicable_loop(self, var):
    index_collector=CollectIndexVariables()
    for op in var.indexes.blocks[0].ops:
      index_collector.traverse(op)
    lowest_loop=len(self.parent_loops_ordered)
    for index in index_collector.index_vars:
      potential_lowest_loop=self.parent_loops_ordered.index(self.parent_loops[index.var_name.data])
      if potential_lowest_loop < lowest_loop: lowest_loop=potential_lowest_loop
    return lowest_loop

  def replace_op_with_range(self, index_collector, op, isStart):
    new_op=op.clone()
    index_collector.clear()
    index_collector.traverse(new_op)
    for key, value in index_collector.index_vars.items():
      # From this grab the name and then can deduce the loop by a look up in the collection
      corresponding_loop=self.parent_loops[key]
      for expr_to_replace in value:
        if expr_to_replace.parent is not None:
          # Replace index_entry (an expression) with the name from the loop and it's a range - therefore duplicate binary op etc from start to end
          parent_block=expr_to_replace.parent
          idx=parent_block.ops.index(expr_to_replace)
          expr_to_replace.detach()
          if isStart:
            parent_block.insert_op(corresponding_loop.start.blocks[0].ops[0].clone(), idx)
          else:
            parent_block.insert_op(corresponding_loop.stop.blocks[0].ops[0].clone(), idx)
        else:
          if isStart:
            new_op=corresponding_loop.start.blocks[0].ops[0].clone()
          else:
            new_op=corresponding_loop.stop.blocks[0].ops[0].clone()
    return new_op

  def find_or_add_data_region_above_loop(self, loop):
    if isinstance(loop.parent_op(), psy_pgas.DataRegion):
      return loop.parent_op()
    immediate_parent=loop.parent
    idx=loop.parent.ops.index(loop)
    loop.detach()
    newdr=psy_pgas.DataRegion.get([loop], [], [])
    immediate_parent.insert_op(newdr, idx)
    return newdr

  def move_single_data_items_to_ranges(self, singledataitem, isInput):
    if self.check_is_var_indexes_iterated_here(singledataitem):
      # Need this to append the range above this loop
      top_level_loop_index=self.find_top_level_applicable_loop(singledataitem)
      index_collector=CollectIndexVariablesAndExpr()
      ranges=[]
      for op in singledataitem.indexes.blocks[0].ops:
        start_op=self.replace_op_with_range(index_collector, op, True)
        stop_op=self.replace_op_with_range(index_collector, op, False)
        newrange=psy_pgas.RangeIndex.get([start_op], [stop_op])
        ranges.append(newrange)
      vdi=psy_pgas.VectorDataItem.get([singledataitem.variable.blocks[0].ops[0].clone()], ranges)
      new_dr=self.find_or_add_data_region_above_loop(self.parent_loops_ordered[top_level_loop_index])
      if isInput:
        new_dr.inputs.blocks[0].add_op(vdi)
      else:
        new_dr.outputs.blocks[0].add_op(vdi)
      singledataitem.detach()

  @op_type_rewrite_pattern
  def match_and_rewrite(self, data_region: psy_pgas.DataRegion, rewriter: PatternRewriter):
    if len(self.parent_loops) > 0:
      for singledataitem in data_region.inputs.blocks[0].ops:
        self.move_single_data_items_to_ranges(singledataitem, True)
      for singledataitem in data_region.outputs.blocks[0].ops:
        self.move_single_data_items_to_ranges(singledataitem, False)
      # Now check if the data_region is empty, if so then remove it!
      if len(data_region.inputs.blocks[0].ops) == 0 and len(data_region.outputs.blocks[0].ops) == 0:
        parent=data_region.parent
        idx=parent.ops.index(data_region)
        data_region.detach()
        op_to_insert=data_region.contents.blocks[0].ops[0]
        op_to_insert.detach()
        parent.insert_op(op_to_insert, idx)

class CollectIndexExprAssociatedWithIndex(Visitor):
  def __init__(self, index_var):
    self.index_var=index_var
    self.index_expr=None

  def clear(self):
    self.index_var=None
    self.index_expr=None

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)

  def traverse_expr_name(self, id_expr: ftn_dag.ExprName):
    if not id_expr.var.var_name.data in self.index_vars.keys():
      self.index_vars[id_expr.var.var_name.data]=[]
    self.index_vars[id_expr.var.var_name.data].append(id_expr)

class GetAccessedVarName(Visitor):
  def __init__(self):
    self.accessed_var_name=None

  def clear(self):
    self.accessed_var_name=None

  def traverse_call_expr(self, call_expr: ftn_dag.CallExpr):
    for op in call_expr.bound_function_instance.blocks[0].ops:
      self.traverse(op)

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)

  def traverse_array_access(self, member_access_expr: ftn_dag.ArrayAccess):
    self.accessed_var_name=member_access_expr.var.var_name.data

  def traverse_member_access(self, member_access_expr: ftn_dag.MemberAccess):
    self.accessed_var_name=member_access_expr.var.var_name.data

class RangeDirection(Enum):
  UNKNOWN = 0
  FROM = 1
  TO = 2

class GetIndexRangeExpression(Visitor):
  def __init__(self, var_name):
    self.range_expression=None
    self.var_to_find_name=var_name
    self.found=False
    self.direction=RangeDirection.UNKNOWN

  def clear(self):
    self.found=False
    self.range_expression=None
    self.direction=RangeDirection.UNKNOWN

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    if self.found: return
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
      if self.found: return
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)
      if self.found: return

  def traverse_expr_name(self, id_expr: ftn_dag.ExprName):
    if self.found: return
    if (id_expr.var.var_name.data == self.var_to_find_name):
      self.found=True

  def traverse_member_access(self, member_access):
    if self.found: return
    name=member_access.var.var_name.data
    for member in member_access.fields.data:
      name+="%"+member.data
    if (name == self.var_to_find_name):
      self.found=True

  def traverse_vector_data_item(self, vdi):
    if self.found: return
    for op in vdi.indexes.blocks[0].ops:
      self.traverse(op)

  def traverse_range_index(self, ri):
    if self.found: return
    self.direction=RangeDirection.FROM
    for op in ri.index_from.blocks[0].ops:
      self.range_expression=op
      self.traverse(op)
      if self.found: return
      self.range_expression=None
    self.direction=RangeDirection.TO
    for op in ri.index_to.blocks[0].ops:
      self.range_expression=op
      self.traverse(op)
      if self.found: return
      self.range_expression=None
    self.direction=RangeDirection.UNKNOWN

class CalculateIndexAppliedOffset(Visitor):
  def __init__(self):
    self.offset=0
    self.op=None

  def clear(self):
    self.offset=0
    self.op=None

  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    self.op=binary_expr.op.data
    for op in binary_expr.lhs.blocks[0].ops:
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:
      self.traverse(op)

  def traverse_literal(self, literal):
    # For now only handle literal offsets - TODO: Add variable and runtime check for these
    if self.op == "+":
      self.offset+=literal.value.value.data
    elif self.op == "-":
      self.offset-=literal.value.value.data

class ConcatenateRanges(RewritePattern):
  def check_if_matching(self, potential_dataitem, var_name, indexes):
    cav=GetAccessedVarName()
    cav.traverse(potential_dataitem.variable.blocks[0].ops[0])
    if cav.accessed_var_name != var_name: return False

    index_collector=CollectIndexVariables()
    index_collector.traverse(potential_dataitem)
    if len(indexes) != len(index_collector.index_vars): return False
    indexes_names=[]
    for item in indexes: indexes_names.append(item.var_name.data)
    for item in index_collector.index_vars:
      if item.var_name.data not in indexes_names: return False
    return True

  def get_match_expr_and_direction(self, var_name, item_to_process):
    ire=GetIndexRangeExpression(var_name)
    ire.traverse(item_to_process)
    return ire.range_expression, ire.direction

  def compare_and_swap_index_if_required(self, index, dataitem_A, dataitem_B):
    A_expr, A_direction=self.get_match_expr_and_direction(index.var_name.data, dataitem_A)
    B_expr, B_direction=self.get_match_expr_and_direction(index.var_name.data, dataitem_B)

    offset_extractor=CalculateIndexAppliedOffset()
    offset_extractor.traverse(A_expr)
    A_offset=offset_extractor.offset
    offset_extractor.clear()
    offset_extractor.traverse(B_expr)
    B_offset=offset_extractor.offset

    if A_direction == RangeDirection.FROM:
      # Lowest number is best here
      if B_offset < A_offset:
        # Need to replace orig expr with to match expression
        cloned_to_match=B_expr.clone()
        parent=A_expr.parent
        idx=parent.ops.index(A_expr)
        A_expr.detach()
        parent.insert_op(cloned_to_match, idx)
    elif A_direction == RangeDirection.TO:
      # Highest number is best here
      if B_offset > A_offset:
        # Need to replace orig expr with to match expression
        cloned_to_match=B_expr.clone()
        parent=A_expr.parent
        idx=parent.ops.index(A_expr)
        A_expr.detach()
        parent.insert_op(cloned_to_match, idx)

  def find_same_data_items(self, alldataitems, dataitem, var_name, indexes):
    combined_indexes=[]
    for data_item_idx, comparison_item in enumerate(alldataitems):
      if dataitem != comparison_item:
        if self.check_if_matching(comparison_item, var_name, indexes):
          for index in indexes:
            self.compare_and_swap_index_if_required(index, dataitem, comparison_item)
          combined_indexes.append(data_item_idx)
    return combined_indexes

  def process_vectordataitem_for_concatenation(self, dataitem, alldataitems):
    index_collector=CollectIndexVariables()
    index_collector.traverse(dataitem)
    cav=GetAccessedVarName()
    cav.traverse(dataitem.variable.blocks[0].ops[0])
    return self.find_same_data_items(alldataitems, dataitem, cav.accessed_var_name, index_collector.index_vars)

  def process_region(self, region):
    indexes_to_ignore=[]
    for idx, dataitem in enumerate(region.blocks[0].ops):
      if isinstance(dataitem, psy_pgas.VectorDataItem) and idx not in indexes_to_ignore:
        indexes_to_ignore.extend(self.process_vectordataitem_for_concatenation(dataitem, region.blocks[0].ops))
    indexes_to_ignore.sort(reverse=True)
    for idx in indexes_to_ignore:
      region.blocks[0].erase_op(idx)

  @op_type_rewrite_pattern
  def match_and_rewrite(self, data_region: psy_pgas.DataRegion, rewriter: PatternRewriter):
    self.process_region(data_region.inputs)
    self.process_region(data_region.outputs)

def apply_pgas_analysis(ctx: ftn_dag.MLContext, module: ModuleOp) -> ModuleOp:
    applyPGASRewriter=ApplyPGASRewriter()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([applyPGASRewriter]), apply_recursively=False, walk_regions_first=False)
    walker.rewrite_module(module)

    # Do these separately as need to do the pgas data item locations before we collect them together
    collector=CollectPGASDataRegions()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([collector]), apply_recursively=False, walk_regions_first=False)
    walker.rewrite_module(module)

    combiner=CombineDataItemsIntoRanges()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([combiner]), apply_recursively=False, walk_regions_first=False)
    walker.rewrite_module(module)

    concatenator=ConcatenateRanges()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([concatenator]), apply_recursively=False, walk_regions_first=False)
    walker.rewrite_module(module)

    #print("a"+3)
    return module
