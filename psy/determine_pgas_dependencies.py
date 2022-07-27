from xdsl.dialects.builtin import ModuleOp
from xdsl.ir import Operation, SSAValue, Region, Block
from xdsl.dialects.builtin import IntegerAttr, StringAttr, ArrayAttr, IntAttr
from xdsl.pattern_rewriter import (GreedyRewritePatternApplier,
                                   PatternRewriter, PatternRewriteWalker,
                                   RewritePattern, op_type_rewrite_pattern)

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
    
  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:      
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:      
      self.traverse(op)
      
  def traverse_expr_name(self, id_expr: ftn_dag.ExprName): 
    self.index_vars.append(id_expr.var)  
          
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
    print(call_expr.regions[0].blocks[0].ops)
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

def apply_pgas_analysis(ctx: ftn_dag.MLContext, module: ModuleOp) -> ModuleOp:
    applyPGASRewriter=ApplyPGASRewriter()
    collector=CollectPGASDataRegions()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([applyPGASRewriter]), apply_recursively=False, walk_regions_first=False)
    walker.rewrite_module(module)
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([collector]), apply_recursively=False, walk_regions_first=False)
    walker.rewrite_module(module)
    
    #print("a"+3)
    return module
