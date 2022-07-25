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
          write_vars.append(psy_pgas.DataItem.get([value[0]], value[1]))

        visitor.clear()
        for op in assignment.rhs.blocks[0].ops:
          visitor.traverse(op)  
        for key, value in visitor.accessed_variables.items():          
          read_vars.append(psy_pgas.DataItem.get([value[0]], value[1]))

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
    
    self.loop_iterator_var=[loop_iterator_var]
    self.loop_iterator_var_names=[]
    for v in self.loop_iterator_var:
      loop_iterator_var_names.append(v.var_name.data)
      
  def check_is_var_indexes_iterated_here(self, var):
    index_collector=CollectIndexVariables()
    index_collector.traverse(var.indexes)      
    # For now just key on the name, might need to make more complex in future
    for index in index_collector.index_vars:
      if not index.var_name.data in self.loop_iterator_var_names: return False
    return True
      
  def traverse_data_region(self, data_region: psy_pgas.DataRegion):
    for var in data_region.inputs.blocks[0].ops:
      if self.check_is_var_indexes_iterated_here(var):
        self.applicable_input_items.append(var)
    for var in data_region.outputs.blocks[0].ops:
      if self.check_is_var_indexes_iterated_here(var):
        self.applicable_output_items.append(var)       
        
        # store pair here, the data region and the data item - as will need to remove from data region and then check if empty or not (to remove entirely)
          
class CollectPGASDataRegions(RewritePattern):
  def __init__(self):
    pass
    
  @op_type_rewrite_pattern
  def match_and_rewrite(  # type: ignore reportIncompatibleMethodOverride
            self, do: ftn_dag.Do, rewriter: PatternRewriter):
              
    block = do.parent
    idx = block.ops.index(do)
    
    print(do.iterator.blocks[0].ops[0].var.var_name.data)
    visitor = CollectDataRegions(do.iterator.blocks[0].ops[0].var)
    for op in do.body.blocks[0].ops:
      visitor.traverse(op)
      
    read_vars=[]
    write_vars=[]
    if len(visitor.data_regions) > 0:
      for data_region in visitor.data_regions:
        for op in data_region.inputs.blocks[0].ops:
          read_vars.append(op)
          op.detach()
        for op in data_region.outputs.blocks[0].ops:
          write_vars.append(op)
          op.detach()
        parent=data_region.parent
        parent_idx=parent.ops.index(data_region)        
        data_region.detach()
        a=data_region.contents.blocks[0].ops[0]
        data_region.contents.blocks[0].ops[0].detach()
        rewriter.insert_op_at_pos(a, parent, parent_idx)
        
      do.detach()      
      combined_data_region = psy_pgas.DataRegion.get([do], read_vars, write_vars) 
      rewriter.insert_op_at_pos(combined_data_region, block, idx)

def apply_pgas_analysis(ctx: ftn_dag.MLContext, module: ModuleOp) -> ModuleOp:
    applyPGASRewriter=ApplyPGASRewriter()
    collector=CollectPGASDataRegions()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([applyPGASRewriter, collector]), apply_recursively=False, walk_regions_first=True)
    walker.rewrite_module(module)          
    
    print("a"+3)
    return module