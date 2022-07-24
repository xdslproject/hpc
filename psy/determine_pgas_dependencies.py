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
          
class CollectPGASDataRegions(RewritePattern):
  def __init__(self):
    pass
    
  @op_type_rewrite_pattern
    def match_and_rewrite(  # type: ignore reportIncompatibleMethodOverride
            self, assignment: ftn_dag.Do, rewriter: PatternRewriter):
        

def apply_pgas_analysis(ctx: ftn_dag.MLContext, module: ModuleOp) -> ModuleOp:
    applyPGASRewriter=ApplyPGASRewriter()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([applyPGASRewriter]), apply_recursively=False, walk_regions_first=True)
    walker.rewrite_module(module)
    
    #print("a"+3)
    return module
