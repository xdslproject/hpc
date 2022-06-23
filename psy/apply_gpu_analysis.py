from xdsl.dialects.builtin import ModuleOp
from xdsl.ir import Operation, SSAValue, Region, Block
from xdsl.dialects.builtin import IntegerAttr, StringAttr, ArrayAttr, IntAttr
from xdsl.pattern_rewriter import (GreedyRewritePatternApplier,
                                   PatternRewriter, PatternRewriteWalker,
                                   RewritePattern, op_type_rewrite_pattern)

from ftn.dialects import ftn_dag
from psy.dialects import psy_gpu
from util.visitor import Visitor
from enum import Enum

class ApplyCoalesceToNestedLoops(RewritePattern):
      @op_type_rewrite_pattern
      def match_and_rewrite(  # type: ignore reportIncompatibleMethodOverride
            self, for_loop: ftn_dag.Do, rewriter: PatternRewriter):
        patentblock = for_loop.parent
        assert isinstance(patentblock, Block)
        idx = patentblock.ops.index(for_loop)
        for_loop.detach()
        
        collapsed_loop=psy_gpu.CollapsedParallelLoop.get([for_loop])
        rewriter.insert_op_at_pos(collapsed_loop, patentblock, idx)
        
class AccessMode(Enum):
    WRITE = 1
    READ = 2         
        
class CollectWrittenVariables(Visitor):      
  written_variables={}
  read_variables={}
  current_function_bound_vars=[]  
  currentMode=AccessMode.WRITE
  
  def populate_current_function_bound_vars(self, bound_variables):
    for var in bound_variables.data:
      self.current_function_bound_vars.append(var.data)
  
  def traverse_call_expr(self, call_expr: ftn_dag.CallExpr):
    self.current_function_bound_vars.clear()
    self.populate_current_function_bound_vars(call_expr.bound_variables)
    for op in call_expr.bound_function_instance.blocks[0].ops:
      self.traverse(op)
    self.current_function_bound_vars.clear()
      
  def traverse_binary_expr(self, binary_expr: ftn_dag.BinaryExpr):
    for op in binary_expr.lhs.blocks[0].ops:      
      self.traverse(op)
    for op in binary_expr.rhs.blocks[0].ops:      
      self.traverse(op)
      
  def traverse_array_access(self, member_access_expr: ftn_dag.ArrayAccess):
    for op in member_access_expr.var.blocks[0].ops:
      self.traverse(op)
      
  def traverse_member_access(self, member_access_expr: ftn_dag.MemberAccess):
    if member_access_expr.var.var_name.data in self.current_function_bound_vars:      
      if self.currentMode == AccessMode.WRITE:
        self.written_variables[member_access_expr.var.var_name.data]=member_access_expr
      else:
        self.read_variables[member_access_expr.var.var_name.data]=member_access_expr
      
  def traverse_expr_name(self, id_expr: ftn_dag.ExprName):
    if id_expr.var.var_name.data in self.current_function_bound_vars:
      if self.currentMode == AccessMode.WRITE:
        self.written_variables[id_expr.var.var_name.data]=id_expr
      else:            
        self.read_variables[id_expr.var.var_name.data]=id_expr  
      
  def traverse_assign(self, assign:ftn_dag.Assign):   
    self.currentMode=AccessMode.WRITE
    for op in assign.lhs.blocks[0].ops:
      self.traverse(op)
    self.currentMode=AccessMode.READ
    for op in assign.rhs.blocks[0].ops:
      self.traverse(op)
          
class ApplyGPURewriter(RewritePattern):
    @op_type_rewrite_pattern
    def match_and_rewrite(  # type: ignore reportIncompatibleMethodOverride
            self, for_loop: ftn_dag.Do, rewriter: PatternRewriter):        
        
        block = for_loop.parent
        assert isinstance(block, Block)        
        if isinstance(block.parent.parent, psy_gpu.CollapsedParallelLoop): return
        
        idx = block.ops.index(for_loop)
           
        for_loop.detach()       
         
        for region in for_loop.regions:
          for op in region.blocks[0].ops:
            walker = PatternRewriteWalker(GreedyRewritePatternApplier([ApplyCoalesceToNestedLoops()]), apply_recursively=False)
            walker.rewrite_module(op)
            
        visitor = CollectWrittenVariables()
        visitor.traverse(for_loop)
        
        copy_in_vars=[]
        copy_out_vars=[]
        create_vars=[]
        
        for key, value in visitor.written_variables.items():
          copy_out_vars.append(value.clone())
          if key not in visitor.read_variables:
            create_vars.append(value.clone())
            
        for key, value in visitor.read_variables.items():
          copy_in_vars.append(value.clone())
        
        gpu_loop = psy_gpu.ParallelLoop.get([for_loop], 10, 10, copy_in_vars, copy_out_vars,
                                             create_vars, [])
        rewriter.insert_op_at_pos(gpu_loop, block, idx)
        
class DetermineNumberOfCollapsedInnerLoops(Visitor):
  num_collapsed_loops=0
  
  def traverse_parallel_loop(self, parallel_loop:psy_gpu.ParallelLoop):
     self.num_collapsed_loops=0     
     self.traverse(parallel_loop.loop.blocks[0].ops[0])
     parallel_loop.attributes["num_inner_loops_to_collapse"]=IntAttr(self.num_collapsed_loops)
     
  def traverse_collapsed_parallel_loop(self, collapsed_parallel_loop:psy_gpu.CollapsedParallelLoop):
    self.num_collapsed_loops+=1
    self.traverse(collapsed_parallel_loop.loop.blocks[0].ops[0])

def apply_gpu_analysis(ctx: ftn_dag.MLContext, module: ModuleOp) -> ModuleOp:
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([
        ApplyGPURewriter(),
    ]), apply_recursively=False)
    walker.rewrite_module(module)
    
    visitor = DetermineNumberOfCollapsedInnerLoops()
    visitor.traverse(module)

    return module
