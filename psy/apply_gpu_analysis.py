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
        
class ApplySequentialRoutineToCalledProcedures(RewritePattern):
  def __init__(self, procedures_to_match):
    self.procedures_to_match=procedures_to_match
  
  @op_type_rewrite_pattern
  def match_and_rewrite(self, routine: ftn_dag.Routine, rewriter: PatternRewriter):
    if routine.routine_name.data in self.procedures_to_match:      
      op_list=[]
      for op in routine.routine_body.blocks[0].ops:
        op.detach()
        op_list.append(op)
          
      sequential_proc=psy_gpu.SequentialRoutineBody.get(op_list)      
      rewriter.insert_op_at_pos(sequential_proc, routine.routine_body.blocks[0], 0)
        
class AccessMode(Enum):
    WRITE = 1
    READ = 2    
    
class DetermineCalledProcedures(Visitor):
  def __init__(self):
    self.called_procedures=[]
    
  def visit_call_expr(self, call_expr: ftn_dag.CallExpr):
    self.called_procedures.append(call_expr.func.data)
        
class CollectWrittenVariables(Visitor): 
  
  def __init__(self):     
    self.written_variables={}
    self.read_variables={}
    self.loop_variables={}
    self.current_function_bound_vars=[]  
    self.currentMode=AccessMode.WRITE
  
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
        
  def traverse_do(self, dl: ftn_dag.Do):
    self.loop_variables[dl.iterator.blocks[0].ops[0].var.var_name.data]=dl.iterator.blocks[0].ops[0]
    self.traverse(dl.start.blocks[0].ops[0]) 
    self.traverse(dl.stop.blocks[0].ops[0])
    self.traverse(dl.step.blocks[0].ops[0])
    for op in dl.body.blocks[0].ops:
      self.traverse(op)
      
  def traverse_assign(self, assign:ftn_dag.Assign):   
    self.currentMode=AccessMode.WRITE
    for op in assign.lhs.blocks[0].ops:
      self.traverse(op)
    self.currentMode=AccessMode.READ
    for op in assign.rhs.blocks[0].ops:
      self.traverse(op)
          
class ApplyGPURewriter(RewritePattern):
    def __init__(self):
      self.called_procedures=[]
      
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
        private_vars=[]
        
        # Need plain copy too (copies in and out)
        for key, value in visitor.written_variables.items():
          if isinstance(value, ftn_dag.MemberAccess):
            copy_in_vars.append(ftn_dag.ExprName.create(attributes={"id": value.var.var_name, "var": value.var}))
          copy_out_vars.append(value.clone())          
            
        for key, value in visitor.read_variables.items():
          if key not in  visitor.loop_variables:
            if isinstance(value, ftn_dag.MemberAccess):
              copy_in_vars.append(ftn_dag.ExprName.create(attributes={"id": value.var.var_name, "var": value.var}))
            copy_in_vars.append(value.clone())
          
        for key,value in visitor.loop_variables.items():
          if isinstance(value, ftn_dag.MemberAccess):
            copy_in_vars.append(ftn_dag.ExprName.create(attributes={"id": value.var.var_name, "var": value.var}))
          private_vars.append(value.clone())
        
        gpu_loop = psy_gpu.ParallelLoop.get([for_loop], 10, 10, copy_in_vars, copy_out_vars,
                                             create_vars, private_vars)
        rewriter.insert_op_at_pos(gpu_loop, block, idx)
        
        dcpVisitor=DetermineCalledProcedures()
        dcpVisitor.traverse(gpu_loop)
        
        self.called_procedures.extend(dcpVisitor.called_procedures)
        
class DetermineNumberOfCollapsedInnerLoops(Visitor):
  num_collapsed_loops=0
  
  def traverse_parallel_loop(self, parallel_loop:psy_gpu.ParallelLoop):
     self.num_collapsed_loops=0     
     self.traverse(parallel_loop.loop.blocks[0].ops[0])
     parallel_loop.attributes["num_inner_loops_to_collapse"]=IntAttr(self.num_collapsed_loops)
     
  def traverse_collapsed_parallel_loop(self, collapsed_parallel_loop:psy_gpu.CollapsedParallelLoop):
    self.num_collapsed_loops+=1
    self.traverse(collapsed_parallel_loop.loop.blocks[0].ops[0])
    
class CombineGPUParallelLoopsForDataRegionRewriter(RewritePattern):
  @op_type_rewrite_pattern
  def match_and_rewrite(self, parallel_loop: psy_gpu.ParallelLoop, rewriter: PatternRewriter):
    block = parallel_loop.parent
    if isinstance(block.parent.parent, psy_gpu.DataRegion): return        
    start_idx = block.ops.index(parallel_loop)
    idx=start_idx
    for idx in range(start_idx, len(block.ops)):
      if not isinstance(block.ops[idx], psy_gpu.ParallelLoop):
        idx-=1 
        break    
    total_parallel_loops=((idx+1)-start_idx)
    if total_parallel_loops > 1:      
      parallel_loop_block = Block()
      copyin_vars={}
      copyout_vars={}
      create_vars={}
      # Go backwards to preserve indexes      
      for idx in range(total_parallel_loops-1, start_idx-1, -1):        
        parallel_loop=block.ops[idx]
        parallel_loop.detach()
        self.handle_data_accesses(copyin_vars, copyout_vars, create_vars, parallel_loop)                
        parallel_loop_block.insert_op(parallel_loop, 0)
                      
      parallel_loops_region = Region()
      parallel_loops_region.add_block(parallel_loop_block)
      data_region=psy_gpu.DataRegion.get(parallel_loops_region, list(copyin_vars.values()), 
                                         list(copyout_vars.values()), list(create_vars.values()))
      rewriter.insert_op_at_pos(data_region, block, start_idx)
      
  def get_concrete_var_name(self, op):
    if isinstance(op, ftn_dag.ExprName):
      return op.var.var_name
    elif isinstance(op, ftn_dag.ArrayAccess):
      return get_concrete_var_name(op.var.blocks[0].ops[0])
    elif isinstance(op, ftn_dag.MemberAccess):
      # Do the append here as we need both the top level type and the data type too
      return StringAttr(op.var.var_name.data+self.get_field_names(op.fields))
    else:
      return None
    
  def get_field_names(self, field_array):
    str=""    
    for entry in field_array.data:      
      str+="%"+entry.data
    return str
    
  def handle_data_accesses(self, copyin_vars, copyout_vars, create_vars, parallel_loop):
    for op in parallel_loop.copy_in_vars.blocks[0].ops:
      op.detach()
      var_name=self.get_concrete_var_name(op)
      if var_name is not None: copyin_vars[var_name]=op
    for op in parallel_loop.copy_out_vars.blocks[0].ops:
      op.detach()
      var_name=self.get_concrete_var_name(op)
      if var_name is not None: copyout_vars[var_name]=op
    for op in parallel_loop.create_vars.blocks[0].ops:
      op.detach()
      var_name=self.get_concrete_var_name(op)
      if var_name is not None: create_vars[var_name]=op

def apply_gpu_analysis(ctx: ftn_dag.MLContext, module: ModuleOp) -> ModuleOp:
    applyGPURewriter=ApplyGPURewriter()
    walker = PatternRewriteWalker(GreedyRewritePatternApplier([applyGPURewriter]), apply_recursively=False)
    walker.rewrite_module(module)    
    
    walker=PatternRewriteWalker(GreedyRewritePatternApplier([ApplySequentialRoutineToCalledProcedures(applyGPURewriter.called_procedures)]), apply_recursively=False)
    walker.rewrite_module(module)
    
    visitor = DetermineNumberOfCollapsedInnerLoops()
    visitor.traverse(module)  
    
    walker=PatternRewriteWalker(GreedyRewritePatternApplier([CombineGPUParallelLoopsForDataRegionRewriter()]), apply_recursively=False)
    walker.rewrite_module(module)      

    #print(1 + "J")
    return module
