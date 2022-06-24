from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Type, Union
from xdsl.dialects.builtin import IntegerAttr, StringAttr, IntegerType, Float32Type, i32, f32, ArrayAttr, BoolAttr, IntAttr
from xdsl.ir import Data, MLContext, Operation, ParametrizedAttribute
from xdsl.irdl import (AnyOf, AttributeDef, SingleBlockRegionDef, builder, AnyAttr, ResultDef, OperandDef,
                       irdl_attr_definition, irdl_op_definition, ParameterDef)

@irdl_op_definition
class DataRegion(Operation):
  name = "psy.gpu.dataregion"
  
  contents=SingleBlockRegionDef()
  copy_in_vars= SingleBlockRegionDef()
  copy_out_vars= SingleBlockRegionDef()
  create_vars= SingleBlockRegionDef()
  
  @staticmethod
  def get(contents: List[Operation],          
          copy_in_vars: List[Operation],
          copy_out_vars: List[Operation],
          create_vars: List[Operation],         
          verify_op: bool = True) -> ParallelLoop:
    res = DataRegion.build(regions=[contents, copy_in_vars, copy_out_vars, create_vars])
    if verify_op:
        res.verify(verify_nested_ops=False)
    return res

  def verify_(self) -> None:
    pass      

@irdl_op_definition
class ParallelLoop(Operation):
  name = "psy.gpu.parallelloop"
    
  loop = SingleBlockRegionDef()
    
  vector_length = AttributeDef(IntAttr)
  num_workers = AttributeDef(IntAttr)
  num_inner_loops_to_collapse = AttributeDef(IntAttr)
    
  copy_in_vars= SingleBlockRegionDef()
  copy_out_vars= SingleBlockRegionDef()
  create_vars= SingleBlockRegionDef()
  private_vars= SingleBlockRegionDef()
    
  @staticmethod
  def get(loop: List[Operation],
          vector_length: int,
          num_workers: int,
          copy_in_vars: List[Operation],
          copy_out_vars: List[Operation],
          create_vars: List[Operation],
          private_vars: List[Operation],
          num_inner_loops_to_collapse=0,
          verify_op: bool = True) -> ParallelLoop:
    res = ParallelLoop.build(attributes={"vector_length": vector_length, "num_workers": num_workers,
                                          "num_inner_loops_to_collapse": num_inner_loops_to_collapse}, 
                                          regions=[loop, copy_in_vars, copy_out_vars, create_vars, private_vars])
    if verify_op:
        res.verify(verify_nested_ops=False)
    return res

  def verify_(self) -> None:
    pass    

@irdl_op_definition    
class CollapsedParallelLoop(Operation):
  name = "psy.gpu.collapsedparallelloop"
    
  loop = SingleBlockRegionDef()  
    
  @staticmethod
  def get(loop: List[Operation],
          verify_op: bool = True) -> CollapsedParallelLoop:
    res = CollapsedParallelLoop.build(regions=[loop])
    if verify_op:
      res.verify(verify_nested_ops=False)
    return res

  def verify_(self) -> None:
    pass
    
@irdl_op_definition    
class SequentialRoutine(Operation):
  name = "psy.gpu.sequentialroutine"
    
  routine = SingleBlockRegionDef()
    
  @staticmethod
  def get(proc: List[Operation],
          verify_op: bool = True) -> SequentialRoutine:
    res = SequentialRoutine.build(regions=[proc])
    if verify_op:
      res.verify(verify_nested_ops=False)
    return res

  def verify_(self) -> None:
    pass
    