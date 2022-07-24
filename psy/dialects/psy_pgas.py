from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Type, Union
from xdsl.dialects.builtin import IntegerAttr, StringAttr, IntegerType, Float32Type, i32, f32, ArrayAttr, BoolAttr, IntAttr
from xdsl.ir import Data, MLContext, Operation, ParametrizedAttribute
from xdsl.irdl import (AnyOf, AttributeDef, SingleBlockRegionDef, builder, AnyAttr, ResultDef, OperandDef,
                       irdl_attr_definition, irdl_op_definition, ParameterDef)

@irdl_op_definition
class DataRegion(Operation):
  name = "psy.pgas.dataregion"

  contents=SingleBlockRegionDef()
  inputs= SingleBlockRegionDef()
  outputs= SingleBlockRegionDef()

  @staticmethod
  def get(contents: List[Operation],
          inputs: List[Operation],
          outputs: List[Operation],
          verify_op: bool = True) -> DataRegion:
    res = DataRegion.build(regions=[contents, inputs, outputs])
    if verify_op:
        res.verify(verify_nested_ops=False)
    return res

  def verify_(self) -> None:
    pass

@irdl_op_definition
class DataItem(Operation):
  name = "psy.pgas.dataitem"
  
  variable=SingleBlockRegionDef()
  indexes=SingleBlockRegionDef()
  
  @staticmethod
  def get(variable: List[Operation],
          indexes: List[Operation],          
          verify_op: bool = True) -> DataItem:
    res = DataItem.build(regions=[variable, indexes])
    if verify_op:
        res.verify(verify_nested_ops=False)
    return res

  def verify_(self) -> None:
    pass
