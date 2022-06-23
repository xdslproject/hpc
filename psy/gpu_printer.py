from ftn.dialects import *
from psy.dialects import psy_gpu
from xdsl.dialects.builtin import StringAttr, IntegerAttr
from ftn import ftn_printer
import sys

class OpenAccPrinter(ftn_printer.FortranPrinter):
  def print_op(self, op, stream=sys.stdout):
    if isinstance(op, psy_gpu.ParallelLoop):
      print("!$acc data", end="")
      self.generate_data_directive(op.copy_in_vars, " copyin")
      self.generate_data_directive(op.copy_out_vars, " copyout")   
      self.generate_data_directive(op.create_vars, " create")
      print("")
      self.generate_loop_annotation(op)
      self.print_op(op.loop.blocks[0].ops[0], stream)
      print("!$acc end data")
    elif isinstance(op, psy_gpu.CollapsedParallelLoop):
      self.print_op(op.loop.blocks[0].ops[0], stream)
    else:
      ftn_printer.FortranPrinter.print_op(self, op, stream)
      
  def generate_loop_annotation(self, parallel_loop):
    print(f"!$acc parallel loop vector_length({parallel_loop.attributes['vector_length'].data}) num_workers({parallel_loop.attributes['num_workers'].data}) gang", end="")
    collapse_loops=parallel_loop.attributes["num_inner_loops_to_collapse"].data
    if collapse_loops > 0:
      print(f" collapse({collapse_loops})", end="")
      
    print("")
      
  def generate_data_directive(self, data_vars, pragma_str):
    if len(data_vars.blocks[0].ops) == 0: return
    print(f"{pragma_str}(", end="")
    needs_comma=False
    for data_op in data_vars.blocks[0].ops:
      if (needs_comma): print(", ", end="")
      needs_comma=True
      self.print_op(data_op)
    print(")", end="")

def print_openacc(instructions, stream=sys.stdout):    
  openacc_printer=OpenAccPrinter()
  for op in instructions:
    openacc_printer.print_op(op, stream=stream)
    