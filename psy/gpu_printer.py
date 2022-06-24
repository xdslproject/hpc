from ftn.dialects import *
from psy.dialects import psy_gpu
from xdsl.dialects.builtin import StringAttr, IntegerAttr
from ftn import ftn_printer
import sys

class OpenAccPrinter(ftn_printer.FortranPrinter):
  def print_op(self, op, stream=sys.stdout):
    if isinstance(op, psy_gpu.ParallelLoop):
      self.print_indent()
      print("!$acc enter data", end="")
      self.generate_data_directive(op.copy_in_vars, " copyin")
      self.generate_data_directive(op.copy_out_vars, " copyout")   
      self.generate_data_directive(op.create_vars, " create")
      print("")
      self.generate_loop_annotation(op)
      self.print_op(op.loop.blocks[0].ops[0], stream)
      self.print_indent()
      print("!$acc exit data")
    elif isinstance(op, psy_gpu.CollapsedParallelLoop):
      self.print_op(op.loop.blocks[0].ops[0], stream)
    elif isinstance(op, psy_gpu.SequentialRoutine):
      print("")
      self.print_indent()
      print("!$acc routine seq")
      self.print_out_routine(op.routine.blocks[0].ops[0])
    else:
      ftn_printer.FortranPrinter.print_op(self, op, stream)
      
  def generate_loop_annotation(self, parallel_loop):
    self.print_indent()
    print(f"!$acc parallel loop vector_length({parallel_loop.attributes['vector_length'].data}) num_workers({parallel_loop.attributes['num_workers'].data}) gang", end="")
    collapse_loops=parallel_loop.attributes["num_inner_loops_to_collapse"].data
    if collapse_loops > 0:
      # Collapse plus one here as we need to include the outer loop that is being decorated too
      print(f" collapse({collapse_loops+1})", end="")
      
    self.generate_data_directive(parallel_loop.private_vars, " private")
      
    print("")
      
  def generate_data_directive(self, data_vars, pragma_str, ignore_scalars=False, scalars_only=False):
    if len(data_vars.blocks[0].ops) == 0: return
    print(f"{pragma_str}(", end="")
    for index, data_op in enumerate(data_vars.blocks[0].ops):
      if (index > 0): print(", ", end="")          
      self.print_op(data_op)
    print(")", end="")

def print_openacc(instructions, stream=sys.stdout):    
  openacc_printer=OpenAccPrinter()
  for op in instructions:
    openacc_printer.print_op(op, stream=stream)
    