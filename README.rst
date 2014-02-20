Overview
========

Pysheet is your best companion for data management. It can read and write to a delimited text file (spreadsheet), consolidate columns and merge spreadsheets together. It allows you to query for information thus turning your text file into a lightweight database. It can be used both as a python library (from pysheet import Pysheet) and as a command-line tool (pysheet -h) and supports concurrent access control for reading/writing to the same file in parallel.

Quick Start
-----------

   ::

       pip install pysheet

       pysheet --write 1 A world 2 B hello 3 C ! -k 2 1 3 -v

Output:

   ::

      ID |   B   |   A   | C
      ===+=======+=======+==
      1  |       | world |
      2  | hello |       |
      3  |       |       | !


Help!
-----

| For more documentation, please visit `Pysheet on github <https://github.com/isthisthat/Pysheet/>`__.
| I hope you find pysheet useful. If you need more help, please `contact me <https://github.com/isthisthat>`__! I'd be happy to hear from you.
| Please submit feature requests and bug reports `via github <https://github.com/isthisthat/Pysheet/issues>`__.

