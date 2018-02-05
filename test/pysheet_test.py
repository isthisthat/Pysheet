#!/usr/bin/env python

import unittest, os
from pysheet.pysheet import Pysheet, PysheetException
from subprocess import call, check_output, Popen, PIPE, STDOUT
from time import sleep

class TestFunctions(unittest.TestCase):

  def setUp(self):
    self.table = \
        [["ID","H1","H2","H3"],\
        [1,"a","b","c"], \
        [2,"aa","bb","cc"], \
        [99, "", "", ""], \
        [88, "", 8, 8]]

  def test_load(self):
    p = Pysheet(iterable=self.table)
    self.assertEqual(sorted(p.getRow("1")), sorted(self.table[1]))
    self.assertEqual(p.getRow(3), None)
    self.assertEqual(sorted(p.getIds()), sorted([i[0] for i in self.table][1:]))
    self.assertEqual(p.grab(key=2,header="h3"), "cc")
    self.assertEqual(p.grab(key=2,header="h4"), None)
    self.assertRaises(PysheetException, p.grab, level="c")
    self.assertEqual(p.grab(level="c",header="h3"), [1])
    self.assertEqual(p.grab(header="ALL",level="c"), None)
    self.assertEqual(sorted(p.grab(header="h2",level="All")), [1,2,88])
    self.assertEqual(p.grab(header="H2",level="bb"), [2])
    self.assertEqual(p.grab(header="h2",level="foo"), [])
    self.assertFalse(p.isEmpty())
    self.assertEqual(p[1],[1, 'a', 'b', 'c'])
    self.assertEqual(p[3],None)
    p.setCell("1",'ID',3)
    self.assertEqual(p[3],[3, 'a', 'b', 'c'])
    p = Pysheet(iterable=self.table, trans=True)
    self.assertEqual(p.getHeaders(),['ID', '1', '2', '99', '88'])
    p = Pysheet(iterable=self.table, trans=True, cstack=True) + \
            Pysheet(iterable=self.table, cstack=True)
    self.assertEqual(len(p),11)
    self.assertEqual(p.height(),5)
    p = Pysheet(iterable=self.table, trans=True, rstack=True) + \
            Pysheet(iterable=self.table, rstack=True)
    p.contract()
    self.assertEqual(len(p),5)
    self.assertEqual(p.height(),9)

  def test_columns(self):
    p = Pysheet()
    p.load(self.table)
    self.assertEqual(sorted(p.produceColumn([0,3,1])[1]), ['1_c_a', '2_cc_aa', '88_8', '99'])
    self.assertEqual(sorted(p.produceColumn(["h2",3])[1]), ['', '8_8', 'b_c', 'bb_cc'])
    self.assertEqual(len(p.getColumns()),3)
    self.assertEqual(len(p.getColumns(blanks=True)),5)
    self.assertEqual(len(p.getColumns(3)),4)
    self.assertTrue(p.containsColumn("id"))
    self.assertTrue(p.containsColumn("h3"))
    self.assertEqual(p.getHeaders(),['ID', 'H1', 'H2', 'H3'])
    p.removeMissing(rows=False)
    self.assertEqual(p.getHeaders(),['ID'])

  def test_operations(self):
    p = Pysheet(iterable=self.table)
    p.consolidate("h")
    self.assertEqual(sorted(p.produceColumn("h")[1]), ['', '8', 'a;b;c', 'aa;bb;cc'])
    p.zeroFill()
    self.assertEqual(p[99], [99, 0, 0, 0, 0])
    p.consolidate("h",cleanUp=True)
    self.assertEqual(len(p.getColumns()[0]), 2)
    p.consolidate("h")
    self.assertEqual(sorted(p.produceColumn("h")[1]), ['0', '8;0', 'a;b;c', 'aa;bb;cc'])
    p.insertColumn("foo",1)
    self.assertEqual(p.getHeaders(),['ID', '__foo', '__h'])
    p.rename("hh",header="foo")
    p.addCell(99,"hh",-1)
    p.consolidate("h",mode='add')
    self.assertEqual(p.grab(99,"h"),-1)
    p.consolidate("h",mode='overwrite')
    p.consolidate("h",mode='append')
    self.assertEqual(p.grab(99,"h"),'-1;-1')
    p.removeCell(88,header="h")
    self.assertEqual(p[88],[88, '', ''])
    p = Pysheet(iterable=self.table)
    p.consolidate("h",mode='mean')
    self.assertEqual(p.grab(88,"h"),8)
    p.addCell('%','%','1%')
    p.addCell('%','%%','0.1%')
    p.consolidate("%",mode='mean',cleanUp=True)
    self.assertEqual(p.grab('%','%'),'0.55%')
    p.addCell('%','H1','foo')
    p._COLLAPSE='|'
    p.consolidate(["bar","%","h"],mode='mean')
    self.assertEqual(p.grab('%','bar'),'foo|0.55%')
    
  def test_example(self):
    # get the directories right
    test_dir = os.path.dirname(os.path.realpath(__file__))
    pysheet = os.path.join(test_dir, "..", "pysheet", "pysheet.py")
    cgc = os.path.join(test_dir, "cancer.tsv")
    ortho = os.path.join(test_dir, "ortho.txt")
    test = os.path.join(test_dir, "test.csv")
    test_lock = os.path.join(test_dir, "test.csv.lock")

    out = [
' Human Gene   |   ZFIN ID    | ZFIN Symbol  |    Entrez    | Entrez Human | V005\n',
'   Symbol     |              |              |  Zebrafish   |   Gene ID    |     \n',
'              |              |              |   Gene ID    |              |     \n',
'==============+==============+==============+==============+==============+=====\n',
'ABCA12        | ZDB-GENE-030 | abca12       | 558335       | 26154        |     \n',
'              | 131-9790     |              |              |              |     \n'
]
    cmd = "%s -d %s -i3 -D\\t -k ALL" % (pysheet, ortho)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout[:6], out)

    out = [
'  Symbol    |               Name               | GeneID | Chr |     Chr Band    \n',
'============+==================================+========+=====+=================\n',
'ABL1        | v-abl Abelson murine leukemia    | 25     | 9   | 9q34.1          \n',
'            | viral oncogene homolog 1         |        |     |                 \n'
]
    cmd = "%s -d %s -k 1-4" % (pysheet, cgc)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout[:4], out)

    out = [\
'CBFA2T3\n',
'CBFB\n',
'GMPS\n',
'HOXA13\n',
'MYH11\n',
'NUP98\n'\
]
    cmd = "%s -d %s %s -i 3 0 -D \\t -q" % (pysheet, ortho, cgc)
    p = Popen(cmd.split() + ['ZFIN ID','Tumour Types  (Somatic Mutations)=AML'], stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout, out)

    out = [\
'CBFA2T3\n',
'CBFB\n',
'ERG\n',
'FUS\n',
'GAS7\n',
'GATA2\n',
'GMPS\n',
'HOXA13\n',
'JAK2\n',
'KIT\n',
'KRAS\n',
'MYH11\n',
'NCOA2\n',
'NPM1\n',
'NUP98\n',
'PDGFRB\n',
'PTPN11\n',
'RUNX1\n'\
]
    cmd = "%s -d %s %s -i 3 0 -q" % (pysheet, ortho, cgc)
    p = Popen(cmd.split() + ['ZFIN ID','Tumour Types  (Somatic Mutations)~AML'], stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout, out)
    
    # remove old file to avoid warning
    try:
      os.remove(test)
    except OSError:
      pass
    cmd = "%s -d %s %s -i 3 0 -D \\t \\t -C Phenotype Cancer Mut Other -k 5 2 3 1 10 -o %s" % (pysheet, ortho, cgc, test)
    p = Popen(cmd.split(), stdout=PIPE)
    p.communicate()

    out = [\
'0 Human Gene Symbol\n',
'1 Entrez Human Gene ID\n',
'2 ZFIN Symbol\n',
'3 Entrez Zebrafish Gene ID\n',
'4 Phenotype\n',
'5 Chr Band\n'\
]
    cmd = "%s -d %s -H" % (pysheet, test)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout, out)

    cmd = "%s -d %s -q 1 Phenotype" % (pysheet, test)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(len(myout), 116)

    counter = 0
    while counter < 10:
      cmd = "%s -d %s -o %s -w %s Match yes -L" % (pysheet, test, test, myout[counter])
      Popen(cmd.split())
      counter += 1

    sleep(30) # wait for processes to finish...
    
    cmd = "%s -d %s -q Match" % (pysheet, test)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(len(myout), 10)

    # check that we have cleaned up the lock..
    self.assertRaises(OSError, os.remove, test_lock)

    # clean up
    os.unlink(test)
    

#if __name__ == '__main__': unittest.main()
suite = unittest.TestLoader().loadTestsFromTestCase(TestFunctions)
unittest.TextTestRunner(verbosity=3).run(suite)

