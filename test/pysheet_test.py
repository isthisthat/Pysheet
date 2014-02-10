#!/usr/bin/env python

import unittest, os
from pysheet import Pysheet, PysheetException
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
    self.assertEqual(sorted(p.get("1")), sorted(self.table[1]))
    self.assertEqual(p.get(3), None)
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
    p = Pysheet(iterable=self.table, trans=True)
    self.assertEqual(p.getHeaders(),['ID', '1', '2', '99', '88'])

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
    
  def test_example(self):
    # get the directories right
    test_dir = os.path.dirname(os.path.realpath(__file__))
    pysheet = os.path.join(test_dir, "..", "pysheet.py")
    cgc = os.path.join(test_dir, "cancer.tsv")
    ortho = os.path.join(test_dir, "ortho.txt")
    test = os.path.join(test_dir, "test.csv")
    test_lock = os.path.join(test_dir, "test.csv.lock")

    out = [\
'Human Gene |  ZFIN ID   |    ZFIN    |   Entrez   |   Entrez   |            \n',
'  Symbol   |            |   Symbol   | Zebrafish  | Human Gene |            \n',
'           |            |            |  Gene ID   |     ID     |            \n',
'===========+============+============+============+============+===========\n',
'ABCA12     | ZDB-GENE-0 | abca12     | 558335     | 26154      |            \n',
'           | 30131-9790 |            |            |            |            \n'\
]
    cmd = "%s -d %s -i3 -D\\t -k ALL" % (pysheet, ortho)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout[:6], out)

    out = [\
'   Symbol    |     Name     |    GeneID    |     Chr      |   Chr Band   \n',
'=============+==============+==============+==============+=============\n',
'ABL1         | v-abl        | 25           | 9            | 9q34.1       \n',
'             | Abelson      |              |              |              \n',
'             | murine       |              |              |              \n',
'             | leukemia     |              |              |              \n',
'             | viral        |              |              |              \n',
'             | oncogene     |              |              |              \n',
'             | homolog 1    |              |              |              \n'\
]
    cmd = "%s -d %s -D\\t -k 1-4" % (pysheet, cgc)
    p = Popen(cmd.split(), stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout[:9], out)

    out = [\
'CBFA2T3\n',
'CBFB\n',
'GMPS\n',
'HOXA13\n',
'MYH11\n',
'NUP98\n'\
]
    cmd = "%s -d %s -i3 -D\\t -m %s -M\\t -q" % (pysheet, ortho, cgc)
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
    cmd = "%s -d %s -i3 -D\\t -m %s -M\\t -q" % (pysheet, ortho, cgc)
    p = Popen(cmd.split() + ['ZFIN ID','Tumour Types  (Somatic Mutations)~AML'], stdout=PIPE)
    myout = p.stdout.readlines()
    self.assertEqual(myout, out)
    
    # remove old file to avoid warning
    try:
      os.remove(test)
    except OSError:
      pass
    cmd = "%s -d %s -i3 -D\\t -m %s -M\\t -C Phenotype Cancer Mut Other -k 5 2 3 1 10 -o %s" % (pysheet, ortho, cgc, test)
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

    

#if __name__ == '__main__': unittest.main()
suite = unittest.TestLoader().loadTestsFromTestCase(TestFunctions)
unittest.TextTestRunner(verbosity=3).run(suite)

