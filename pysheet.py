#!/usr/bin/env python

"""
A library to read, write and manipulate spreadsheets

Copyright (c) 2013, Stathis Kanterakis
Last Update: May 2013
"""

__version__ = "2.0"
__author__  = "Stathis Kanterakis"
__license__ = "LGPL"

import csv, sys, os, logging, re, traceback
import argparse
from numpy import reshape, array, ndarray, floating, zeros
from types import ListType, IntType
from errno import EPIPE
from itertools import izip
import cPickle
from texttable import Texttable
from time import sleep
from datetime import datetime
from random import random

# unbuffered stdout
unbuffered = os.fdopen(sys.stdout.fileno(), 'w', 0)
sys.stdout = unbuffered


####################################
########### CLI WRAPPER ############
####################################

def main():
  """parse command line input and call appropriate functions"""

  parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
  description="A library to read and write comma-separated (.csv) spreadsheets", epilog="""
Examples:
  %(prog)s -o mystudy.csv -w ID001 Age 38 ID002 Gender M
    creates a blank sheet and adds two cell entries to it. Saves it as ./mystudy.csv
  
  %(prog)s -d mystudy.csv -c Items store -C Price price -C Availability avail -q Availability Price>0.5
    consolidates Items across columns whose headers contain the keyword 'store'. Similarly for Price and Availability
    then prints all IDs of Items with Price>0.5 and non-blank Availability
  
  %(prog)s -d /path/table.txt -D'\\t' -o ./test/mystudy.csv -k 5 1-3 -v
    reads a tab-delimited sheet and saves the columns 0 (assumed to be the IDs) 5,1,2,3 in csv format as ./test/mystudy.csv

  %(prog)s -d mystudy.csv -k
    prints the entire data sheet to screen

  %(prog)s -d mystudy.csv -R 01001 Status -o mystudy.csv
    removes the cell for ID '01001' under the 'Status' column

  %(prog)s -d results.csv -w iteration_$i Result $val -o results.csv -L
    adds a cell to the results sheet (locking the file before read/write access)
""")
  groupIO = parser.add_argument_group('Input/Output')
  groupIO.add_argument('--dataSheet', '-d', type=valid_file, metavar="CSV", help='A delimited spreadsheet with unique IDs in the first column (or use -i) and headers in the first row')
  groupIO.add_argument('--dataDelim', '-D', metavar='DELIMITER', help='The delimiter of the input dataSheet. Default is comma (,)', default=',')
  groupIO.add_argument('--dataIdCol', '-i', type=int, metavar='N', help='Column number (starting from 0) which contains the unique IDs. Default is 0', default=0)
  groupIO.add_argument('--lockFile', '-L', nargs='?', type=writeable_file, help="Prevents parallel jobs from overwriting the dataSheet. Use in cluster environments or asynchronous loops. \
      Optionally, specify a filename (default is <dataSheet>.lock", const=True)
  groupIO.add_argument('--outSheet', '-o', type=writeable_file, metavar="CSV", help='Output filename (may include path). Delimiter is always comma (,)')
  groupIO.add_argument('--outDelim', metavar='DELIMITER', help='The delimiter of the output Sheet. Default is comma (,)', default=',')
  
  groupRW = parser.add_argument_group('Read/Write')
  groupRW_me = groupRW.add_mutually_exclusive_group()
  groupRW_me.add_argument('--write', '-w', nargs='*', metavar="ID HEADER VALUE", help="Write new cells")
  groupRW_me.add_argument('--read', '-r', nargs='*', metavar="ID HEADER", help="Print value of cells to screen")
  groupRW_me.add_argument('--remove', '-R', nargs='*', metavar="ID HEADER", help="Remove cells")

  groupM = parser.add_argument_group('Merge')
  groupM.add_argument('--mergeSheet', '-m', action='append', type=valid_file, metavar="CSV", help='Merge another spreadsheet to this file (can be used multiple times)')
  groupM.add_argument('--mergeDelim', '-M', action='append', metavar='DELIMITER', help='The delimiter of mergeSheet. Default is comma (,)')
  groupM.add_argument('--mergeIdCol', '-I', action='append', type=int, metavar='N', help='Column number (starting from 0) which contains the unique IDs of the mergeSheet. Default is 0')

  groupC = parser.add_argument_group('Consolidate')
  groupC.add_argument('--consolidate', '-c', nargs='*', action='append', metavar="HEADER KEYWORD1 KEYWORD2 etc", help='Consolidate columns according to keywords (can be used multiple times)')
  groupC.add_argument('--clean', '-C', nargs='*', action='append', metavar="HEADER KEYWORD1 KEYWORD2 etc", help="Consolidate and remove consolitated columns (can be used multiple times)")

  groupQ = parser.add_argument_group('Query')
  groupQ.add_argument('--columns', '-k', nargs='*', help="Extracts specific columns from the dataSheet. e.g. '1-3 Age'. Default is print all columns")
  groupQ.add_argument('--query', '-q', nargs='*', help="Extracts IDs that meet a query. e.g. 'Age>25 Group=Normal Validated' \
      (NOTE: will not return IDs that have a non-blank entry in the special 'Exclude' column)")
  groupQ.add_argument('--printHeaders', '-H', action='store_true', help="Prints out all column headers and their index")
  
  parser.add_argument('--wait', type=int, help=argparse.SUPPRESS) # this is for testing purposes. will sleep before writing to test locking stuff..
  parser.add_argument('catchall', nargs=argparse.REMAINDER, help=argparse.SUPPRESS)

  parser.add_argument('--version', '-V', action='version', version="%(prog)s v" + __version__)
  parser.add_argument('--verbose', '-v', action='count', help='verbosity level')

  args = parser.parse_args()

  # setup the logger. we'll use the process ID for the name
  myPid = str(os.getpid())
  logger = logging.getLogger(myPid)
  if args.verbose == 1:
    logger.setLevel(logging.INFO)
  elif args.verbose > 1:
    logger.setLevel(logging.DEBUG)
  ch = logging.StreamHandler()
  formatter = logging.Formatter('%(name)s %(levelname)s %(message)s')
  ch.setFormatter(formatter)
  logger.addHandler(ch)

  # no arguments given
  if not len(sys.argv) > 1:
    parser.print_usage()
    
  # are there leftover parameters?
  if args.catchall:
    logger.warn("!!! Unused parameters: %s" % flatten(args.catchall))

  # save once at the end if needed
  save = False
  if args.outSheet: save = True

  # some IO info
  if args.dataSheet:
    logger.info("+++ Input sheet: %s" % args.dataSheet)
  if args.outSheet:
    logger.info("+++ Output sheet: %s" % args.outSheet)
  if args.mergeSheet:
    for m in args.mergeSheet:
      logger.info("+++ Merging with: %s" % m)

  # LOCKING
  lock = False
  if args.lockFile:
    
    # set lockFile and check that we are not overwriting critical stuff..
    if args.lockFile == True:
      args.lockFile = os.path.realpath(args.dataSheet + ".lock")
    if os.path.isfile(args.lockFile):
      if args.outSheet and os.path.samefile(args.lockFile, args.outSheet):
        logger.critical("!!! lockFile can't be the same as your outSheet!")
        return 1
      if args.dataSheet and os.path.samefile(args.lockFile, args.dataSheet):
        logger.critical("!!! lockFile can't be the same as your dataSheet!")
        return 1
    args.lockFile = writeable_file(args.lockFile) # check it is writeable

    # check that we have something to lock
    if args.dataSheet:
      lock = True
      logger.info("+++ Lock file: %s" % args.lockFile)
    else:
      logger.warn("!!! You didn't specify a dataSheet to lock...")

    # now perform locking
    if lock:
      counter = 1.0
      timeoutSec = 180.0
      staleSec = timeoutSec + 2
      # see if there are leftover locks
      if os.path.isfile(args.lockFile):
        lockTime = datetime.fromtimestamp(os.path.getmtime(args.lockFile))
        curTime = datetime.now()
        if (curTime - lockTime).seconds > staleSec:
          logger.warn("!!! Removing stale lock: %s" % args.lockFile)
          os.remove(args.lockFile)
      # wait for all other instances to finish writing
      while counter < timeoutSec:
        if os.path.isfile(args.lockFile):
          counter += random()
          sleep(counter)
        else: # attempt a lock
          open(args.lockFile,'w').write(myPid)
          sleep(random())
          try:
            thisPid = open(args.lockFile,'rU').read()
            if thisPid == myPid: # we've successfully acquired a lock!
              break
          except IOError: # somebody removed the lock externally...
            continue
      if counter > 1:
        logger.debug(">>> Process slept for %d sec waiting for lock..." % counter)
      # we've exceeded the timeoutSec and a process is still writing. now what?
      if counter >= timeoutSec:
        args.outSheet += "." + myPid
        logger.critical("""
!!! LOCK TIMEOUT LIMIT EXCEEDED (%(lt)d seconds)
!!! Changes (if any) to: %(ds)s
!!! will be saved to: %(os)s
!!! To merge changes back, use:
!!! %(prog)s -d %(ds)s -m %(os)s -o %(ds)s""" % {"lt":timeoutSec, "ds":args.dataSheet, "os":args.outSheet, "prog":sys.argv[0]})
        lock = False
      else:
        logger.debug(">>> Grabbing lock...")

  # check if output exists
  if args.outSheet and os.path.isfile(args.outSheet) and not os.path.samefile(args.outSheet, args.dataSheet):
    logger.warn("!!! outSheet already exists and will be overwritten: %s" % args.outSheet)
  if args.outSheet and not args.dataSheet:
    logger.warn(">>> Creating a blank sheet: %s" % args.outSheet)

  # now read the file
  mycsv = Pysheet(args.dataSheet, delimiter=args.dataDelim, idColumn=args.dataIdCol)
  
  # merge
  if args.mergeSheet:
    # strangely, defaults don't work as expected when action='append' so we have to do it manually here...
    if not args.mergeDelim:
      args.mergeDelim = [',']
    if not args.mergeIdCol:
      args.mergeIdCol = [0]
    # now check merge parameters..
    if len(args.mergeDelim) != len(args.mergeSheet):
      if len(args.mergeDelim) == 1:
        args.mergeDelim = args.mergeDelim * len(args.mergeSheet) # make this the delimiter for all mergeSheets
      else:
        logger.critical("!!! You must provide either one mergeDelim or as many as your mergeSheets (%d). Currently: %d" % (len(args.mergeSheet), len(args.mergeDelim)))
        return 1
    if len(args.mergeIdCol) != len(args.mergeSheet):
      if len(args.mergeIdCol) == 1:
        args.mergeIdCol = args.mergeIdCol * len(args.mergeSheet) # make this the idColumn for all mergeSheets
      else:
        logger.critical("!!! You must provide either one mergeIdCol or as many as your mergeSheets (%d). Currently: %d" % (len(args.mergeSheet), len(args.mergeIdCol)))
        return 1
    # now merge
    for m in range(len(args.mergeSheet)):
      myothercsv = Pysheet(args.mergeSheet[m], delimiter=args.mergeDelim[m], idColumn=args.mergeIdCol[m])
      mycsv += myothercsv # __add__
      mycsv.contract() # merge same columns

    
  # remove cells
  if args.remove:
    try:
      cells = reshape(args.remove,(len(args.remove)/2,2))
      printed = 0
      for i in range(len(cells)):
        if cells[i][1].lower() == "none":
          ret = mycsv.removeCell(key=cells[i][0])
          #if ret: save = True
          if isList(ret):
            ret = '|'.join(ret)
        else:
          ret = mycsv.removeCell(key=cells[i][0],header=cells[i][1])
          #if ret: save = True
        if ret:
          sys.stdout.write(str(ret))
          printed += 1
      logger.info("=== Deleted %d cell%s.." % (printed, '' if printed==1 else 's'))
    except ValueError:
      logger.critical("!!! Cell entries must be of the form 'ID header': " % flatten(args.remove))
      return 1
    except IOError as e:
      if e.errno == EPIPE:
        pass # pipe to the other program was closed

  
  # add cells
  if args.write:
    try:
      cells = reshape(args.write,(len(args.write)/3,3))
      #save = True
      for i in range(len(cells)):
        if cells[i][1].lower() == "none":
          mycsv.addCell(cells[i][0])
        else:
          mycsv.addCell(cells[i][0],cells[i][1],cells[i][2])
      logger.info("=== Added %d cell%s.." % (len(cells), '' if len(cells)==1 else 's'))
    except ValueError:
      logger.critical("!!! Cell entries must be of the form 'ID header value': " % flatten(args.write))
      return 1
    except IOError as e:
      if e.errno == EPIPE:
        pass # pipe to the other program was closed

    
  # consolidate
  if args.clean:
    for c in args.clean:
      logger.info(">>> Consolidating (clean): %s" % flatten(c))
    mycsv.consolidate(args.clean, cleanUp=True)
  if args.consolidate:
    for c in args.consolidate:
      logger.info(">>> Consolidating: %s" % flatten(c))
    mycsv.consolidate(args.consolidate)
  

  # query
  # by columns
  if args.columns != None: # we still need to handle []
    try:
      if not args.columns == []: # if we got some column specification, extract those columns (else print all columns)
        cols = Pysheet()
        cols.load(mycsv.getColumns(args.columns, blanks=True, exclude=False))
        mycsv = cols # make this the current spreadsheet
      if not args.outSheet and not args.query and not args.read and not args.printHeaders:
        sys.stdout.write(str(mycsv))
    except IOError as e:
      if e.errno == EPIPE:
        pass # pipe to the other program was closed
  # print headers
  if args.printHeaders:
    for hi in range(len(mycsv.getHeaders())):
      sys.stdout.write("%d %s\n" % (hi, mycsv.getHeaders()[hi]))
  # by rows
  if args.query:
    try:
      retList = transpose(mycsv.getColumns(args.query))[0][1:] # skip the headers
      retList.sort()
      logger.info("=== Query '%s' returned %d ID%s.." % (flatten(args.query), len(retList), '' if len(retList)==1 else 's'))
      for item in retList:
        sys.stdout.write(item+"\n")
    except IOError as e:
      if e.errno == EPIPE:
        pass # pipe to the other program was closed
  # by cells
  if args.read:
    try:
      cells = reshape(args.read,(len(args.read)/2,2))
      printed = 0
      for i in range(len(cells)):
        if cells[i][1].lower() == "none":
          ret = mycsv.grab(key=cells[i][0])
          if isList(ret):
            ret = '|'.join(ret)
        else:
          ret = mycsv.grab(key=cells[i][0],header=cells[i][1])
        if ret:
          sys.stdout.write(str(ret))
          printed += 1
      logger.info("=== Printed %d cell%s.." % (printed, '' if printed==1 else 's'))
    except ValueError:
      logger.critical("!!! Cell entries must be of the form 'ID header': " % flatten(args.read))
      return 1
    except IOError as e:
      if e.errno == EPIPE:
        pass # pipe to the other program was closed

  # now save
  if save:
    if args.wait:
      logger.debug(">>> Sleeping %d sec" % args.wait)
      sleep(args.wait)
    mycsv.save(args.outSheet, args.outDelim)
    logger.info("=== Sheet saved as: %s" % args.outSheet)
  
  if lock:
    logger.debug(">>> Releasing lock...")
    os.remove(args.lockFile)
      
  return 0


####################################
######## CLASS STARTS HERE #########
####################################

class Pysheet:
  """Pysheet - A library to read, write and manipulate spreadsheets"""
  # globals
  HEADERS_ID      = "__headers__" # denotes the row which contains the header labels
  EXCLUDE_HEADER  = "__exclude__" # special column header used to flag a row for exclusion
  MIN_LINE_LEN    = 2 # minimum length of input line array to treat as valid
  APPEND_CHAR     = ';' # character used to merge strings in consolidated columns
  BLANK_VALUE     = '' # default value used for empty cells
  FLAG_VALUE      = '__flag__' # flags a row for filtering
  MAX_PRINT_WIDTH = 200 # the max width of a printout on the command line
  
  # parameters
  filename  = None   # the input/output path and name
  delimiter = None   # the sheet delimiter
  rows      = None   # the dictionary that maps an ID to its row
  idColumn  = 0      # the column index that contains the IDs
  
  def __init__(self, filename=None, delimiter=',', iterable=None, idColumn=None):
    """initializes the object and reads in a sheet from a file or an iterable.
    Optionally specify the column number that contains the unique IDs (starting from 0)"""
    self.filename = filename
    if idColumn != None:
      self.idColumn = max(int(idColumn),0) if str(idColumn).isdigit() else 0
    if delimiter == r'\t':
      self.delimiter = "\t"
    elif delimiter == r'\s':
      self.delimiter = "\s"
    else:
      self.delimiter = delimiter
    self.rows = {}
    if filename and os.path.exists(filename):
      self.loadFile(self.filename, self.idColumn)
    elif iterable != None:
      self.load(iterable, self.idColumn)
    else:
      self[self.HEADERS_ID] = ["ID"]
    
  def loadFile(self, filename, idColumn=None):
    """loads the sheet into a dictionary where the IDs in the first column are mapped to their rows.
    Optionally specify the column number that contains the unique IDs (starting from 0)"""
    try:
      reader = csv.reader(open(filename, "rUb"), delimiter=self.delimiter)
    except csv.Error as e:
      raise PysheetException(e, filename)
    self.filename = filename
    if idColumn != None:
      self.idColumn = max(int(idColumn),0) if str(idColumn).isdigit() else 0
    self.load(reader, self.idColumn)
  
  def load(self, iterable, idColumn=None):
    """creates a Pysheet object from an iterable.
    Optionally specify the column number that contains the unique IDs (starting from 0)"""
    try:
      iterator = iter(iterable)
    except TypeError:
      raise PysheetException("%s is not iterable!" % iterable)

    if self.rows != None: # clear the object
      self.__init__(filename=None,delimiter=self.delimiter)

    row = 1
    head_len = -1
    if idColumn != None:
      self.idColumn = max(int(idColumn),0) if str(idColumn).isdigit() else 0
    try:
      while True:
        line = iterator.next()
        line_len = len(line)
        if line_len < self.MIN_LINE_LEN: # skip blank and short lines..
          continue
        if row == 1:
          self.rows[self.HEADERS_ID] = [str(value).strip() for value in line] # sanitize(value) for value in line]
          head_len = line_len
          #print "HEAD_LEN", head_len, self.idColumn, line
          if self.idColumn > head_len-1:
            self.idColumn = 0
        else:
          thisline = []
          for value in line:
            thisline.append(value)
          while line_len < head_len:
            thisline.append(self.BLANK_VALUE)
            line_len += 1
          self.rows[clean(sanitize(line[self.idColumn]))] = thisline
        row += 1
    except StopIteration:
      pass
    except Exception as e:
      printStackTrace()
      raise PysheetException(e.message)
  
  def __iter__(self):
    """returns an iterator over the ID:row items in the csv"""
    return self.rows.iteritems()
  
  def __len__(self):
    """returns the length of the header row in the dictionary"""
    return len(self.getHeaders())
  
  def pop(self, x, default=None):
    """pops an item out of the dictionary"""
    return self.rows.pop(clean(x),default)
  
  def keys(self, headers=True, exclude=True, lockedRows=True):
    """returns a list of the keys in the dictionary.
    headers=True adds columnheaders to the list returned
    exclude=True skips rows with a non-blank __exclude__ column
    lockedRows=True also returns rows whose ID starts with '__'"""
    return [self[x][self.idColumn] for x in self.rows.keys() if \
        (headers or x != self.HEADERS_ID) and (lockedRows or not x.startswith('__')) and \
        (not exclude or not self.excluded(x))]
  
  def getIds(self):
    """returns all keys of the dictionary, except the header key and any IDs starting with '__'"""
    return self.keys(headers=False, exclude=True, lockedRows=False)

  def __getitem__(self, key, default=None):
    """gets the row of an ID from the dictionary"""
    return self.rows.get(clean(sanitize(key)), default)
  def get(self, key, default=None):
    """gets the row of an ID from the dictionary"""
    return self[key]
    
  def __setitem__(self, key, row):
    """changes the row of an ID in the dictionary"""
    self.rows[clean(key)] = row
  def set(self, key, row):
    """changes the row of an ID in the dictionary"""
    self[key] = row
  
  def __delitem__(self, key):
    """deletes a row from the dictionary"""
    del self.rows[clean(key)]
  
  def headerIndex(self, header):
    """returns the index of a header in the dictionary"""
    # if a column number, return the index
    if header != None:
      header = clean(str(header)).replace('__','')
      headers_lower = [l.lower().replace('__','') for l in self.getHeaders()]
      if header in headers_lower:
        return headers_lower.index(header)
      elif str(header).isdigit():
        header = int(header)
        if header >= 0 and header < len(self):
          return header
    return -1 # all other cases return -1 to indicate error
  
  def getHeaders(self): #, lockedHeaders=True):
    """returns the headers of the columns in the dictionary.
    If lockedHeaders=False, skips the headers starting with '__'"""
    #if not lockedHeaders:
    #  return [l for l in self[self.HEADERS_ID] if not l.startswith('__')]
    return self[self.HEADERS_ID]
  
  def containsColumn(self, header):
    """True if header exists in the csv header list"""
    return self.headerIndex(header) > -1
  
  def addCell(self, key, header=None, value=None, mode='overwrite'):
    """adds a single cell in the dictionary. mode can be 'smart_append'
    (adds a value if not already present), 'append', 'overwrite' and
    'add' (performs plus operation if values are numeric)"""

    cleankey = clean(sanitize(key))
    if self[cleankey] == None:
      self[cleankey] = [key.strip()] + [self.BLANK_VALUE] * (len(self)-1)
    if header != None:
      header = header.strip()
      if self.headerIndex(header) == -1: # add the header
        if header.startswith('__'):
          self.insertColumn(header)
        else:
          self.getHeaders().append(header)
          self.expand()
      if value == None:
        value = self.BLANK_VALUE
      # add the value using the correct mode
      self[cleankey][self.headerIndex(header)] = self.mergedValue(self[cleankey][self.headerIndex(header)], value, mode=mode)
  
  def removeCell(self, key, header=None):
    """deletes a cell or a row from the dictionary"""
    ret = None
    cleankey = clean(sanitize(key))
    if self[cleankey] != None:
      if header != None:
        header = header.strip()
        if self.headerIndex(header) != -1: # header exists
          ret = self[cleankey][self.headerIndex(header)]
          self[cleankey][self.headerIndex(header)] = self.BLANK_VALUE
      else:
        ret = self[cleankey]
        del self[cleankey]
    return ret

  def insertColumn(self, header, index=None):
    """inserts a blank column in the dictionary at index (number)"""
    header = str(header)
    if not header.lower().replace('__','') in [l.lower().replace('__','') for l in self.getHeaders()]:
      if index == None:
        index = 1
        while index < len(self) and self.getHeaders()[index].startswith('__'):
          index+=1
      else:
        assert type(index) is IntType, "index is not an integer: %r" % index
        assert index >= 0 and index <= len(self), "index is not in a valid range [%d-%d]: %d" % (0, len(self), index)

      for k in self.rows.keys():
        if k == self.HEADERS_ID:
          if not header.strip().startswith('__'):
            header = '__'+header.strip()
          self[k].insert(index,header)
        else:
          self[k].insert(index,self.BLANK_VALUE)
      # did we insert before the idColumn?
      if index <= self.idColumn:
        self.idColumn += 1
  
  def grab(self, key=None, header=None, level=None):
    """grabs a cell (key + header), a whole row (just key), or all keys that correspond to 'level' (header + level) from the dictionary. level='ALL' is valid"""
    if key != None:
      cleankey = clean(key)
      if self[cleankey] == None:
        return None
      if header != None:
        header = header.strip()
        if self.headerIndex(header) == -1:
          return None
        return self[cleankey][self.headerIndex(header)] # return the item
      return self[cleankey] # return the whole row
    elif level != None and header != None:
      if self.headerIndex(header) == -1:
        return None
      level = tryNumber(str(level).lower())
      thiscol = self.produceColumn(header)
      ret = []
      for i in range(len(thiscol[0])):
        if (level == 'all' and not self.isBlank(str(thiscol[1][i])) and not str(thiscol[0][i]).startswith('__')) \
        or tryNumber(str(thiscol[1][i]).lower()) == level:
          ret.append(thiscol[0][i])
      return ret
    else:
      raise PysheetException("grab either requires a key, or a header AND a level!")
  
  def __add__(self, other, mergeHeaders=False):
    """merges two Pysheets together"""
    if not isinstance(other, self.__class__):
      try:
        other = Pysheet(iterable=other) # if we have an iterable, make a default Pysheet on the fly
      except Exception:
        raise PysheetException("I don't know how to add this. Please use a Pysheet")
        printStackTrace()
    oldlen = len(self)
    # merge the existing IDs
    for i in self.rows.keys():
      item = other.pop(i,None)
      if item != None and isList(item) and len(item)>1:
        self[i] += item
    # loop through the rest of the IDs, null padding to the left
    for i in other.rows.keys():
      self[i] = [self.BLANK_VALUE] * oldlen + other[i]
    # make it sqare again
    self.expand()
    # make ID columns header names the same so they will be merged in the next step!
    self.getHeaders()[oldlen + other.idColumn] = self.getHeaders()[self.idColumn]
    # merge common headers?
    if mergeHeaders:
      self.contract()
    return self
  
  def excluded(self, key):
    """returns True if an item's exclusion header is non-blank"""
    if key == self.HEADERS_ID:
      return False
    return not self.isBlank(self.grab(key=key, header=self.EXCLUDE_HEADER))

  def parseColumns(self, cols):
    """parses the input column specification. For example expands ["5","1-3","Age>13"] to [[5,1,2,3,9],['','','','','>'],['','','','',13]]
    Returns list of corresponding [[header index, ...], [operator, ...], [argument, ...]]"""
    ret = [[],[],[]]
    if cols:
      if not isList(cols):
        cols=[cols]
      if 'ALL' in cols: # use all columns
        rng = range(self.idColumn) + range(self.idColumn+1, len(self))
        ret = [rng, ['']*len(rng), ['']*len(rng)]
        cols.remove("ALL")
      for i in range(len(cols)):
        added = False # flag to track if we were able to parse this column or not
        c = cols[i]
        hi = self.headerIndex(c)
        if hi >= 0:
          ret[0].append(hi) # add column as is
          ret[1].append('')
          ret[2].append('')
          added = True
        else:
          # try ranges
          try:
            frm,to = c.split("-",1)
            if frm.isdigit():
              frm = int(frm)
              if to.isdigit(): # it is of the form e.g. '5-10'
                to = int(to)
                if to >= frm and frm < len(self) and to < len(self):
                  rng = range(frm, to+1)
                  ret[0].extend(rng)
                  ret[1].extend(['']*len(rng))
                  ret[2].extend(['']*len(rng))
                  added = True
              elif not to: # it is of the form e.g. '5-' which means from column 5 till the last column
                rng = range(frm, len(self))
                ret[0].extend(rng)
                ret[1].extend(['']*len(rng))
                ret[2].extend(['']*len(rng))
                added = True
          except (TypeError, ValueError):
            pass
          if not added:
            # try operators
            arg = ''
            op  = ''
            try:
              if '<' in c:
                op+='<'
              if '>' in c:
                op+='>'
              if '=' in c:
                op+='='
              if '!' in c:
                op+='!'
              if '~' in c:
                op+='~'
            except TypeError:
              pass
            if len(op) == 1: # if there is an operator, capture the column (eg. age) and the argument (eg. 10)
              col,arg = c.split(op,1)
              if arg:
                hi = self.headerIndex(col)
                if hi >= 0: #and ind != self.idColumn: # append [column index, operator (if any), argument (if any)]
                  ret[0].append(hi)
                  ret[1].append(op)
                  ret[2].append(tryNumber(arg))
                  added = True
        if not added:
          raise PysheetException("Column '%s' cannot be parsed!" % c)
    return ret

  def produceColumn(self, col=0, blanks=True, exclude=True):
    """extracts a column and corresponding IDs from the dictionary (no column headers)
    returns a 2D list where [0] is a list of IDs and [1] is the requested column
    Input a list of columns to make a hybrid column eg. col=[3,6,5]
    blanks=False will remove rows that contain *any* blank column entries whatsoever
    exclude=True will skip rows that have a value in the __exclude__ column
    Default is return a list of a list of IDs"""
    # first expand the column specification
    cols = self.parseColumns(col)

    # check if we have columns to join
    if not cols[0]:
      return [self.keys(headers=False)] # return all IDs
    
    # case we have some columns to join
    ret = []
    for i in self.rows.keys():
      hybrid = []
      if i == self.HEADERS_ID or (exclude and self.excluded(i)):
        continue
      for j in range(len(cols[0])): # add in the columns that we want
        if not cols[1][j] or not cols[2][j] or \
        (cols[1][j] == '<' and (tryNumber(self[i][cols[0][j]]) < cols[2][j])) or \
        (cols[1][j] == '>' and (tryNumber(self[i][cols[0][j]]) > cols[2][j])) or \
        (cols[1][j] == '=' and (tryNumber(self[i][cols[0][j]]) == cols[2][j])) or \
        (cols[1][j] == '~' and (str(cols[2][j]) in str(self[i][cols[0][j]]))) or \
        (cols[1][j] == '!' and (tryNumber(self[i][cols[0][j]]) != cols[2][j])):
          hybrid.append(self[i][cols[0][j]])
        else:
          hybrid.append(self.BLANK_VALUE)
      if not blanks and self.BLANK_VALUE in hybrid:
        continue
      ret.append([self[i][self.idColumn], "_".join([str(x) for x in hybrid if not self.isBlank(x)])]) # append the ID and a join of the requested columns
    return transpose(ret) # transpose so that [0] are IDs and [1] is group assignment
  
  def getColumns(self, cols=None, blanks=False, exclude=True):
    """extracts columns and corresponding IDs from the dictionary (with column headers)
    returns requested columns, row-by-row. Supports operators like col='age>20'. To get multiple columns e.g. set col=[3,6,5]
    Valid column operators: > (greater than), < (less than), = (equals), ! (does not equal), ~ (contains),
    =UNIQUE (will only keep one of each duplicate in the column)
    blanks=False will remove rows that contain *any* blank column entries whatsoever
    exclude=True will skip rows that have a value in the __exclude__ column
    Default is return all columns"""
    # first expand the column specification
    extrct = self.parseColumns(cols)

    # check if we have columns to return
    if not extrct[0]: # return all columns
      all_header_ind = range(self.idColumn) + range(self.idColumn+1, len(self))
      extrct = [all_header_ind, [self.BLANK_VALUE]*len(all_header_ind), [self.BLANK_VALUE]*len(all_header_ind)]

    # case we have some columns to return
    ret = []
    for i in self.rows.keys():
      if exclude and self.excluded(i):
        continue
      add = [] # initialize the row to be appended
      for j in range(len(extrct[0])): # add in the columns that we want (loops through column idices)
        if i == self.HEADERS_ID: # add operator to the name of the header!
          if not extrct[2][j]:
            add.append(self[i][extrct[0][j]])
          else:
            if extrct[1][j] == '<':
              add.append(self[i][extrct[0][j]] + '<' + str(extrct[2][j]))
            if extrct[1][j] == '>':
              add.append(self[i][extrct[0][j]] + '>' + str(extrct[2][j]))
            if extrct[1][j] == '=':
              add.append(self[i][extrct[0][j]] + '=' + str(extrct[2][j]))
            if extrct[1][j] == '!':
              add.append(self[i][extrct[0][j]] + '!' + str(extrct[2][j]))
            if extrct[1][j] == '~':
              add.append(self[i][extrct[0][j]] + '~' + str(extrct[2][j]))
        else: # if we have an operator and argument, check if we satify them
          if not extrct[1][j] or not extrct[2][j] or \
          (extrct[1][j] == '<' and (tryNumber(self[i][extrct[0][j]]) < extrct[2][j])) or \
          (extrct[1][j] == '>' and (tryNumber(self[i][extrct[0][j]]) > extrct[2][j])) or \
          (extrct[1][j] == '=' and (tryNumber(self[i][extrct[0][j]]) == extrct[2][j])) or \
          (extrct[1][j] == '~' and (str(extrct[2][j]) in str(self[i][extrct[0][j]]))) or \
          (extrct[1][j] == '!' and (tryNumber(self[i][extrct[0][j]]) != extrct[2][j])) or \
          (extrct[2][j] == 'UNIQUE' and (not ret or (ret and self[i][extrct[0][j]] not in transpose(ret)[j+1]))):
            add.append(self[i][extrct[0][j]]) # add this value to the new row
          else:
            add.append(self.FLAG_VALUE) # add blank to the new row
      if i == self.HEADERS_ID: # prepend
        ret = [[self[i][self.idColumn]] + [x.replace('__','') for x in add]] + ret
      elif self.FLAG_VALUE in add:
        pass # skip this row
      elif blanks or not self.BLANK_VALUE in add: # if all values there, append to return
        ret.append([self[i][self.idColumn]] + add)
    return ret
  
  def expand(self):
    """blank-pads to make all rows as long as the headers"""
    headlen = len(self)
    for i in self.rows.keys():
      thislen = len(self[i])
      assert thislen <= headlen, "Error in dictionary row %s. Greater than length of headers row (%d): %s" % (i, headlen, self[i])
      if thislen < headlen:
        self[i] += [self.BLANK_VALUE] * (headlen - thislen)
  
  def contract(self, mode='overwrite'):
    """concatenates columns that have the same header. mode can be:
    'append', 'overwrite', 'smart_append' (consolidates values if not already present)
    or 'add' (performs plus operation for numeric values)"""

    deleteme = []
    for i in range(0,len(self)-1):
      for j in range(i+1,len(self)):
        if self.getHeaders()[i].lower().replace('__','') == self.getHeaders()[j].lower().replace('__',''): # caught the same header!
          deleteme.append(j)
          # copy over new values
          for k in self.rows.keys():
            self[k][i] = self.mergedValue(self[k][i], self[k][j], mode=mode)
            # if we merged IDs, we might need to re-index
            if i == self.idColumn:
              self.rename(self[k][i], key=k)
                
    # now delete duplicate columns
    if deleteme:
      self.removeColumns(deleteme)
  
  def zeroFill(self, zero=0):
    """fills blank cells with zero"""
    for k in self.rows.keys():
      for h in range(len(self[k])):
        if self[k][h] in [None, [], 0, '', self.BLANK_VALUE]:
          self[k][h]=zero
    
  def removeColumns(self, cols):
    """removes columns from the dictionary by index, starting from 0 (not by header name)"""
    if cols:
      if not isList(cols):
        cols = [cols]
      else:
        cols.sort()
        cols.reverse()
      # check columns to be removed
      for c in cols:
        assert type(c) is IntType, "column is not an integer: %r" % c
        if c == self.idColumn:
          raise PysheetException("Cannot remove the ID Column!")
        assert c >= 0 and c < len(self), "column is not in a valid range [%d-%d]: %d" % (0, len(self)-1, c)
        if c < self.idColumn: # if we are removing a column which is before our IDs, then we need to update the idColumn
          self.idColumn -= 1
      # now remove
      for k in self.rows.keys():
        for c in cols:
          del self[k][c]
  
  def rename(self, newName, header=None, key=None):
    """renames a column header, or a row key (not both)"""
    if header:
      hi = self.headerIndex(header)
      if hi >= 0:
        self.getHeaders()[hi] = str(newName).strip()
      else:
        raise PysheetException("Cannot rename. No such header: %s" % header)
    elif key and key != self.HEADERS_ID:
      cleanKey = clean(sanitize(key))
      cleanNewKey = clean(sanitize(newName))
      row = self[cleanKey]
      if row != None:
        if cleanKey != cleanNewKey: # don't rename if the keys are the same
          del self[cleanKey]
          val[self.idColumn] = newName
          self[cleanNewKey] = row
      else:
        raise PysheetException("Cannot rename. No such key: %s" % key)
    
  def consolidate(self, consolidations, cleanUp=False, mode='smart_append'):
    """consolidates columns according to keywords. consolidations is a 2D list of [[header, keyword, keyword, ...], ...]
    Use cleanUp=True to delete the consolidated columns after consolidation
    mode is one of: 'smart_append' (appends new value if not already present), 'append', 'overwrite' and 'add' (adds up values if numeric)"""

    # check what the user gave us
    if not isList(consolidations):
      consolidations = [consolidations]
    if not isList(consolidations[0]):
      consolidations = [consolidations]
    
    # contract first to deal with same name headers!
    self.contract(mode=mode)
    
    # now insert the new headers
    consolidationHeaders = izip(*consolidations).next() # get the new headers
    for header in consolidationHeaders:
      self.insertColumn(header)

    # now consolidate
    deleteme = []
    for i in range(len(self)):
      try:
        for h in range(len(consolidationHeaders)):
          header = consolidationHeaders[h]
          header_index = self.headerIndex(header)
          keywords = []
          if len(consolidations[h]) == 1: # no keywords given! use the header itself as keyword
            keywords = [k.lower() for k in consolidations[h]]
          else: # keywords given, skip the header
            keywords = [k.lower() for k in consolidations[h][1:]]
          for keyword in keywords:
            if header_index != i and not self.getHeaders()[i].startswith('__') and keyword in self.getHeaders()[i].lower(): # caught a similar header!
              deleteme.append(i)
              # copy over new values
              for k in self.getIds():
                self[k][header_index] = self.mergedValue(self[k][header_index], self[k][i], mode=mode)
              # skip to next column
              raise StopIteration
      except StopIteration:
        pass

    # now delete copied columns?
    if cleanUp:
      self.removeColumns(deleteme)
  
  def mergedValue(self, cellA, cellB, mode='smart_append'):
    """returns the merged value of two cells, according to mode:
    'smart_append' (appends new value if not already present), 'append', 'overwrite' and 'add' (adds up values if numeric)"""
    # check more
    mode = mode.lower()
    if mode not in ['smart_append','append','overwrite','add']:
      raise PysheetException("Merge mode '%s' is invalid!" % mode)

    if self.isBlank(cellA): # clean copy
      return cellB
    else: # merge
      if mode == 'smart_append':
        if not self.isBlank(cellB) and not str(cellB) in str(cellA).split(self.APPEND_CHAR): # if not already in there
          return "%s%s%s" % (cellA, self.APPEND_CHAR, cellB)
      elif mode == 'append':
        if not self.isBlank(cellB): # just append
          return "%s%s%s" % (cellA, self.APPEND_CHAR, cellB)
      elif mode == 'overwrite': # just copy on top
        if not self.isBlank(cellB):
          return cellB
      elif mode == 'add': # try to do numeric addition
        if not self.isBlank(cellB):
          if not self.isBlank(cellA):
            return tryNumber(cellA) + tryNumber(cellB)
          else:
            return tryNumber(cellB)
    return cellA # default is the existing value remains

  def levels(self, column, hasHeader=False):
    """returns a tuple containing (the discreet items or 'levels', is a numeric list?, the number of levels)"""
    if not isList(column):
      qcolumn = self.produceColumn(column)
      if not qcolumn or len(qcolumn) == 1: # our object is blank or this header does not exist!
        return([],False,0)
      else:
        qcolumn=qcolumn[1]
      hasHeader=False
    else:
      qcolumn = column
    offset = 1 if hasHeader else 0
    levs = unique(tryNumber(qcolumn[offset:]), blanks=False)
    return (levs, isNumber(levs), len(levs))

  def save(self, output=None, delimiter=','):
    """saves the current state of the dictionary as a delimited text file"""
    # check output
    if not output:
      if not self.filename:
        raise PysheetException("No save target given!")
      else:
        output = self.filename
    # fix delimiter
    if delimiter == r'\t':
      delimiter = "\t"
    elif delimiter == r'\s':
      delimiter = "\s"
    # write
    writer = csv.writer(open(output, "wb"), delimiter=delimiter)
    keys = self.rows.keys()
    keys.sort()
    ret = [self.rows[self.HEADERS_ID]]
    for k in keys:
      if k == self.HEADERS_ID:
        continue
      i = self.rows[k]
      line = []
      for x in i:#i[1]:
        # if x == None: saves N.
        if isinstance(x,str):
          line.append(x)
        elif isNumber(x):
          line.append(str(x))
        else:
          line.append(cPickle.dumps(x))
      ret.append(line)
    writer.writerows(ret)
    # now set the filename
    if not self.filename:
      self.filename = output
  
  def isEmpty(self):
    """returns True if this sheet is blank"""
    return len(self)<=1 and len(self.rows)<=1

  def isBlank(self, cell):
    """returns True if cell is considered blank"""
    if isList(cell):
      return len(filter((lambda i : not self.isBlank(i)),cell)) == 0
    if cell in [None, '', self.BLANK_VALUE]:
      return True
    return False

  def __str__(self):
    """returns a string representation of this object"""
    if self.isEmpty():
      return "* empty *"
    header = [[x.replace('__','') for x in self.getHeaders()]]
    ids = self.getIds()
    ids.sort()
    table = [self[i] for i in ids]
    table = header + table
    ttable = Texttable()#self.MAX_PRINT_WIDTH)
    ttable.set_deco(Texttable.HEADER | Texttable.VLINES)
    ttable.add_rows(table)
    return ttable.draw() + '\n'
    #return Pretty.indent(table, hasHeader=True, separateRows=False, headerChar='=', delim='  | ')#, wrapfunc=lambda x: Pretty.wrap_onspace_strict(x,22))

class PysheetException(Exception):
  """Pysheet exception class. You can raise it with a msg"""
  def __init__(self, msg, fname=None, line=0):
    if not fname:
      self.message = msg
    elif line and str(line).isdigit:
      self.message = "Error %s in file %s at %d" % (msg, fname, line)
    else:
      self.message = "Error %s in file %s" % (msg, fname)
  def __str__(self):
    return repr(self.message)

###############################
###### UTILITY FUNCTIONS ######
###############################

def valid_file(f):
  """type for argparse - checks that a file exists but does not open it"""
  f = os.path.realpath(f)
  if not os.path.isfile(f):
    raise PysheetException("File does not exist!", f)
  return f

def writeable_file(f):
  """type for argparse - checks that a file is writeable but does not open it"""
  f = os.path.realpath(f)
  if os.path.isfile(f) and not os.access(f, os.W_OK):# or not os.access(os.path.dirname(f), os.W_OK):
    raise PysheetException("File is not writeable!", f)
  return f

def flatten(l):
  """converts an array to string with a space in between items"""
  return reduce(lambda x,y: x+" "+y, l)

def printStackTrace():
  """prints an exception trace. To be used in an Except block"""
  traceback.print_exc(file=sys.stdout)

def isList(x):
  """returns True if x is some kind of a list"""
  return isinstance(x, (ListType, ndarray, tuple))

def isNumber(x, strOk=False):
  """returns True if x is a number (or list of numbers)"""
  if isList(x):
    return len(filter((lambda i : not isNumber(i, strOk=strOk)),x)) == 0
  if strOk: x = tryNumber(x)
  return isinstance(x, (int, long, float, floating))

def tryNumber(x):
  """tries to return an appropriate number from x or just x if not a number"""
  if isList(x):
    return [tryNumber(i) for i in x]
  if not str(x).isdigit():
    try:
      return float(x)
    except ValueError:
      return x
  else:
    try:
      return int(x) # will return long if long int!
    except ValueError:
      return x

def transpose(arr):
  """transposes a nested list (2D-array)"""
  return map(list, zip(*arr))

def unique(seq, blanks=True):
  """returns a list of unique elements from seq"""
  ret = []
  for e in seq:
    if e not in ret and (blanks or (not blanks and e)):
      ret.append(e)
  return ret

def clean(s):
  """returns the stripped lower-case of a string"""
  if isList(s):
    return [clean(i) for i in s]
  return (str(s).lower()).strip()

def sanitize(s):
  """removes special characters from a string"""
  if isList(s):
    return [sanitize(i) for i in s]
  p = re.compile('[\$\\\/&><\s\[\]\(\)\*:^]')
  return p.sub('_',str(s).strip())


if __name__ == '__main__': exit(main())

